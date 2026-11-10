"""Daemon do Agente de Borda para operação em tempo real com MQTT.

Integração dos componentes do Gateway de Borda:
 1. Transporte MQTT (recebe telemetrias de campo e envia comandos).
 2. Ingestão e validação estrita de contrato (codec.py).
 3. Detecção leve de anomalias (DetectorZ) por dispositivo e local.
 4. Diagnóstico de causa-raiz (regras causais ou LLM local via Ollama).
 5. Motor de políticas e guardrails de segurança (politicas.py).
 6. Atuador de sistema operacional (atuadores.py) para mitigações locais.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from ..catalogo import ACAO_POR_FALHA, camada_de
from ..deteccao import DetectorZ
from ..diagnostico import Diagnostico, DiagnosticadorRegras
from ..mqtt import (
    EnderecoMQTT,
    ErroMensagem,
    MensagemComando,
    MensagemEstado,
    MensagemMQTT,
    MensagemResultadoComando,
    MensagemTelemetria,
    TipoMensagem,
    decodificar_publicacao,
)
from ..politicas import Decisao, MotorPoliticas
from .atuadores import AtuadorLinux, ResultadoAtuacao
from .auditoria_db import AuditoriaSQLite
from .metricas_so import coletar_metricas_sistema
from .transporte import TransporteMQTT

logger = logging.getLogger("agente_borda")


class AgenteBordaGateway:
    """Cérebro autônomo do Gateway de Borda."""

    def __init__(
        self,
        tenant_id: str,
        site_id: str,
        gateway_id: str,
        transporte: TransporteMQTT,
        motor_politicas: MotorPoliticas,
        diagnosticador: Any | None = None,
        dry_run: bool = False,
        cooldown_s: float = 10.0,
        arquivo_auditoria: str | None = None,
        banco_auditoria: str | AuditoriaSQLite | None = None,
    ) -> None:
        self.tenant_id = tenant_id
        self.site_id = site_id
        self.gateway_id = gateway_id
        self.transporte = transporte
        self.motor = motor_politicas
        self.diag = diagnosticador or DiagnosticadorRegras()
        self.dry_run = dry_run
        self.cooldown_ms = cooldown_s * 1000.0
        self.arquivo_auditoria = arquivo_auditoria
        if isinstance(banco_auditoria, str):
            self.db: AuditoriaSQLite | None = AuditoriaSQLite(banco_auditoria)
        else:
            self.db = banco_auditoria

        # Atuador do SO para mitigações na própria borda
        self.atuador_local = AtuadorLinux(dry_run=dry_run)

        # Detectores de anomalias: um por nó monitorado (dispositivos de campo + borda)
        self.detectores: dict[str, DetectorZ] = {}
        # Observações recentes para verificação de integridade das políticas: (no, metrica) -> (valor, t_ms)
        self.observado: dict[tuple[str, str], tuple[float, float]] = {}
        # Controle de cooldown por nó: no -> ultimo_diagnostico_ms
        self.ultimos_diagnosticos: dict[str, float] = {}

        # Log em memória de auditoria e ações
        self.historico_acoes: list[dict[str, Any]] = []

    def _obter_detector(self, no: str) -> DetectorZ:
        """Obtém ou instancia um DetectorZ dedicado para o dispositivo."""
        if no not in self.detectores:
            self.detectores[no] = DetectorZ(
                janela=30, aquecimento=10, limiar_z=4.0, confirmacoes=2
            )
        return self.detectores[no]

    def iniciar(self) -> None:
        """Conecta ao transporte e assina os tópicos do site."""
        self.transporte.conectar()
        self.transporte.registrar_callback(self._ao_receber_mensagem)

        # Assina todos os eventos e telemetrias de dispositivos pertencentes a este site
        filtro_site = EnderecoMQTT.assinatura_site(self.tenant_id, self.site_id)
        self.transporte.assinar(filtro_site, qos=1)

    def parar(self) -> None:
        """Encerra a conexão com o broker."""
        self.transporte.desconectar()

    def _cooldown_liberado(self, no: str, agora_ms: float) -> bool:
        """Evita flapping garantindo um intervalo mínimo entre diagnósticos do mesmo nó."""
        ultimo = self.ultimos_diagnosticos.get(no, -10**12)
        if agora_ms - ultimo < self.cooldown_ms:
            return False
        self.ultimos_diagnosticos[no] = agora_ms
        return True

    def _registrar_auditoria(self, registro: dict[str, Any]) -> None:
        """Registra a ação em memória, em arquivo e no banco SQLite local."""
        self.historico_acoes.append(registro)
        if self.db:
            try:
                self.db.registrar(registro)
            except Exception as e:
                logger.error(f"Erro ao salvar auditoria no SQLite: {e}")
        if self.arquivo_auditoria:
            try:
                with open(self.arquivo_auditoria, "a", encoding="utf-8") as f:
                    f.write(json.dumps(registro, ensure_ascii=False) + "\n")
            except Exception as e:
                logger.error(f"Erro ao salvar auditoria: {e}")

    # -------------------------------------------------------------------------
    # Recepção de Mensagens MQTT
    # -------------------------------------------------------------------------

    def _ao_receber_mensagem(self, topico: str, payload_bytes: bytes) -> None:
        """Ponto de entrada de toda mensagem MQTT recebida pelo Gateway."""
        try:
            _, mensagem = decodificar_publicacao(topico, payload_bytes)
        except ErroMensagem as e:
            # Mensagens fora do contrato de segurança v1.0 são rejeitadas imediatamente
            logger.warning(f"Mensagem rejeitada no tópico {topico}: {e}")
            return

        agora_ms = time.time() * 1000.0

        if isinstance(mensagem, MensagemTelemetria):
            self._processar_telemetria(mensagem, agora_ms)
        elif isinstance(mensagem, MensagemResultadoComando):
            self._processar_resultado_comando(mensagem)
        elif isinstance(mensagem, MensagemEstado):
            self._processar_estado(mensagem)

    def _processar_telemetria(self, msg: MensagemTelemetria, agora_ms: float) -> None:
        """Processa leituras de sensores e recursos de um dispositivo de campo."""
        dispositivo_id = msg.device_id
        leituras = {m.nome: m.valor for m in msg.metricas}

        # Atualiza a janela de telemetria observada (usada pelo motor de políticas)
        for nome_metrica, valor in leituras.items():
            self.observado[(dispositivo_id, nome_metrica)] = (valor, agora_ms)

        # Executa o DetectorZ em O(1)
        detector = self._obter_detector(dispositivo_id)
        anomalias = detector.atualizar(leituras)

        if anomalias:
            logger.info(f"Anomalia confirmada em {dispositivo_id}: {anomalias}")
            if self._cooldown_liberado(dispositivo_id, agora_ms):
                self._diagnosticar_e_agir(dispositivo_id, anomalias, agora_ms)

    def _processar_resultado_comando(self, msg: MensagemResultadoComando) -> None:
        """Registra a resposta de um comando enviado anteriormente a um dispositivo."""
        reg = {
            "tipo": "resultado_comando",
            "device_id": msg.device_id,
            "command_id": msg.command_id,
            "status": msg.status.value,
            "resultado": msg.resultado,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._registrar_auditoria(reg)

    def _processar_estado(self, msg: MensagemEstado) -> None:
        """Acompanha presença e estado do dispositivo (online/offline via LWT)."""
        logger.info(f"Estado do dispositivo {msg.device_id}: {msg.status.value}")

    # -------------------------------------------------------------------------
    # Auto-Monitoramento do Próprio Gateway de Borda
    # -------------------------------------------------------------------------

    def tick_auto_monitoramento(self) -> None:
        """Chamado periodicamente para monitorar os recursos locais do Gateway."""
        agora_ms = time.time() * 1000.0
        no_borda = self.gateway_id

        # Lê CPU, RAM e Temperatura reais do Linux
        metricas = coletar_metricas_sistema()
        for nome, valor in metricas.items():
            self.observado[(no_borda, nome)] = (valor, agora_ms)

        detector = self._obter_detector(no_borda)
        anomalias = detector.atualizar(metricas)

        if anomalias:
            logger.warning(f"Anomalia no próprio Gateway ({no_borda}): {anomalias}")
            if self._cooldown_liberado(no_borda, agora_ms):
                self._diagnosticar_e_agir(no_borda, anomalias, agora_ms)

    # -------------------------------------------------------------------------
    # Diagnóstico, Validação de Políticas e Ação
    # -------------------------------------------------------------------------

    def _diagnosticar_e_agir(
        self, no: str, anomalias: list[dict[str, Any]], agora_ms: float
    ) -> None:
        """Gera a hipótese de causa-raiz e submete a ação aos guardrails de segurança."""
        # 1. Diagnóstico estruturado
        diag: Diagnostico | None = self.diag.diagnosticar(no, anomalias, agente="edge_agent")
        if diag is None:
            return

        # 2. Avaliação rigorosa pelo Motor de Políticas (Policy Enforcement)
        decisao: Decisao = self.motor.avaliar("edge_agent", diag, self.observado, agora_ms)

        registro = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "no": no,
            "alvo": diag.alvo,
            "falha": diag.falha,
            "acao": diag.acao,
            "confianca": diag.confianca,
            "decisao": decisao.efeito,
            "regra": decisao.regra,
            "motivo": decisao.motivo,
        }

        # 3. Se negada, encerra aqui (ação insegura contida)
        if decisao.efeito == "negar":
            registro["executada"] = False
            self._registrar_auditoria(registro)
            logger.warning(
                f"Ação {diag.acao} para {diag.alvo} NEGADA pela política [{decisao.regra}]: {decisao.motivo}"
            )
            return

        # 4. Se condicional, exigiria operador humano
        if decisao.efeito == "exigir_humano":
            registro["executada"] = False
            registro["pendente_humano"] = True
            self._registrar_auditoria(registro)
            logger.info(f"Ação {diag.acao} suspensa: aguardando confirmação humana.")
            return

        # 5. Ação aprovada: Despachar
        registro["executada"] = True
        self._despachar_acao(diag)
        self._registrar_auditoria(registro)

    def _despachar_acao(self, diag: Diagnostico) -> None:
        """Despacha a ação para o SO local ou via comando MQTT para dispositivo de campo."""
        alvo = diag.alvo

        if alvo == self.gateway_id or alvo.startswith("edge"):
            # Ação incide no próprio nó de borda -> invoca atuador local Linux
            resultado: ResultadoAtuacao = self.atuador_local.executar(diag.acao, alvo)
            logger.info(
                f"[Ação Local] {diag.acao} em {alvo}: sucesso={resultado.sucesso} ({resultado.mensagem})"
            )
        else:
            # Ação incide em dispositivo de campo -> envia comando via MQTT
            self._enviar_comando_mqtt(
                device_id=alvo,
                acao=diag.acao,
                parametros={},
            )

    def _enviar_comando_mqtt(
        self, device_id: str, acao: str, parametros: dict[str, Any]
    ) -> str:
        """Monta um envelope de comando versionado e publica no tópico do dispositivo."""
        cmd_id = str(uuid.uuid4())
        endereco = EnderecoMQTT(
            tenant_id=self.tenant_id, site_id=self.site_id, device_id=device_id
        )
        topico = endereco.topico(TipoMensagem.COMANDO)

        mensagem = MensagemComando(
            message_id=cmd_id,
            device_id=device_id,
            timestamp=datetime.now(timezone.utc),
            sequence=1,
            boot_id=f"gw-{self.gateway_id}",
            acao=acao,
            expira_em=datetime.now(timezone.utc) + timedelta(minutes=1),
            solicitado_por=f"edge-gateway:{self.gateway_id}",
            parametros=parametros,
            exige_confirmacao=True,
            politica_id="politicas-v1",
        )

        payload_bytes = mensagem.para_bytes()
        self.transporte.publicar(topico, payload_bytes, qos=1, retain=False)
        logger.info(f"[Comando MQTT Enviado] {acao} para {device_id} no tópico {topico}")
        return cmd_id

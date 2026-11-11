"""Daemon do Agente de Borda para operação em tempo real com MQTT.

Integração dos componentes do Gateway de Borda:
 1. Transporte MQTT (recebe telemetrias de campo e envia comandos).
 2. Ingestão e validação estrita de contrato (codec.py).
 3. Detecção leve de anomalias (DetectorZ) por dispositivo e local.
 4. Diagnóstico de causa-raiz (regras causais, LLM simulado ou Ollama).
 5. Motor de políticas e guardrails de segurança (politicas.py).
 6. Atuador de sistema operacional (atuadores.py) para mitigações locais.
 7. Auditoria persistente em SQLite e sincronização assíncrona com a Nuvem.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from ..deteccao import DetectorZ
from ..diagnostico import Diagnostico, DiagnosticadorRegras
from ..mqtt import (
    EnderecoMQTT,
    ErroMensagem,
    MensagemComando,
    MensagemEstado,
    MensagemResultadoComando,
    MensagemTelemetria,
    TipoMensagem,
    decodificar_publicacao,
)
from ..politicas import Decisao, MotorPoliticas
from .atuadores import AtuadorLinux, ResultadoAtuacao
from .auditoria_db import AuditoriaSQLite
from .metricas_so import ColetorMetricasLinux
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

        self._lock = threading.RLock()
        self._coletor_so = ColetorMetricasLinux()
        self.atuador_local = AtuadorLinux(dry_run=dry_run)

        self.detectores: dict[str, DetectorZ] = {}
        self.observado: dict[tuple[str, str], tuple[float, float]] = {}
        self.ultimos_diagnosticos: dict[str, float] = {}
        self.estado_dispositivos: dict[str, str] = {}
        self.aprovacoes_pendentes: dict[str, tuple[Diagnostico, dict[str, Any]]] = {}

        self.historico_acoes: list[dict[str, Any]] = []
        self._cursor_sync_memoria = 0
        self._seq_comandos = 0

    def _obter_detector(self, no: str) -> DetectorZ:
        """Obtém ou instancia um DetectorZ dedicado para o dispositivo."""
        with self._lock:
            if no not in self.detectores:
                self.detectores[no] = DetectorZ(
                    janela=30, aquecimento=10, limiar_z=4.0, confirmacoes=2
                )
            return self.detectores[no]

    def iniciar(self) -> None:
        """Conecta ao transporte e assina os tópicos do site."""
        self.transporte.registrar_callback(self._ao_receber_mensagem)
        if not self.transporte.esta_conectado():
            self.transporte.conectar()

        filtro_site = EnderecoMQTT.assinatura_site(self.tenant_id, self.site_id)
        self.transporte.assinar(filtro_site, qos=1)

    def parar(self) -> None:
        """Encerra a conexão com o broker."""
        self.transporte.desconectar()

    def _cooldown_liberado(self, no: str, agora_ms: float) -> bool:
        """Evita flapping garantindo um intervalo mínimo entre diagnósticos do mesmo nó."""
        with self._lock:
            ultimo = self.ultimos_diagnosticos.get(no, -10**12)
            if agora_ms - ultimo < self.cooldown_ms:
                return False
            self.ultimos_diagnosticos[no] = agora_ms
            return True

    def _registrar_auditoria(self, registro: dict[str, Any]) -> None:
        """Registra a ação em memória, em arquivo e no banco SQLite local."""
        with self._lock:
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

        with self._lock:
            for nome_metrica, valor in leituras.items():
                self.observado[(dispositivo_id, nome_metrica)] = (valor, agora_ms)

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
            "alvo": msg.device_id,
            "command_id": msg.command_id,
            "status": msg.status.value,
            "resultado": msg.resultado,
            "executada": msg.status.value == "succeeded",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._registrar_auditoria(reg)

    def _processar_estado(self, msg: MensagemEstado) -> None:
        """Acompanha presença e estado do dispositivo (online/offline via LWT)."""
        with self._lock:
            anterior = self.estado_dispositivos.get(msg.device_id)
            self.estado_dispositivos[msg.device_id] = msg.status.value

        logger.info(f"Estado do dispositivo {msg.device_id}: {msg.status.value}")
        if anterior != msg.status.value:
            reg = {
                "tipo": "mudanca_estado",
                "device_id": msg.device_id,
                "alvo": msg.device_id,
                "acao": f"status_{msg.status.value}",
                "motivo": msg.motivo or "anuncio_dispositivo",
                "decisao": "informativo",
                "executada": True,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self._registrar_auditoria(reg)

    # -------------------------------------------------------------------------
    # Auto-Monitoramento do Próprio Gateway de Borda
    # -------------------------------------------------------------------------

    def tick_auto_monitoramento(
        self, metricas_override: dict[str, float] | None = None
    ) -> list[dict[str, Any]]:
        """Monitora os recursos locais do Gateway (CPU, RAM, Temp) e reage a anomalias."""
        agora_ms = time.time() * 1000.0
        no_borda = self.gateway_id
        metricas = dict(metricas_override) if metricas_override else self._coletor_so.coletar()

        with self._lock:
            for nome, valor in metricas.items():
                self.observado[(no_borda, nome)] = (valor, agora_ms)

            detector = self._obter_detector(no_borda)
            anomalias = detector.atualizar(metricas)

        if anomalias:
            logger.warning(f"Anomalia no próprio Gateway ({no_borda}): {anomalias}")
            if self._cooldown_liberado(no_borda, agora_ms):
                self._diagnosticar_e_agir(no_borda, anomalias, agora_ms)
        return anomalias

    # -------------------------------------------------------------------------
    # Diagnóstico, Validação de Políticas, Aprovação Humana e Ação
    # -------------------------------------------------------------------------

    def _diagnosticar_e_agir(
        self, no: str, anomalias: list[dict[str, Any]], agora_ms: float
    ) -> None:
        """Gera a hipótese de causa-raiz e submete a ação aos guardrails de segurança."""
        diag: Diagnostico | None = self.diag.diagnosticar(no, anomalias, agente="edge_agent")
        if diag is None:
            return

        with self._lock:
            decisao: Decisao = self.motor.avaliar("edge_agent", diag, self.observado, agora_ms)

        registro: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tipo": "acao_proposta",
            "no": no,
            "alvo": diag.alvo,
            "falha": diag.falha,
            "acao": diag.acao,
            "confianca": diag.confianca,
            "decisao": decisao.efeito,
            "regra": decisao.regra,
            "motivo": decisao.motivo,
        }
        if diag.alucinacao:
            registro["alucinacao"] = diag.alucinacao

        if decisao.efeito == "negar":
            registro["executada"] = False
            self._registrar_auditoria(registro)
            logger.warning(
                f"Ação {diag.acao} para {diag.alvo} NEGADA pela política [{decisao.regra}]: {decisao.motivo}"
            )
            return

        if decisao.efeito == "exigir_humano":
            approval_id = str(uuid.uuid4())
            registro["executada"] = False
            registro["pendente_humano"] = True
            registro["approval_id"] = approval_id
            with self._lock:
                self.aprovacoes_pendentes[approval_id] = (diag, dict(registro))
            self._registrar_auditoria(registro)
            logger.info(
                f"Ação {diag.acao} suspensa (id={approval_id}): aguardando confirmação humana."
            )
            return

        registro["executada"] = True
        self._despachar_acao(diag)
        self._registrar_auditoria(registro)

    def resolver_aprovacao_humana(self, approval_id: str, aprovado: bool) -> bool:
        """Processa decisão do operador humano para ação condicional pendente."""
        with self._lock:
            item = self.aprovacoes_pendentes.pop(approval_id, None)
        if item is None:
            return False

        diag, orig_reg = item
        reg = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tipo": "aprovacao_humana",
            "approval_id": approval_id,
            "no": diag.no,
            "alvo": diag.alvo,
            "falha": diag.falha,
            "acao": diag.acao,
            "decisao": "aprovado_humano" if aprovado else "rejeitado_humano",
            "regra": orig_reg.get("regra", "condicional"),
            "executada": aprovado,
        }
        if aprovado:
            self._despachar_acao(diag)
        self._registrar_auditoria(reg)
        return True

    def _despachar_acao(self, diag: Diagnostico) -> None:
        """Despacha a ação para o SO local ou via comando MQTT para dispositivo de campo."""
        alvo = diag.alvo

        if alvo == self.gateway_id or alvo.startswith(("edge", "gw")):
            resultado: ResultadoAtuacao = self.atuador_local.executar(diag.acao, alvo)
            logger.info(
                f"[Ação Local] {diag.acao} em {alvo}: sucesso={resultado.sucesso} ({resultado.mensagem})"
            )
        else:
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

        with self._lock:
            self._seq_comandos += 1
            seq = self._seq_comandos

        mensagem = MensagemComando(
            message_id=cmd_id,
            device_id=device_id,
            timestamp=datetime.now(timezone.utc),
            sequence=seq,
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

    # -------------------------------------------------------------------------
    # Sincronização Borda -> Nuvem (com Buffer Offline SQLite)
    # -------------------------------------------------------------------------

    def sincronizar_com_nuvem(self, cloud_url: str, timeout_s: float = 2.0) -> dict[str, Any]:
        """Envia registros pendentes do SQLite local para a Nuvem e recebe decisões humanas."""
        with self._lock:
            if self.db is not None:
                pendentes_db = self.db.obter_pendentes_sincronizacao(limite=100)
                ids_db = [row["id"] for row in pendentes_db]
                registros = [json.loads(row["detalhes_json"]) for row in pendentes_db]
            else:
                ids_db = []
                registros = list(self.historico_acoes[self._cursor_sync_memoria :])

            estados = dict(self.estado_dispositivos)
            pendentes_aprov = [
                {
                    "approval_id": app_id,
                    "alvo": diag.alvo,
                    "falha": diag.falha,
                    "acao": diag.acao,
                    "timestamp": reg.get("timestamp"),
                }
                for app_id, (diag, reg) in self.aprovacoes_pendentes.items()
            ]

        corpo = json.dumps(
            {
                "gateway_id": self.gateway_id,
                "tenant_id": self.tenant_id,
                "site_id": self.site_id,
                "dispositivos_estado": estados,
                "registros": registros,
                "pendentes_aprovacao": pendentes_aprov,
            },
            ensure_ascii=False,
        ).encode("utf-8")

        url = f"{cloud_url.rstrip('/')}/api/v1/sync"
        req = urllib.request.Request(
            url, data=corpo, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                dados = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, TimeoutError, ValueError) as e:
            return {"conectado": False, "enviados": 0, "erro": str(e)}

        with self._lock:
            if self.db is not None and ids_db:
                self.db.marcar_como_sincronizado(ids_db)
            elif self.db is None:
                self._cursor_sync_memoria += len(registros)

        for dec in dados.get("decisoes_humanas", []):
            app_id = dec.get("approval_id")
            if app_id:
                self.resolver_aprovacao_humana(app_id, bool(dec.get("aprovado", False)))

        return {"conectado": True, "enviados": len(registros)}

"""Cliente de dispositivo IoT de campo (simula o firmware de um ESP32 ou nó Linux).

Responsabilidades:
 1. Publica estado (online/offline) no tópico 'state' (compatível com LWT).
 2. Publica leituras periódicas no tópico 'telemetry' com sequência e boot_id.
 3. Assina o tópico 'command' para receber instruções do Gateway de Borda.
 4. Executa a ação solicitada e publica a confirmação no tópico 'command-result'.
 5. Permite injetar falhas programadas para testes de bancada.
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping

from ..mqtt import (
    EnderecoMQTT,
    ErroMensagem,
    EstadoDispositivo,
    LeituraMetrica,
    MensagemComando,
    MensagemEstado,
    MensagemResultadoComando,
    MensagemTelemetria,
    StatusComando,
    TipoMensagem,
    decodificar_publicacao,
)
from .transporte import TransporteMQTT

logger = logging.getLogger("dispositivo_campo")


class DispositivoCampoIoT:
    """Emulador de dispositivo de campo conversando via contrato MQTT v1.0."""

    def __init__(
        self,
        tenant_id: str,
        site_id: str,
        device_id: str,
        transporte: TransporteMQTT,
        firmware_version: str = "1.0.0",
    ) -> None:
        self.tenant_id = tenant_id
        self.site_id = site_id
        self.device_id = device_id
        self.transporte = transporte
        self.firmware_version = firmware_version

        # Identificadores de ciclo de vida
        self.boot_id = f"boot-{str(uuid.uuid4())[:8]}"
        self.sequence = 0
        self.inicio_tempo = time.time()

        # Endereçamento MQTT padronizado
        self.endereco = EnderecoMQTT(
            tenant_id=self.tenant_id, site_id=self.site_id, device_id=self.device_id
        )

        # Estado interno de simulação de hardware
        self.falha_ativa: str | None = None
        self.comandos_recebidos: list[MensagemComando] = []
        self._comandos_processados: set[str] = set()

    def _configurar_lwt(self) -> None:
        topico_estado = self.endereco.topico(TipoMensagem.ESTADO)
        msg_lwt = MensagemEstado(
            message_id=str(uuid.uuid4()),
            device_id=self.device_id,
            timestamp=datetime.now(timezone.utc),
            sequence=0,
            boot_id=self.boot_id,
            status=EstadoDispositivo.OFFLINE,
            motivo="lwt_desconexao_abrupta",
        )
        self.transporte.configurar_lwt(topico_estado, msg_lwt.para_bytes(), qos=1, retain=True)

    def iniciar(self) -> None:
        """Configura LWT, conecta ao broker, assina comandos e publica estado online."""
        if not self.transporte.esta_conectado():
            self._configurar_lwt()
            self.transporte.conectar()

        # Registra callback de recebimento de comandos
        self.transporte.registrar_callback(self._ao_receber_mensagem)

        # Assina estritamente o seu próprio tópico de comandos
        topico_comando = self.endereco.topico(TipoMensagem.COMANDO)
        self.transporte.assinar(topico_comando, qos=1)

        # Publica estado 'online' com retain=True
        self._publicar_estado(EstadoDispositivo.ONLINE)
        logger.info(f"Dispositivo {self.device_id} inicializado com boot_id={self.boot_id}")

    def parar(self) -> None:
        """Notifica desligamento gracioso (offline) e desconecta."""
        self._publicar_estado(EstadoDispositivo.OFFLINE)
        self.transporte.desconectar()

    def _proximo_sequence(self) -> int:
        self.sequence += 1
        return self.sequence

    def _publicar_estado(self, status: EstadoDispositivo) -> None:
        topico = self.endereco.topico(TipoMensagem.ESTADO)
        uptime = int(time.time() - self.inicio_tempo)
        msg = MensagemEstado(
            message_id=str(uuid.uuid4()),
            device_id=self.device_id,
            timestamp=datetime.now(timezone.utc),
            sequence=self._proximo_sequence(),
            boot_id=self.boot_id,
            status=status,
            uptime_s=uptime,
            firmware_version=self.firmware_version,
            capacidades=("reset_sensor", "restart_device", "notificar_operador"),
        )
        self.transporte.publicar(topico, msg.para_bytes(), qos=1, retain=True)

    def enviar_telemetria(
        self, valores_customizados: Mapping[str, float] | None = None
    ) -> MensagemTelemetria:
        """Coleta leituras e publica telemetria periódica."""
        # Comportamento padrão dos sensores
        metricas_dict = {
            "sensor_std": 0.5,
            "cpu": 30.0,
            "mem": 40.0,
            "pkt_loss": 0.5,
        }

        # Aplica falha ativa se houver
        if self.falha_ativa == "sensor_failure":
            metricas_dict["sensor_std"] = 0.001  # Sensor travou em valor fixo
        elif self.falha_ativa == "cpu_saturation":
            metricas_dict["cpu"] = 98.5
        elif self.falha_ativa == "memory_overflow":
            metricas_dict["mem"] = 97.5
        elif self.falha_ativa == "packet_loss":
            metricas_dict["pkt_loss"] = 45.0

        # Permite override manual para testes
        if valores_customizados:
            metricas_dict.update(valores_customizados)

        leituras = [
            LeituraMetrica(nome=k, valor=v, unidade="none")
            for k, v in metricas_dict.items()
        ]

        topico = self.endereco.topico(TipoMensagem.TELEMETRIA)
        msg = MensagemTelemetria(
            message_id=str(uuid.uuid4()),
            device_id=self.device_id,
            timestamp=datetime.now(timezone.utc),
            sequence=self._proximo_sequence(),
            boot_id=self.boot_id,
            metricas=tuple(leituras),
        )

        self.transporte.publicar(topico, msg.para_bytes(), qos=0, retain=False)
        return msg

    def injetar_falha(self, tipo_falha: str) -> None:
        """Simula a ocorrência de uma pane no sensor ou no processador."""
        self.falha_ativa = tipo_falha
        logger.warning(f"Falha '{tipo_falha}' injetada no dispositivo {self.device_id}")

    # -------------------------------------------------------------------------
    # Processamento de Comandos Vindos do Gateway
    # -------------------------------------------------------------------------

    def _ao_receber_mensagem(self, topico: str, payload_bytes: bytes) -> None:
        """Escuta o tópico de comandos e executa a instrução do Gateway."""
        try:
            _, mensagem = decodificar_publicacao(topico, payload_bytes)
        except ErroMensagem as e:
            logger.warning(f"Comando rejeitado no parse: {e}")
            return

        if not isinstance(mensagem, MensagemComando):
            return

        if mensagem.device_id != self.device_id:
            logger.warning(f"Comando ignorado: destinado a {mensagem.device_id}")
            return

        if mensagem.message_id in self._comandos_processados:
            logger.info(f"Comando duplicado ignorado (idempotência): {mensagem.message_id}")
            return
        self._comandos_processados.add(mensagem.message_id)

        self.comandos_recebidos.append(mensagem)
        self._executar_comando(mensagem)

    def _executar_comando(self, cmd: MensagemComando) -> None:
        """Executa a ação solicitada e devolve o command-result com status."""
        agora = datetime.now(timezone.utc)
        topico_resultado = self.endereco.topico(TipoMensagem.RESULTADO_COMANDO)

        # 1. Validação de expiração do comando
        if cmd.expirado(agora):
            logger.warning(f"Comando {cmd.acao} ({cmd.message_id}) expirou.")
            res = MensagemResultadoComando(
                message_id=str(uuid.uuid4()),
                device_id=self.device_id,
                timestamp=agora,
                sequence=self._proximo_sequence(),
                boot_id=self.boot_id,
                correlation_id=cmd.message_id,
                command_id=cmd.message_id,
                status=StatusComando.EXPIRADO,
                codigo_erro="timed_out",
                mensagem_erro="Comando recebido apos o prazo de validade",
            )
            self.transporte.publicar(topico_resultado, res.para_bytes(), qos=1)
            return

        # 2. Execução da ação correspondente
        logger.info(f"Executando ação solicitada pelo Gateway: {cmd.acao}")
        t_inicio = agora

        mapa_cura = {
            "reset_sensor": "sensor_failure",
            "throttle_workload": "cpu_saturation",
            "clear_cache": "memory_overflow",
            "reroute_link": "packet_loss",
        }

        if cmd.acao in mapa_cura:
            if self.falha_ativa == mapa_cura[cmd.acao]:
                self.falha_ativa = None
            resultado = {"acao_executada": cmd.acao, "status": "ok"}
            status = StatusComando.SUCESSO
        elif cmd.acao == "restart_device":
            # Reinicialização: novo boot_id, sequência zerada
            self.boot_id = f"boot-{str(uuid.uuid4())[:8]}"
            self.sequence = 0
            self.falha_ativa = None
            resultado = {"dispositivo_reiniciado": True}
            status = StatusComando.SUCESSO
        elif cmd.acao == "notificar_operador":
            resultado = {"notificado": True}
            status = StatusComando.SUCESSO
        else:
            resultado = {}
            status = StatusComando.REJEITADO

        t_fim = datetime.now(timezone.utc)

        # 3. Publica a confirmação de execução (command-result)
        res = MensagemResultadoComando(
            message_id=str(uuid.uuid4()),
            device_id=self.device_id,
            timestamp=t_fim,
            sequence=self._proximo_sequence(),
            boot_id=self.boot_id,
            correlation_id=cmd.message_id,
            command_id=cmd.message_id,
            status=status,
            iniciado_em=t_inicio,
            concluido_em=t_fim,
            resultado=resultado,
        )
        self.transporte.publicar(topico_resultado, res.para_bytes(), qos=1)
        logger.info(f"Resultado do comando {cmd.acao} publicado: status={status.value}")


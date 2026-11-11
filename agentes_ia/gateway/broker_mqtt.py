"""Broker MQTT 3.1.1 embarcado em Python puro (biblioteca padrão).

Implementa o protocolo binário MQTT 3.1.1 sobre TCP/IP, compatível com clientes
padrão (ESP32 PubSubClient, Paho-MQTT, mosquitto_pub/sub e TransporteTCP):
 - CONNECT / CONNACK (com Last Will and Testament - LWT)
 - PUBLISH (QoS 0 e QoS 1 com PUBACK, suporte a mensagens retidas)
 - SUBSCRIBE / SUBACK (roteamento com curingas '+' e '#')
 - PINGREQ / PINGRESP (controle de keep-alive)
 - DISCONNECT (desconexão graciosa vs queda abrupta com disparo de LWT)
"""

from __future__ import annotations

import logging
import socket
import socketserver
import struct
import threading
from dataclasses import dataclass, field

from .mqtt_wire import (
    PKT_CONNACK,
    PKT_CONNECT,
    PKT_DISCONNECT,
    PKT_PINGREQ,
    PKT_PINGRESP,
    PKT_PUBACK,
    PKT_PUBLISH,
    PKT_SUBACK,
    PKT_SUBSCRIBE,
    PKT_UNSUBACK,
    PKT_UNSUBSCRIBE,
    codificar_tamanho_restante,
    ler_bytes,
    ler_pacote,
    ler_str,
    montar_publish,
)
from .transporte import topico_corresponde

logger = logging.getLogger("broker_mqtt")


@dataclass
class SessaoCliente:
    client_id: str
    sock: socket.socket
    lock_envio: threading.Lock = field(default_factory=threading.Lock)
    assinaturas: dict[str, int] = field(default_factory=dict)
    will: tuple[str, bytes, int, bool] | None = None
    desconectado_graciosamente: bool = False
    packet_id_seq: int = 1

    def enviar_raw(self, dados: bytes) -> bool:
        with self.lock_envio:
            try:
                self.sock.sendall(dados)
                return True
            except OSError:
                return False

    def proximo_packet_id(self) -> int:
        with self.lock_envio:
            pid = self.packet_id_seq
            self.packet_id_seq = (self.packet_id_seq % 65535) + 1
            return pid


class _ServidorTCP(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], broker: "BrokerMQTTLocal") -> None:
        self.broker = broker
        super().__init__(server_address, _HandlerClienteMQTT)


class _HandlerClienteMQTT(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        broker: BrokerMQTTLocal = self.server.broker  # type: ignore[attr-defined]
        sock: socket.socket = self.request
        sessao: SessaoCliente | None = None

        try:
            sock.settimeout(15.0)
            tipo, flags, corpo = ler_pacote(sock)
            if tipo != PKT_CONNECT:
                return

            sessao, keepalive = self._processar_connect(sock, corpo)
            broker.registrar_sessao(sessao)

            # Envia CONNACK (session_present=0, return_code=0)
            sessao.enviar_raw(bytes([PKT_CONNACK << 4, 2, 0, 0]))

            # Ajusta timeout do socket com base no keepalive (regra MQTT: 1.5 * keepalive)
            sock.settimeout(keepalive * 1.5 if keepalive > 0 else None)

            while broker.ativo:
                tipo, flags, corpo = ler_pacote(sock)
                if tipo == PKT_PUBLISH:
                    self._processar_publish(broker, sessao, flags, corpo)
                elif tipo == PKT_PUBACK:
                    pass
                elif tipo == PKT_SUBSCRIBE:
                    self._processar_subscribe(broker, sessao, corpo)
                elif tipo == PKT_UNSUBSCRIBE:
                    self._processar_unsubscribe(sessao, corpo)
                elif tipo == PKT_PINGREQ:
                    sessao.enviar_raw(bytes([PKT_PINGRESP << 4, 0]))
                elif tipo == PKT_DISCONNECT:
                    sessao.desconectado_graciosamente = True
                    break

        except (EOFError, TimeoutError, socket.timeout, OSError, ValueError):
            pass
        finally:
            if sessao is not None:
                broker.remover_sessao(sessao)
            try:
                sock.close()
            except OSError:
                pass

    def _processar_connect(
        self, sock: socket.socket, corpo: bytes
    ) -> tuple[SessaoCliente, int]:
        proto_name, offset = ler_str(corpo, 0)
        if proto_name not in ("MQTT", "MQIsdp"):
            raise ValueError(f"Protocolo não suportado: {proto_name}")
        _proto_level = corpo[offset]
        connect_flags = corpo[offset + 1]
        (keepalive,) = struct.unpack("!H", corpo[offset + 2 : offset + 4])
        offset += 4

        client_id, offset = ler_str(corpo, offset)
        will_flag = bool(connect_flags & 0x04)
        will_qos = (connect_flags >> 3) & 0x03
        will_retain = bool(connect_flags & 0x20)

        will = None
        if will_flag:
            will_topic, offset = ler_str(corpo, offset)
            will_msg, offset = ler_bytes(corpo, offset)
            will = (will_topic, will_msg, will_qos, will_retain)

        sessao = SessaoCliente(client_id=client_id, sock=sock, will=will)
        return sessao, keepalive

    def _processar_publish(
        self, broker: BrokerMQTTLocal, sessao: SessaoCliente, flags: int, corpo: bytes
    ) -> None:
        qos = (flags >> 1) & 0x03
        retain = bool(flags & 0x01)
        topico, offset = ler_str(corpo, 0)
        if qos > 0:
            (packet_id,) = struct.unpack("!H", corpo[offset : offset + 2])
            offset += 2
            sessao.enviar_raw(struct.pack("!BBH", PKT_PUBACK << 4, 2, packet_id))
        payload = corpo[offset:]
        broker.publicar(topico, payload, qos=qos, retain=retain)

    def _processar_subscribe(
        self, broker: BrokerMQTTLocal, sessao: SessaoCliente, corpo: bytes
    ) -> None:
        (packet_id,) = struct.unpack("!H", corpo[:2])
        offset = 2
        codigos = bytearray()

        while offset < len(corpo):
            filtro, offset = ler_str(corpo, offset)
            req_qos = corpo[offset]
            offset += 1
            qos_concedido = min(req_qos, 1)
            sessao.assinaturas[filtro] = qos_concedido
            codigos.append(qos_concedido)

        suback_corpo = struct.pack("!H", packet_id) + bytes(codigos)
        pkt = bytes([PKT_SUBACK << 4]) + codificar_tamanho_restante(len(suback_corpo)) + suback_corpo
        sessao.enviar_raw(pkt)

        broker.entregar_retidas(sessao)

    def _processar_unsubscribe(self, sessao: SessaoCliente, corpo: bytes) -> None:
        (packet_id,) = struct.unpack("!H", corpo[:2])
        offset = 2
        while offset < len(corpo):
            filtro, offset = ler_str(corpo, offset)
            sessao.assinaturas.pop(filtro, None)
        sessao.enviar_raw(struct.pack("!BBH", PKT_UNSUBACK << 4, 2, packet_id))


class BrokerMQTTLocal:
    """Broker MQTT 3.1.1 completo rodando sobre TCP na biblioteca padrão."""

    def __init__(self, host: str = "127.0.0.1", port: int = 1883) -> None:
        self.host = host
        self.port = port
        self._lock = threading.Lock()
        self._sessoes: dict[str, SessaoCliente] = {}
        self._retidas: dict[str, tuple[bytes, int]] = {}
        self._servidor: _ServidorTCP | None = None
        self._thread: threading.Thread | None = None
        self.ativo = False
        self.total_mensagens = 0
        self.lwt_disparados = 0

    def registrar_sessao(self, nova: SessaoCliente) -> None:
        with self._lock:
            antiga = self._sessoes.get(nova.client_id)
            if antiga is not None:
                antiga.desconectado_graciosamente = True
                try:
                    antiga.sock.close()
                except OSError:
                    pass
            self._sessoes[nova.client_id] = nova
            logger.info(f"Cliente MQTT conectado: {nova.client_id}")

    def remover_sessao(self, sessao: SessaoCliente) -> None:
        will_a_disparar = None
        with self._lock:
            atual = self._sessoes.get(sessao.client_id)
            if atual is sessao:
                self._sessoes.pop(sessao.client_id, None)
                if not sessao.desconectado_graciosamente and sessao.will is not None:
                    will_a_disparar = sessao.will

        if will_a_disparar is not None:
            topico, payload, qos, retain = will_a_disparar
            self.lwt_disparados += 1
            logger.warning(f"Queda abrupta de {sessao.client_id}: disparando LWT em {topico}")
            self.publicar(topico, payload, qos=qos, retain=retain)

    def publicar(
        self, topico: str, payload: bytes, qos: int = 0, retain: bool = False
    ) -> None:
        with self._lock:
            self.total_mensagens += 1
            if retain:
                if len(payload) == 0:
                    self._retidas.pop(topico, None)
                else:
                    self._retidas[topico] = (payload, qos)
            destinatarios = list(self._sessoes.values())

        for sessao in destinatarios:
            for filtro, qos_assinatura in list(sessao.assinaturas.items()):
                if topico_corresponde(filtro, topico):
                    qos_entrega = min(qos, qos_assinatura)
                    pid = sessao.proximo_packet_id() if qos_entrega > 0 else 0
                    pkt = montar_publish(
                        topico, payload, qos=qos_entrega, retain=False, packet_id=pid
                    )
                    sessao.enviar_raw(pkt)
                    break

    def entregar_retidas(self, sessao: SessaoCliente) -> None:
        with self._lock:
            retidas = list(self._retidas.items())
        for topico, (payload, qos) in retidas:
            for filtro, qos_assinatura in list(sessao.assinaturas.items()):
                if topico_corresponde(filtro, topico):
                    qos_entrega = min(qos, qos_assinatura)
                    pid = sessao.proximo_packet_id() if qos_entrega > 0 else 0
                    pkt = montar_publish(
                        topico, payload, qos=qos_entrega, retain=True, packet_id=pid
                    )
                    sessao.enviar_raw(pkt)
                    break

    def iniciar(self) -> int:
        """Inicia o servidor TCP em thread em background e retorna a porta vinculada."""
        self.ativo = True
        self._servidor = _ServidorTCP((self.host, self.port), self)
        self.port = self._servidor.server_address[1]
        self._thread = threading.Thread(
            target=self._servidor.serve_forever, daemon=True, name="mqtt-broker-tcp"
        )
        self._thread.start()
        logger.info(f"Broker MQTT 3.1.1 escutando em {self.host}:{self.port}")
        return self.port

    def parar(self) -> None:
        """Encerra o broker e fecha todas as conexões ativas."""
        self.ativo = False
        with self._lock:
            for s in list(self._sessoes.values()):
                s.desconectado_graciosamente = True
                try:
                    s.sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    s.sock.close()
                except OSError:
                    pass
            self._sessoes.clear()
        if self._servidor is not None:
            self._servidor.shutdown()
            self._servidor.server_close()
            self._servidor = None

"""Camada de transporte agnóstica para comunicação MQTT.

Desacopla o Agente de Borda da biblioteca de rede:
 - TransporteMQTT: Interface base abstrata.
 - TransporteTCP: Cliente MQTT 3.1.1 nativo sobre sockets TCP (biblioteca padrão).
 - TransportePaho: Adaptador opcional para Paho-MQTT conectado ao Mosquitto.
 - TransporteMemoria: Broker em memória para testes unitários rápidos e validação.
"""

from __future__ import annotations

import logging
import socket
import struct
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Callable

from .mqtt_wire import (
    PKT_CONNACK,
    PKT_DISCONNECT,
    PKT_PINGREQ,
    PKT_PUBACK,
    PKT_PUBLISH,
    PKT_SUBACK,
    ler_pacote,
    ler_str,
    montar_connect,
    montar_publish,
    montar_subscribe,
)

logger = logging.getLogger("transporte_mqtt")

# Tipo do callback: recebe (topico: str, payload: bytes)
CallbackMensagem = Callable[[str, bytes], None]


def topico_corresponde(filtro: str, topico: str) -> bool:
    """Verifica se um tópico MQTT corresponde a um filtro com curingas (+ e #).

    Exemplos:
      filtro: iot/v1/+/+/+/telemetry  topico: iot/v1/acme/sp/dev1/telemetry -> True
      filtro: iot/v1/acme/#           topico: iot/v1/acme/sp/dev1/command   -> True
    """
    if filtro == topico or filtro == "#":
        return True

    partes_filtro = filtro.split("/")
    partes_topico = topico.split("/")

    i = 0
    while i < len(partes_filtro):
        f = partes_filtro[i]
        if f == "#":
            return True
        if i >= len(partes_topico):
            return False
        t = partes_topico[i]
        if f != "+" and f != t:
            return False
        i += 1

    return i == len(partes_topico)


class TransporteMQTT(ABC):
    """Interface abstrata de transporte de mensagens MQTT."""

    def configurar_lwt(
        self, topico: str, payload: bytes | str, qos: int = 1, retain: bool = True
    ) -> None:
        """Configura mensagem de Last Will and Testament (opcional por transporte)."""
        pass

    @abstractmethod
    def conectar(self) -> None:
        """Estabelece a conexão com o broker."""
        pass

    @abstractmethod
    def desconectar(self) -> None:
        """Encerra a conexão com o broker."""
        pass

    @abstractmethod
    def esta_conectado(self) -> bool:
        """Informa se a conexão está ativa."""
        pass

    @abstractmethod
    def assinar(self, topico: str, qos: int = 1) -> None:
        """Assina um tópico ou padrão com curingas."""
        pass

    @abstractmethod
    def publicar(
        self, topico: str, payload: bytes | str, qos: int = 1, retain: bool = False
    ) -> None:
        """Publica uma mensagem no tópico indicado."""
        pass

    @abstractmethod
    def registrar_callback(self, callback: CallbackMensagem) -> None:
        """Define a função que será chamada ao receber mensagens assinadas."""
        pass


class TransporteMemoria(TransporteMQTT):
    """Transporte em memória para testes e validação com suporte a curingas MQTT."""

    def __init__(self) -> None:
        self._conectado = False
        self._assinaturas: set[str] = set()
        self._callbacks: list[CallbackMensagem] = []
        self._historico_publicacoes: list[tuple[str, bytes, int, bool]] = []
        self._lwt: tuple[str, bytes, int, bool] | None = None

    def configurar_lwt(
        self, topico: str, payload: bytes | str, qos: int = 1, retain: bool = True
    ) -> None:
        dados = payload.encode("utf-8") if isinstance(payload, str) else payload
        self._lwt = (topico, dados, qos, retain)

    def conectar(self) -> None:
        self._conectado = True

    def desconectar(self) -> None:
        self._conectado = False

    def esta_conectado(self) -> bool:
        return self._conectado

    def assinar(self, topico: str, qos: int = 1) -> None:
        self._assinaturas.add(topico)

    def registrar_callback(self, callback: CallbackMensagem) -> None:
        self._callbacks.append(callback)

    def publicar(
        self, topico: str, payload: bytes | str, qos: int = 1, retain: bool = False
    ) -> None:
        if not self._conectado:
            raise ConnectionError("Transporte em memória desconectado")

        dados = payload.encode("utf-8") if isinstance(payload, str) else payload
        self._historico_publicacoes.append((topico, dados, qos, retain))

        for assinatura in self._assinaturas:
            if topico_corresponde(assinatura, topico):
                for cb in self._callbacks:
                    cb(topico, dados)
                break

    @property
    def publicacoes(self) -> list[tuple[str, bytes, int, bool]]:
        """Acesso ao histórico de mensagens publicadas para asserções de teste."""
        return list(self._historico_publicacoes)


class TransporteTCP(TransporteMQTT):
    """Cliente MQTT 3.1.1 nativo sobre TCP/IP usando apenas biblioteca padrão."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 1883,
        client_id: str = "edge-gateway-client",
        keepalive: int = 20,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.keepalive = keepalive
        self._sock: socket.socket | None = None
        self._lock_envio = threading.Lock()
        self._conectado = False
        self._parar = False
        self._packet_id = 1
        self._callbacks: list[CallbackMensagem] = []
        self._lwt: tuple[str, bytes, int, bool] | None = None
        self._thread_leitura: threading.Thread | None = None
        self._thread_keepalive: threading.Thread | None = None

    def configurar_lwt(
        self, topico: str, payload: bytes | str, qos: int = 1, retain: bool = True
    ) -> None:
        dados = payload.encode("utf-8") if isinstance(payload, str) else payload
        self._lwt = (topico, dados, qos, retain)

    def _proximo_packet_id(self) -> int:
        pid = self._packet_id
        self._packet_id = (self._packet_id % 65535) + 1
        return pid

    def conectar(self) -> None:
        if self._conectado:
            return
        self._parar = False
        sock = socket.create_connection((self.host, self.port), timeout=10.0)
        sock.settimeout(None)
        pkt_connect = montar_connect(
            client_id=self.client_id, keepalive=self.keepalive, will=self._lwt
        )
        sock.sendall(pkt_connect)

        tipo, _flags, corpo = ler_pacote(sock)
        if tipo != PKT_CONNACK or len(corpo) < 2 or corpo[1] != 0:
            sock.close()
            raise ConnectionError(f"Broker rejeitou conexão MQTT: CONNACK={corpo!r}")

        self._sock = sock
        self._conectado = True

        self._thread_leitura = threading.Thread(
            target=self._loop_leitura, daemon=True, name=f"mqtt-rx-{self.client_id}"
        )
        self._thread_leitura.start()

        if self.keepalive > 0:
            self._thread_keepalive = threading.Thread(
                target=self._loop_keepalive, daemon=True, name=f"mqtt-ka-{self.client_id}"
            )
            self._thread_keepalive.start()

    def desconectar(self) -> None:
        self._parar = True
        self._conectado = False
        with self._lock_envio:
            if self._sock is not None:
                try:
                    self._sock.sendall(bytes([PKT_DISCONNECT << 4, 0]))
                except OSError:
                    pass
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None

    def forcar_queda_abrupta(self) -> None:
        """Fecha o socket TCP sem enviar DISCONNECT para simular perda de energia/rede (dispara LWT)."""
        self._parar = True
        self._conectado = False
        with self._lock_envio:
            if self._sock is not None:
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None

    def esta_conectado(self) -> bool:
        return self._conectado

    def assinar(self, topico: str, qos: int = 1) -> None:
        if not self._conectado or self._sock is None:
            raise ConnectionError("TransporteTCP não conectado")
        with self._lock_envio:
            pid = self._proximo_packet_id()
            self._sock.sendall(montar_subscribe(pid, topico, qos=qos))

    def publicar(
        self, topico: str, payload: bytes | str, qos: int = 1, retain: bool = False
    ) -> None:
        if not self._conectado or self._sock is None:
            raise ConnectionError("TransporteTCP não conectado")
        dados = payload.encode("utf-8") if isinstance(payload, str) else payload
        with self._lock_envio:
            pid = self._proximo_packet_id() if qos > 0 else 0
            pkt = montar_publish(topico, dados, qos=qos, retain=retain, packet_id=pid)
            self._sock.sendall(pkt)

    def registrar_callback(self, callback: CallbackMensagem) -> None:
        self._callbacks.append(callback)

    def _loop_keepalive(self) -> None:
        intervalo = max(1.0, self.keepalive / 2.0)
        while not self._parar and self._conectado:
            time.sleep(intervalo)
            if not self._conectado or self._parar:
                break
            with self._lock_envio:
                if self._sock is not None:
                    try:
                        self._sock.sendall(bytes([PKT_PINGREQ << 4, 0]))
                    except OSError:
                        self._conectado = False
                        break

    def _loop_leitura(self) -> None:
        while not self._parar and self._sock is not None:
            try:
                tipo, flags, corpo = ler_pacote(self._sock)
            except (EOFError, OSError, ValueError):
                if not self._parar:
                    self._conectado = False
                break

            if tipo == PKT_PUBLISH:
                qos = (flags >> 1) & 0x03
                topico, offset = ler_str(corpo, 0)
                if qos > 0:
                    (pid,) = struct.unpack("!H", corpo[offset : offset + 2])
                    offset += 2
                    with self._lock_envio:
                        if self._sock is not None:
                            try:
                                self._sock.sendall(
                                    struct.pack("!BBH", PKT_PUBACK << 4, 2, pid)
                                )
                            except OSError:
                                pass
                payload = corpo[offset:]
                for cb in list(self._callbacks):
                    try:
                        cb(topico, payload)
                    except Exception as exc:
                        logger.error(f"Erro no callback MQTT: {exc}")
            elif tipo in (PKT_SUBACK, PKT_PUBACK):
                pass


class TransportePaho(TransporteMQTT):
    """Adaptador opcional para cliente Paho-MQTT conectado ao Mosquitto."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        client_id: str = "edge-gateway-agent",
        keepalive: int = 60,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.keepalive = keepalive
        self._conectado = False
        self._callback: CallbackMensagem | None = None
        self._cliente: Any = None
        self._lwt: tuple[str, bytes, int, bool] | None = None

    def configurar_lwt(
        self, topico: str, payload: bytes | str, qos: int = 1, retain: bool = True
    ) -> None:
        dados = payload.encode("utf-8") if isinstance(payload, str) else payload
        self._lwt = (topico, dados, qos, retain)
        if self._cliente is not None:
            self._cliente.will_set(topico, dados, qos=qos, retain=retain)

    def _obter_cliente(self) -> Any:
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            raise ImportError(
                "Biblioteca 'paho-mqtt' não encontrada. "
                "Use TransporteTCP (nativo) ou instale 'pip install paho-mqtt'."
            )

        if self._cliente is None:
            try:
                self._cliente = mqtt.Client(
                    mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id
                )
            except AttributeError:
                self._cliente = mqtt.Client(client_id=self.client_id)

            if self._lwt is not None:
                self._cliente.will_set(*self._lwt)

            def _ao_conectar(client: Any, userdata: Any, flags: Any, rc: Any, *args: Any) -> None:
                self._conectado = True

            def _ao_desconectar(client: Any, userdata: Any, rc: Any, *args: Any) -> None:
                self._conectado = False

            def _ao_receber_mensagem(client: Any, userdata: Any, msg: Any) -> None:
                if self._callback:
                    self._callback(msg.topic, msg.payload)

            self._cliente.on_connect = _ao_conectar
            self._cliente.on_disconnect = _ao_desconectar
            self._cliente.on_message = _ao_receber_mensagem

        return self._cliente

    def conectar(self) -> None:
        cliente = self._obter_cliente()
        cliente.connect(self.host, self.port, self.keepalive)
        cliente.loop_start()

    def desconectar(self) -> None:
        if self._cliente:
            self._cliente.loop_stop()
            self._cliente.disconnect()
            self._conectado = False

    def esta_conectado(self) -> bool:
        return self._conectado

    def assinar(self, topico: str, qos: int = 1) -> None:
        cliente = self._obter_cliente()
        cliente.subscribe(topico, qos=qos)

    def publicar(
        self, topico: str, payload: bytes | str, qos: int = 1, retain: bool = False
    ) -> None:
        cliente = self._obter_cliente()
        dados = payload.encode("utf-8") if isinstance(payload, str) else payload
        cliente.publish(topico, dados, qos=qos, retain=retain)

    def registrar_callback(self, callback: CallbackMensagem) -> None:
        self._callback = callback

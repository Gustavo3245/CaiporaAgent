"""Camada de transporte agnóstica para comunicação MQTT.

Desacopla o Agente de Borda da biblioteca de rede:
 - TransporteMQTT: Interface base abstrata.
 - TransportePaho: Adaptador de produção para conexões reais com Eclipse Mosquitto.
 - TransporteMemoria: Broker em memória para testes unitários rápidos e validação.
"""

from __future__ import annotations

import fnmatch
from abc import ABC, abstractmethod
from typing import Any, Callable

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
            # '#' consome todos os níveis restantes
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

        # Notifica callbacks se o tópico corresponder a alguma assinatura
        for assinatura in self._assinaturas:
            if topico_corresponde(assinatura, topico):
                for cb in self._callbacks:
                    cb(topico, dados)
                break

    @property
    def publicacoes(self) -> list[tuple[str, bytes, int, bool]]:
        """Acesso ao histórico de mensagens publicadas para asserções de teste."""
        return list(self._historico_publicacoes)


class TransportePaho(TransporteMQTT):
    """Adaptador de produção para cliente Paho-MQTT conectado ao Mosquitto."""

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

    def _obter_cliente(self) -> Any:
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            raise ImportError(
                "Biblioteca 'paho-mqtt' não encontrada. "
                "Para conectar a um broker físico real, instale com 'pip install paho-mqtt'."
            )

        if self._cliente is None:
            # Suporta tanto Paho MQTT v1 quanto v2
            try:
                self._cliente = mqtt.Client(
                    mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id
                )
            except AttributeError:
                self._cliente = mqtt.Client(client_id=self.client_id)

            def _ao_conectar(client: Any, userdata: Any, flags: Any, rc: Any, *args: Any) -> None:
                self._conectado = True

            def _ao_desconectar(
                client: Any, userdata: Any, rc: Any, *args: Any
            ) -> None:
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


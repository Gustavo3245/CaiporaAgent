"""Pacote do Gateway de Borda para operação com MQTT e hardware/SO real."""

from .metricas_so import coletar_metricas_sistema, ColetorMetricasLinux
from .atuadores import executar_acao_local, AtuadorLinux, ResultadoAtuacao
from .transporte import (
    TransporteMQTT,
    TransporteMemoria,
    TransportePaho,
    TransporteTCP,
    topico_corresponde,
)
from .broker_mqtt import BrokerMQTTLocal
from .agente import AgenteBordaGateway
from .dispositivo_campo import DispositivoCampoIoT
from .auditoria_db import AuditoriaSQLite

__all__ = [
    "coletar_metricas_sistema",
    "ColetorMetricasLinux",
    "executar_acao_local",
    "AtuadorLinux",
    "ResultadoAtuacao",
    "TransporteMQTT",
    "TransporteMemoria",
    "TransportePaho",
    "TransporteTCP",
    "BrokerMQTTLocal",
    "topico_corresponde",
    "AgenteBordaGateway",
    "DispositivoCampoIoT",
    "AuditoriaSQLite",
]

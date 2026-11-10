"""Tipos estáveis que fazem parte do contrato MQTT versão 1."""

from dataclasses import dataclass
from enum import Enum


VERSAO_SCHEMA = "1.0"
PREFIXO_TOPICO = "iot/v1"
TAMANHO_MAX_MENSAGEM = 256 * 1024


class TipoMensagem(str, Enum):
    TELEMETRIA = "telemetry"
    LOG = "log"
    EVENTO = "event"
    ESTADO = "state"
    COMANDO = "command"
    RESULTADO_COMANDO = "command-result"


class QualidadeMetrica(str, Enum):
    BOA = "good"
    INCERTA = "uncertain"
    RUIM = "bad"


class NivelLog(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    AVISO = "warning"
    ERRO = "error"
    CRITICO = "critical"


class EstadoDispositivo(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    DEGRADADO = "degraded"
    MANUTENCAO = "maintenance"


class StatusComando(str, Enum):
    ACEITO = "accepted"
    EXECUTANDO = "running"
    SUCESSO = "succeeded"
    FALHOU = "failed"
    REJEITADO = "rejected"
    EXPIRADO = "timed-out"


@dataclass(frozen=True)
class PoliticaPublicacao:
    qos: int
    retain: bool


POLITICA_PUBLICACAO = {
    TipoMensagem.TELEMETRIA: PoliticaPublicacao(qos=0, retain=False),
    TipoMensagem.LOG: PoliticaPublicacao(qos=1, retain=False),
    TipoMensagem.EVENTO: PoliticaPublicacao(qos=1, retain=False),
    TipoMensagem.ESTADO: PoliticaPublicacao(qos=1, retain=True),
    TipoMensagem.COMANDO: PoliticaPublicacao(qos=1, retain=False),
    TipoMensagem.RESULTADO_COMANDO: PoliticaPublicacao(qos=1, retain=False),
}

"""Modelos de domínio serializáveis no payload das publicações MQTT."""

from __future__ import annotations

import json
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, ClassVar, Mapping

from .tipos import (
    EstadoDispositivo,
    NivelLog,
    QualidadeMetrica,
    StatusComando,
    TipoMensagem,
    VERSAO_SCHEMA,
)
from .validacao import (
    ErroMensagem,
    converter_enum,
    copiar_objeto_json,
    data_do_json,
    data_para_json,
    campo_obrigatorio,
    exigir_objeto,
    rejeitar_campos_extras,
    validar_data,
    validar_identificador,
    validar_texto,
    validar_uuid,
)


@dataclass(frozen=True, kw_only=True)
class LeituraMetrica:
    nome: str
    valor: float
    unidade: str
    qualidade: QualidadeMetrica = QualidadeMetrica.BOA
    timestamp: datetime | None = None
    tags: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validar_identificador("nome da métrica", self.nome)
        if isinstance(self.valor, bool) or not isinstance(self.valor, (int, float)):
            raise ErroMensagem("valor da métrica deve ser numérico")
        if not math.isfinite(self.valor):
            raise ErroMensagem("valor da métrica deve ser finito")
        if not isinstance(self.unidade, str) or len(self.unidade) > 32:
            raise ErroMensagem("unidade deve ser um texto de até 32 caracteres")
        qualidade = converter_enum(QualidadeMetrica, "qualidade", self.qualidade)
        object.__setattr__(self, "qualidade", qualidade)
        validar_data("timestamp da métrica", self.timestamp)
        tags = copiar_objeto_json("tags da métrica", self.tags)
        for chave, valor in tags.items():
            validar_identificador("chave da tag", chave)
            validar_texto("valor da tag", valor, 128)
        object.__setattr__(self, "tags", tags)

    def para_dict(self) -> dict[str, Any]:
        dados = {
            "name": self.nome,
            "value": self.valor,
            "unit": self.unidade,
            "quality": self.qualidade.value,
        }
        if self.timestamp is not None:
            dados["timestamp"] = data_para_json(self.timestamp)
        if self.tags:
            dados["tags"] = dict(self.tags)
        return dados

    @classmethod
    def de_dict(cls, dados: Any) -> "LeituraMetrica":
        metrica = exigir_objeto("métrica", dados)
        rejeitar_campos_extras(
            metrica,
            {"name", "value", "unit", "quality", "timestamp", "tags"},
            "métrica",
        )
        timestamp = (
            data_do_json("timestamp da métrica", metrica["timestamp"])
            if "timestamp" in metrica
            else None
        )
        return cls(
            nome=campo_obrigatorio(metrica, "name", "métrica"),
            valor=campo_obrigatorio(metrica, "value", "métrica"),
            unidade=campo_obrigatorio(metrica, "unit", "métrica"),
            qualidade=metrica.get("quality", QualidadeMetrica.BOA.value),
            timestamp=timestamp,
            tags=metrica.get("tags", {}),
        )


@dataclass(frozen=True, kw_only=True)
class MensagemMQTT:
    """Envelope comum a todas as mensagens do contrato."""

    device_id: str
    sequence: int
    boot_id: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    gateway_id: str | None = None
    correlation_id: str | None = None

    tipo: ClassVar[TipoMensagem]

    def __post_init__(self) -> None:
        validar_identificador("device_id", self.device_id)
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 0:
            raise ErroMensagem("sequence deve ser um inteiro maior ou igual a zero")
        validar_texto("boot_id", self.boot_id, 128)
        validar_data("timestamp", self.timestamp, obrigatoria=True)
        validar_uuid("message_id", self.message_id)
        if self.gateway_id is not None:
            validar_identificador("gateway_id", self.gateway_id)
        if self.correlation_id is not None:
            validar_texto("correlation_id", self.correlation_id, 128)

    def _payload(self) -> dict[str, Any]:
        raise NotImplementedError

    def para_dict(self) -> dict[str, Any]:
        dados = {
            "schema_version": VERSAO_SCHEMA,
            "message_type": self.tipo.value,
            "message_id": self.message_id,
            "device_id": self.device_id,
            "timestamp": data_para_json(self.timestamp),
            "sequence": self.sequence,
            "boot_id": self.boot_id,
            "payload": self._payload(),
        }
        if self.gateway_id is not None:
            dados["gateway_id"] = self.gateway_id
        if self.correlation_id is not None:
            dados["correlation_id"] = self.correlation_id
        return dados

    def para_json(self) -> str:
        return json.dumps(
            self.para_dict(),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    def para_bytes(self) -> bytes:
        return self.para_json().encode("utf-8")


@dataclass(frozen=True, kw_only=True)
class MensagemTelemetria(MensagemMQTT):
    metricas: tuple[LeituraMetrica, ...]
    intervalo_amostragem_ms: int | None = None

    tipo: ClassVar[TipoMensagem] = TipoMensagem.TELEMETRIA

    def __post_init__(self) -> None:
        super().__post_init__()
        try:
            metricas = tuple(self.metricas)
        except TypeError as exc:
            raise ErroMensagem("telemetria deve conter uma coleção de métricas") from exc
        if not metricas or not all(isinstance(item, LeituraMetrica) for item in metricas):
            raise ErroMensagem("telemetria deve conter ao menos uma LeituraMetrica")
        object.__setattr__(self, "metricas", metricas)
        if self.intervalo_amostragem_ms is not None and (
            isinstance(self.intervalo_amostragem_ms, bool)
            or not isinstance(self.intervalo_amostragem_ms, int)
            or self.intervalo_amostragem_ms <= 0
        ):
            raise ErroMensagem("sample_interval_ms deve ser um inteiro positivo")

    def _payload(self) -> dict[str, Any]:
        dados: dict[str, Any] = {"metrics": [item.para_dict() for item in self.metricas]}
        if self.intervalo_amostragem_ms is not None:
            dados["sample_interval_ms"] = self.intervalo_amostragem_ms
        return dados


@dataclass(frozen=True, kw_only=True)
class MensagemLog(MensagemMQTT):
    nivel: NivelLog
    componente: str
    mensagem: str
    codigo: str | None = None
    contexto: Mapping[str, Any] = field(default_factory=dict)

    tipo: ClassVar[TipoMensagem] = TipoMensagem.LOG

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "nivel", converter_enum(NivelLog, "level", self.nivel))
        validar_identificador("component", self.componente)
        validar_texto("message", self.mensagem, 4096)
        if self.codigo is not None:
            validar_identificador("code", self.codigo)
        object.__setattr__(self, "contexto", copiar_objeto_json("context", self.contexto))

    def _payload(self) -> dict[str, Any]:
        dados = {
            "level": self.nivel.value,
            "component": self.componente,
            "message": self.mensagem,
        }
        if self.codigo is not None:
            dados["code"] = self.codigo
        if self.contexto:
            dados["context"] = dict(self.contexto)
        return dados


@dataclass(frozen=True, kw_only=True)
class MensagemEvento(MensagemMQTT):
    evento: str
    severidade: NivelLog
    estado: str | None = None
    atributos: Mapping[str, Any] = field(default_factory=dict)

    tipo: ClassVar[TipoMensagem] = TipoMensagem.EVENTO

    def __post_init__(self) -> None:
        super().__post_init__()
        validar_identificador("event", self.evento)
        severidade = converter_enum(NivelLog, "severity", self.severidade)
        object.__setattr__(self, "severidade", severidade)
        if self.estado is not None:
            validar_texto("state", self.estado, 128)
        object.__setattr__(
            self, "atributos", copiar_objeto_json("attributes", self.atributos)
        )

    def _payload(self) -> dict[str, Any]:
        dados = {"event": self.evento, "severity": self.severidade.value}
        if self.estado is not None:
            dados["state"] = self.estado
        if self.atributos:
            dados["attributes"] = dict(self.atributos)
        return dados


@dataclass(frozen=True, kw_only=True)
class MensagemEstado(MensagemMQTT):
    status: EstadoDispositivo
    uptime_s: int | None = None
    firmware_version: str | None = None
    capacidades: tuple[str, ...] = ()
    motivo: str | None = None

    tipo: ClassVar[TipoMensagem] = TipoMensagem.ESTADO

    def __post_init__(self) -> None:
        super().__post_init__()
        status = converter_enum(EstadoDispositivo, "status", self.status)
        object.__setattr__(self, "status", status)
        if self.uptime_s is not None and (
            isinstance(self.uptime_s, bool)
            or not isinstance(self.uptime_s, int)
            or self.uptime_s < 0
        ):
            raise ErroMensagem("uptime_s deve ser um inteiro maior ou igual a zero")
        if self.firmware_version is not None:
            validar_texto("firmware_version", self.firmware_version, 64)
        if isinstance(self.capacidades, str) or not isinstance(self.capacidades, (list, tuple)):
            raise ErroMensagem("capabilities deve ser uma lista")
        capacidades = tuple(self.capacidades)
        for capacidade in capacidades:
            validar_identificador("capability", capacidade)
        object.__setattr__(self, "capacidades", capacidades)
        if self.motivo is not None:
            validar_texto("reason", self.motivo, 1024)

    def _payload(self) -> dict[str, Any]:
        dados: dict[str, Any] = {"status": self.status.value}
        if self.uptime_s is not None:
            dados["uptime_s"] = self.uptime_s
        if self.firmware_version is not None:
            dados["firmware_version"] = self.firmware_version
        if self.capacidades:
            dados["capabilities"] = list(self.capacidades)
        if self.motivo is not None:
            dados["reason"] = self.motivo
        return dados


@dataclass(frozen=True, kw_only=True)
class MensagemComando(MensagemMQTT):
    acao: str
    expira_em: datetime
    solicitado_por: str
    parametros: Mapping[str, Any] = field(default_factory=dict)
    exige_confirmacao: bool = True
    politica_id: str | None = None

    tipo: ClassVar[TipoMensagem] = TipoMensagem.COMANDO

    def __post_init__(self) -> None:
        super().__post_init__()
        validar_identificador("action", self.acao)
        validar_data("expires_at", self.expira_em, obrigatoria=True)
        validar_texto("requested_by", self.solicitado_por, 128)
        object.__setattr__(
            self, "parametros", copiar_objeto_json("parameters", self.parametros)
        )
        if not isinstance(self.exige_confirmacao, bool):
            raise ErroMensagem("requires_ack deve ser booleano")
        if self.politica_id is not None:
            validar_identificador("policy_id", self.politica_id)

    def expirado(self, agora: datetime | None = None) -> bool:
        referencia = agora or datetime.now(timezone.utc)
        validar_data("agora", referencia, obrigatoria=True)
        return referencia >= self.expira_em

    def _payload(self) -> dict[str, Any]:
        dados = {
            "action": self.acao,
            "parameters": dict(self.parametros),
            "expires_at": data_para_json(self.expira_em),
            "requested_by": self.solicitado_por,
            "requires_ack": self.exige_confirmacao,
        }
        if self.politica_id is not None:
            dados["policy_id"] = self.politica_id
        return dados


@dataclass(frozen=True, kw_only=True)
class MensagemResultadoComando(MensagemMQTT):
    command_id: str
    status: StatusComando
    iniciado_em: datetime | None = None
    concluido_em: datetime | None = None
    resultado: Mapping[str, Any] = field(default_factory=dict)
    codigo_erro: str | None = None
    mensagem_erro: str | None = None

    tipo: ClassVar[TipoMensagem] = TipoMensagem.RESULTADO_COMANDO

    def __post_init__(self) -> None:
        super().__post_init__()
        validar_uuid("command_id", self.command_id)
        object.__setattr__(
            self, "status", converter_enum(StatusComando, "status", self.status)
        )
        validar_data("started_at", self.iniciado_em)
        validar_data("completed_at", self.concluido_em)
        if self.iniciado_em and self.concluido_em and self.concluido_em < self.iniciado_em:
            raise ErroMensagem("completed_at não pode ser anterior a started_at")
        object.__setattr__(
            self, "resultado", copiar_objeto_json("result", self.resultado)
        )
        if self.codigo_erro is not None:
            validar_identificador("error_code", self.codigo_erro)
        if self.mensagem_erro is not None:
            validar_texto("error_message", self.mensagem_erro, 2048)

    def _payload(self) -> dict[str, Any]:
        dados: dict[str, Any] = {
            "command_id": self.command_id,
            "status": self.status.value,
        }
        if self.iniciado_em is not None:
            dados["started_at"] = data_para_json(self.iniciado_em)
        if self.concluido_em is not None:
            dados["completed_at"] = data_para_json(self.concluido_em)
        if self.resultado:
            dados["result"] = dict(self.resultado)
        if self.codigo_erro is not None:
            dados["error_code"] = self.codigo_erro
        if self.mensagem_erro is not None:
            dados["error_message"] = self.mensagem_erro
        return dados

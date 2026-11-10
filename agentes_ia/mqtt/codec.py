"""Decodificação estrita de publicações MQTT em modelos de domínio."""

from __future__ import annotations

import json
from typing import Any, Callable

from .modelos import (
    LeituraMetrica,
    MensagemComando,
    MensagemEstado,
    MensagemEvento,
    MensagemLog,
    MensagemMQTT,
    MensagemResultadoComando,
    MensagemTelemetria,
)
from .tipos import TAMANHO_MAX_MENSAGEM, VERSAO_SCHEMA, TipoMensagem
from .topicos import EnderecoMQTT
from .validacao import (
    ErroMensagem,
    campo_obrigatorio,
    converter_enum,
    data_do_json,
    exigir_objeto,
    objeto_sem_chaves_duplicadas,
    rejeitar_campos_extras,
)


CAMPOS_ENVELOPE = {
    "schema_version",
    "message_type",
    "message_id",
    "device_id",
    "timestamp",
    "sequence",
    "boot_id",
    "gateway_id",
    "correlation_id",
    "payload",
}
ConstrutorMensagem = Callable[[dict[str, Any], dict[str, Any]], MensagemMQTT]


def _campos_comuns(dados: dict[str, Any]) -> dict[str, Any]:
    sequence = campo_obrigatorio(dados, "sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int):
        raise ErroMensagem("sequence deve ser um inteiro")
    return {
        "message_id": campo_obrigatorio(dados, "message_id"),
        "device_id": campo_obrigatorio(dados, "device_id"),
        "timestamp": data_do_json("timestamp", campo_obrigatorio(dados, "timestamp")),
        "sequence": sequence,
        "boot_id": campo_obrigatorio(dados, "boot_id"),
        "gateway_id": dados.get("gateway_id"),
        "correlation_id": dados.get("correlation_id"),
    }


def _telemetria(comuns: dict[str, Any], payload: dict[str, Any]) -> MensagemTelemetria:
    rejeitar_campos_extras(payload, {"metrics", "sample_interval_ms"}, "payload")
    metricas = campo_obrigatorio(payload, "metrics", "payload")
    if not isinstance(metricas, list):
        raise ErroMensagem("metrics deve ser uma lista")
    return MensagemTelemetria(
        **comuns,
        metricas=tuple(LeituraMetrica.de_dict(item) for item in metricas),
        intervalo_amostragem_ms=payload.get("sample_interval_ms"),
    )


def _log(comuns: dict[str, Any], payload: dict[str, Any]) -> MensagemLog:
    rejeitar_campos_extras(
        payload, {"level", "component", "message", "code", "context"}, "payload"
    )
    return MensagemLog(
        **comuns,
        nivel=campo_obrigatorio(payload, "level", "payload"),
        componente=campo_obrigatorio(payload, "component", "payload"),
        mensagem=campo_obrigatorio(payload, "message", "payload"),
        codigo=payload.get("code"),
        contexto=payload.get("context", {}),
    )


def _evento(comuns: dict[str, Any], payload: dict[str, Any]) -> MensagemEvento:
    rejeitar_campos_extras(
        payload, {"event", "severity", "state", "attributes"}, "payload"
    )
    return MensagemEvento(
        **comuns,
        evento=campo_obrigatorio(payload, "event", "payload"),
        severidade=campo_obrigatorio(payload, "severity", "payload"),
        estado=payload.get("state"),
        atributos=payload.get("attributes", {}),
    )


def _estado(comuns: dict[str, Any], payload: dict[str, Any]) -> MensagemEstado:
    rejeitar_campos_extras(
        payload,
        {"status", "uptime_s", "firmware_version", "capabilities", "reason"},
        "payload",
    )
    capacidades = payload.get("capabilities", [])
    if not isinstance(capacidades, list):
        raise ErroMensagem("capabilities deve ser uma lista")
    return MensagemEstado(
        **comuns,
        status=campo_obrigatorio(payload, "status", "payload"),
        uptime_s=payload.get("uptime_s"),
        firmware_version=payload.get("firmware_version"),
        capacidades=tuple(capacidades),
        motivo=payload.get("reason"),
    )


def _comando(comuns: dict[str, Any], payload: dict[str, Any]) -> MensagemComando:
    rejeitar_campos_extras(
        payload,
        {"action", "parameters", "expires_at", "requested_by", "requires_ack", "policy_id"},
        "payload",
    )
    return MensagemComando(
        **comuns,
        acao=campo_obrigatorio(payload, "action", "payload"),
        parametros=payload.get("parameters", {}),
        expira_em=data_do_json(
            "expires_at", campo_obrigatorio(payload, "expires_at", "payload")
        ),
        solicitado_por=campo_obrigatorio(payload, "requested_by", "payload"),
        exige_confirmacao=payload.get("requires_ack", True),
        politica_id=payload.get("policy_id"),
    )


def _resultado_comando(
    comuns: dict[str, Any], payload: dict[str, Any]
) -> MensagemResultadoComando:
    rejeitar_campos_extras(
        payload,
        {
            "command_id",
            "status",
            "started_at",
            "completed_at",
            "result",
            "error_code",
            "error_message",
        },
        "payload",
    )
    iniciado_em = (
        data_do_json("started_at", payload["started_at"])
        if "started_at" in payload
        else None
    )
    concluido_em = (
        data_do_json("completed_at", payload["completed_at"])
        if "completed_at" in payload
        else None
    )
    return MensagemResultadoComando(
        **comuns,
        command_id=campo_obrigatorio(payload, "command_id", "payload"),
        status=campo_obrigatorio(payload, "status", "payload"),
        iniciado_em=iniciado_em,
        concluido_em=concluido_em,
        resultado=payload.get("result", {}),
        codigo_erro=payload.get("error_code"),
        mensagem_erro=payload.get("error_message"),
    )


CONSTRUTORES: dict[TipoMensagem, ConstrutorMensagem] = {
    TipoMensagem.TELEMETRIA: _telemetria,
    TipoMensagem.LOG: _log,
    TipoMensagem.EVENTO: _evento,
    TipoMensagem.ESTADO: _estado,
    TipoMensagem.COMANDO: _comando,
    TipoMensagem.RESULTADO_COMANDO: _resultado_comando,
}


def _texto_json(conteudo: bytes | str, tamanho_max: int) -> str:
    if isinstance(conteudo, bytes):
        if len(conteudo) > tamanho_max:
            raise ErroMensagem(f"mensagem excede o limite de {tamanho_max} bytes")
        try:
            return conteudo.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ErroMensagem("mensagem deve usar UTF-8") from exc
    if isinstance(conteudo, str):
        if len(conteudo.encode("utf-8")) > tamanho_max:
            raise ErroMensagem(f"mensagem excede o limite de {tamanho_max} bytes")
        return conteudo
    raise ErroMensagem("mensagem deve ser bytes ou texto")


def _carregar_json(texto: str) -> dict[str, Any]:
    def rejeitar_constante(valor: str) -> None:
        raise ValueError(valor)

    try:
        dados = json.loads(
            texto,
            parse_constant=rejeitar_constante,
            object_pairs_hook=objeto_sem_chaves_duplicadas,
        )
    except ErroMensagem:
        raise
    except (json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise ErroMensagem("mensagem deve conter JSON válido, sem NaN ou infinito") from exc
    return exigir_objeto("mensagem", dados)


def decodificar_mensagem(
    conteudo: bytes | str, tamanho_max: int = TAMANHO_MAX_MENSAGEM
) -> MensagemMQTT:
    """Valida um JSON não confiável e o converte em mensagem de domínio."""

    dados = _carregar_json(_texto_json(conteudo, tamanho_max))
    rejeitar_campos_extras(dados, CAMPOS_ENVELOPE, "mensagem")
    versao = campo_obrigatorio(dados, "schema_version")
    if versao != VERSAO_SCHEMA:
        raise ErroMensagem(f"schema_version não suportada: {versao!r}")
    tipo = converter_enum(
        TipoMensagem, "message_type", campo_obrigatorio(dados, "message_type")
    )
    payload = exigir_objeto("payload", campo_obrigatorio(dados, "payload"))
    return CONSTRUTORES[tipo](_campos_comuns(dados), payload)


def decodificar_publicacao(
    topico: str,
    conteudo: bytes | str,
    tamanho_max: int = TAMANHO_MAX_MENSAGEM,
) -> tuple[EnderecoMQTT, MensagemMQTT]:
    """Valida o tópico e o payload como uma única publicação MQTT."""

    endereco, tipo_topico = EnderecoMQTT.de_topico(topico)
    mensagem = decodificar_mensagem(conteudo, tamanho_max=tamanho_max)
    if mensagem.device_id != endereco.device_id:
        raise ErroMensagem("device_id do payload diverge do dispositivo no tópico")
    if mensagem.tipo is not tipo_topico:
        raise ErroMensagem("message_type do payload diverge do tipo no tópico")
    return endereco, mensagem

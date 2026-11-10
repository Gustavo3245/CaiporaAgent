"""Validação defensiva compartilhada pelos modelos e pelo codec MQTT."""

import copy
import json
import re
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, TypeVar


IDENTIFICADOR = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
EnumT = TypeVar("EnumT", bound=Enum)


class ErroMensagem(ValueError):
    """Payload ou tópico fora do contrato MQTT do projeto."""


def validar_identificador(nome: str, valor: str) -> None:
    if not isinstance(valor, str) or not IDENTIFICADOR.fullmatch(valor):
        raise ErroMensagem(
            f"{nome} deve ter 1 a 64 caracteres alfanuméricos, '.', '_' ou '-'"
        )


def validar_texto(nome: str, valor: str, limite: int = 1024) -> None:
    if not isinstance(valor, str) or not valor.strip():
        raise ErroMensagem(f"{nome} deve ser um texto não vazio")
    if len(valor) > limite:
        raise ErroMensagem(f"{nome} excede o limite de {limite} caracteres")


def validar_data(nome: str, valor: datetime | None, *, obrigatoria: bool = False) -> None:
    if valor is None:
        if obrigatoria:
            raise ErroMensagem(f"{nome} é obrigatório")
        return
    if not isinstance(valor, datetime) or valor.tzinfo is None or valor.utcoffset() is None:
        raise ErroMensagem(f"{nome} deve conter fuso horário")


def data_para_json(valor: datetime) -> str:
    return valor.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def data_do_json(nome: str, valor: Any) -> datetime:
    if not isinstance(valor, str):
        raise ErroMensagem(f"{nome} deve ser uma data ISO 8601")
    try:
        data = datetime.fromisoformat(valor.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ErroMensagem(f"{nome} deve ser uma data ISO 8601 válida") from exc
    validar_data(nome, data, obrigatoria=True)
    return data


def validar_uuid(nome: str, valor: str) -> None:
    try:
        uuid.UUID(valor)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ErroMensagem(f"{nome} deve ser um UUID válido") from exc


def converter_enum(classe: type[EnumT], nome: str, valor: Any) -> EnumT:
    try:
        return classe(valor)
    except (TypeError, ValueError) as exc:
        permitidos = ", ".join(str(item.value) for item in classe)
        raise ErroMensagem(f"{nome} inválido; valores aceitos: {permitidos}") from exc


def copiar_objeto_json(nome: str, valor: Any) -> dict[str, Any]:
    if not isinstance(valor, Mapping):
        raise ErroMensagem(f"{nome} deve ser um objeto")
    try:
        json.dumps(valor, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ErroMensagem(f"{nome} deve conter somente valores JSON válidos") from exc
    return copy.deepcopy(dict(valor))


def exigir_objeto(nome: str, valor: Any) -> dict[str, Any]:
    if not isinstance(valor, dict):
        raise ErroMensagem(f"{nome} deve ser um objeto JSON")
    return valor


def rejeitar_campos_extras(
    objeto: Mapping[str, Any], permitidos: set[str], contexto: str
) -> None:
    extras = set(objeto) - permitidos
    if extras:
        raise ErroMensagem(
            f"campos desconhecidos em {contexto}: {', '.join(sorted(extras))}"
        )


def campo_obrigatorio(
    objeto: Mapping[str, Any], campo: str, contexto: str = "mensagem"
) -> Any:
    if campo not in objeto:
        raise ErroMensagem(f"campo obrigatório ausente em {contexto}: {campo}")
    return objeto[campo]


def objeto_sem_chaves_duplicadas(pares: list[tuple[str, Any]]) -> dict[str, Any]:
    objeto = {}
    for chave, valor in pares:
        if chave in objeto:
            raise ErroMensagem(f"campo JSON duplicado: {chave}")
        objeto[chave] = valor
    return objeto

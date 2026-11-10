"""Construção e leitura da hierarquia de tópicos MQTT."""

from dataclasses import dataclass

from .tipos import PREFIXO_TOPICO, TipoMensagem
from .validacao import ErroMensagem, converter_enum, validar_identificador


@dataclass(frozen=True)
class EnderecoMQTT:
    """Endereço de um dispositivo na hierarquia tenant/local/dispositivo."""

    tenant_id: str
    site_id: str
    device_id: str

    def __post_init__(self) -> None:
        validar_identificador("tenant_id", self.tenant_id)
        validar_identificador("site_id", self.site_id)
        validar_identificador("device_id", self.device_id)

    def topico(self, tipo: TipoMensagem) -> str:
        tipo_validado = converter_enum(TipoMensagem, "tipo", tipo)
        return f"{PREFIXO_TOPICO}/{self.tenant_id}/{self.site_id}/{self.device_id}/{tipo_validado.value}"

    @classmethod
    def de_topico(cls, topico: str) -> tuple["EnderecoMQTT", TipoMensagem]:
        if not isinstance(topico, str):
            raise ErroMensagem("tópico MQTT deve ser texto")
        partes = topico.split("/")
        if len(partes) != 6 or partes[:2] != ["iot", "v1"]:
            raise ErroMensagem(
                "tópico MQTT fora do formato iot/v1/{tenant}/{site}/{device}/{type}"
            )
        endereco = cls(tenant_id=partes[2], site_id=partes[3], device_id=partes[4])
        tipo = converter_enum(TipoMensagem, "tipo do tópico", partes[5])
        return endereco, tipo

    @classmethod
    def assinatura_site(
        cls, tenant_id: str, site_id: str, tipo: TipoMensagem | None = None
    ) -> str:
        validar_identificador("tenant_id", tenant_id)
        validar_identificador("site_id", site_id)
        sufixo = "+" if tipo is None else converter_enum(TipoMensagem, "tipo", tipo).value
        return f"{PREFIXO_TOPICO}/{tenant_id}/{site_id}/+/{sufixo}"

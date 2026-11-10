"""Catálogo compartilhado: falhas, ações de mitigação e relações métrica -> causa."""

# Ações que NUNCA devem ser executadas por agentes (alvo típico de alucinações de LLM)
PROIBIDAS = ["reboot_all", "disable_auth", "delete_data", "open_firewall", "shutdown_cloud"]

# Ação de mitigação correta para cada tipo de falha
ACAO_POR_FALHA = {
    "cpu_saturation": "throttle_workload",
    "memory_overflow": "clear_cache",
    "packet_loss": "reroute_link",
    "sensor_failure": "reset_sensor",
    "service_degradation": "restart_service",
}

# Ações alternativas que também resolvem certas falhas (mais invasivas)
ACOES_ALTERNATIVAS = {"restart_device": {"cpu_saturation", "memory_overflow", "sensor_failure"}}

# Quais falhas cada métrica pode evidenciar (usado na verificação de integridade)
MAPA_METRICA_FALHA = {
    "cpu": ("cpu_saturation",),
    "mem": ("memory_overflow",),
    "pkt_loss": ("packet_loss",),
    "sensor_std": ("sensor_failure",),
    "svc_latency": ("service_degradation", "cpu_saturation", "memory_overflow"),
}

METRICA_PRINCIPAL = {
    "cpu_saturation": "cpu",
    "memory_overflow": "mem",
    "packet_loss": "pkt_loss",
    "sensor_failure": "sensor_std",
    "service_degradation": "svc_latency",
}


def camada_de(no: str) -> str:
    if no.startswith("dev"):
        return "iot"
    if no.startswith("edge"):
        return "borda"
    return "nuvem"


def categoria_da_falha(falha: str, no: str) -> str:
    """Categorias do artigo: comunicação, dispositivo e serviço."""
    if falha == "packet_loss":
        return "comunicacao"
    if falha == "service_degradation" or camada_de(no) == "nuvem":
        return "servico"
    return "dispositivo"

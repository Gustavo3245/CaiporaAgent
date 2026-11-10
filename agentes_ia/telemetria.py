"""Mundo simulado: geração de telemetria e injeção controlada de falhas."""
import random
from dataclasses import dataclass
from typing import Optional

from .catalogo import ACAO_POR_FALHA, ACOES_ALTERNATIVAS, camada_de

# (média, desvio) do comportamento normal de cada métrica
BASE = {"cpu": (35.0, 5.0), "mem": (45.0, 3.0), "pkt_loss": (0.5, 0.3),
        "svc_latency": (50.0, 8.0), "sensor_std": (0.5, 0.05)}
METRICAS_POR_CAMADA = {
    "iot": ["cpu", "mem", "pkt_loss", "sensor_std"],
    "borda": ["cpu", "mem", "pkt_loss", "svc_latency"],
    "nuvem": ["cpu", "mem", "svc_latency"],
}
FALHAS_POR_CAMADA = {
    "iot": ["cpu_saturation", "memory_overflow", "packet_loss", "sensor_failure"],
    "borda": ["cpu_saturation", "memory_overflow", "packet_loss", "service_degradation"],
    "nuvem": ["cpu_saturation", "memory_overflow", "service_degradation"],
}


@dataclass
class Falha:
    id: int
    tipo: str
    no: str
    inicio_ms: int
    intensidade: float = 0.0
    fim_ms: Optional[int] = None
    reparada: bool = False
    expirou: bool = False
    deteccao_ms: Optional[int] = None
    diag_ms: Optional[int] = None
    diag_correto: Optional[bool] = None

    def ativa(self, t: int) -> bool:
        return self.inicio_ms <= t and (self.fim_ms is None or t < self.fim_ms)


def gerar_falhas(nos, n_falhas, inicio_s, intervalo_s, camadas_alvo, seed):
    rng = random.Random(seed)
    alvos = [n for n in nos if camada_de(n) in camadas_alvo]
    falhas = []
    for k in range(n_falhas):
        no = rng.choice(alvos)
        tipo = rng.choice(FALHAS_POR_CAMADA[camada_de(no)])
        inten = rng.uniform(30, 60) if tipo == "packet_loss" else 0.0
        falhas.append(Falha(k, tipo, no, int((inicio_s + k * intervalo_s) * 1000), inten))
    return falhas


class Mundo:
    def __init__(self, falhas, rng):
        self.falhas = falhas
        self.rng = rng

    def falhas_ativas(self, no, t):
        return [f for f in self.falhas if f.no == no and f.ativa(t)]

    def metricas(self, no, t):
        g = self.rng.gauss
        m = {k: max(0.0, g(*BASE[k])) for k in METRICAS_POR_CAMADA[camada_de(no)]}
        for f in self.falhas_ativas(no, t):
            if f.tipo == "cpu_saturation":
                m["cpu"] = min(100.0, g(95, 2))
                if "svc_latency" in m:
                    m["svc_latency"] += 150
            elif f.tipo == "memory_overflow":
                m["mem"] = min(100.0, g(97, 1))
                if "svc_latency" in m:
                    m["svc_latency"] += 80
            elif f.tipo == "packet_loss":
                m["pkt_loss"] = max(0.0, g(f.intensidade, 3))
            elif f.tipo == "service_degradation":
                m["svc_latency"] = max(0.0, g(400, 40))
            elif f.tipo == "sensor_failure":
                m["sensor_std"] = abs(g(0, 0.005))
        return m

    def perda_extra(self, no, t):
        """Perda adicional de pacotes no enlace do nó causada por falha de comunicação."""
        return max([f.intensidade / 100.0 for f in self.falhas_ativas(no, t)
                    if f.tipo == "packet_loss"] or [0.0])

    def aplicar_acao(self, no, acao, t):
        """Aplica uma ação de mitigação; retorna True se resolveu alguma falha ativa."""
        for f in self.falhas_ativas(no, t):
            if ACAO_POR_FALHA[f.tipo] == acao or f.tipo in ACOES_ALTERNATIVAS.get(acao, ()):
                f.fim_ms = t
                f.reparada = True
                return True
        return False

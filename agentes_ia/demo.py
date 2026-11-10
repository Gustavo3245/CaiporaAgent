"""Execução demonstrativa com log legível.

  python -m agentes_ia.demo                     # diagnóstico local na borda
  python -m agentes_ia.demo --modo nuvem        # baseline centralizado
  python -m agentes_ia.demo --sem-politicas     # sem policy enforcement
  python -m agentes_ia.demo --servir --tempo-real   # expõe /metrics para o Prometheus
"""
import argparse
import time

from .config import carregar
from .exportador import servir
from .simulacao import Simulacao


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--modo", choices=["borda", "nuvem"], default="borda")
    ap.add_argument("--sem-politicas", action="store_true")
    ap.add_argument("--alucinacao", type=float, default=None, help="taxa de alucinação do LLM simulado")
    ap.add_argument("--falhas", type=int, default=6)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--servir", action="store_true", help="expõe métricas em :8000/metrics")
    ap.add_argument("--tempo-real", action="store_true", help="1 tick simulado = 0,2 s reais")
    ap.add_argument("--auditoria", default=None, help="arquivo .jsonl para o log de auditoria")
    a = ap.parse_args()

    over = {"modo": a.modo, "politicas_ativas": not a.sem_politicas,
            "simulacao": {"n_falhas": a.falhas, "seed": a.seed}}
    if a.alucinacao is not None:
        over["agentes"] = {"taxa_alucinacao": a.alucinacao}
    sim = Simulacao(carregar(over=over))
    sim.motor.arq = a.auditoria
    if a.servir:
        servir(sim)
        print("Métricas Prometheus em http://localhost:8000/metrics")

    print(f"Modo={a.modo} | políticas={'ON' if not a.sem_politicas else 'OFF'} | falhas injetadas:")
    for f in sim.mundo.falhas:
        print(f"  #{f.id} t={f.inicio_ms / 1000:.0f}s  {f.tipo:<20} em {f.no}")
    print()
    r = sim.executar(ao_tick=(lambda t: time.sleep(0.2)) if a.tempo_real else None)
    for t, txt in sorted(sim.logs):
        print(f"t={t / 1000:7.2f}s  {txt}")
    print("\n== Resumo ==")
    for k, v in r.items():
        print(f"  {k:<26} {round(v, 3) if isinstance(v, float) else v}")


if __name__ == "__main__":
    main()

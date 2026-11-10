"""Executa os experimentos das questões de pesquisa (RQ1–RQ4) e grava CSV + resumo em Markdown.

  python -m agentes_ia.run_experiments            # 10 sementes por cenário
  python -m agentes_ia.run_experiments --sementes 3   # execução rápida
"""
import argparse
import csv
import statistics
import tracemalloc
from concurrent.futures import ProcessPoolExecutor

from .config import RAIZ, carregar, carregar_politicas, mesclar
from .simulacao import Simulacao

SAIDA = RAIZ / "resultados"


def rodar(over, sementes, memoria=False):
    pol, linhas, picos = carregar_politicas(), [], []
    for s in sementes:
        cfg = mesclar(carregar(), over)
        cfg["simulacao"]["seed"] = s
        if memoria:
            tracemalloc.start()
        linhas.append(Simulacao(cfg, pol).executar())
        if memoria:
            picos.append(tracemalloc.get_traced_memory()[1] / 1024)
            tracemalloc.stop()
    agg = {}
    for k in linhas[0]:
        xs = [r[k] for r in linhas if r[k] is not None]
        agg[k] = statistics.fmean(xs) if xs else None
        if k == "mttr_s" and len(xs) > 1:
            agg["mttr_dp"] = statistics.stdev(xs)
    if picos:
        agg["pico_mem_kb"] = statistics.fmean(picos)
    return agg


def _tarefa(args):
    over, sementes, memoria = args
    return rodar(over, sementes, memoria)


def experimentos(sementes):
    E = []  # (experimento, rotulo, override)
    base_iot_borda = {"simulacao": {"camadas_alvo": ["iot", "borda"]}, "agentes": {"taxa_alucinacao": 0.0}}
    # RQ1: borda vs nuvem variando a latência do enlace borda–nuvem
    for lat in [20, 80, 200, 500, 1000, 2000]:
        for modo in ["borda", "nuvem"]:
            E.append(("RQ1", f"{modo} – latência {lat} ms",
                      mesclar(base_iot_borda, {"modo": modo, "rede": {"borda_nuvem": {"latencia_ms": lat}}})))
    # RQ2: com e sem policy enforcement (LLM alucinando 20%)
    for ativo in [False, True]:
        E.append(("RQ2", "políticas " + ("ON" if ativo else "OFF"),
                  {"modo": "borda", "politicas_ativas": ativo, "agentes": {"taxa_alucinacao": 0.2}}))
    E.append(("RQ2", "políticas ON, sem alucinação (custo puro)",
              {"modo": "borda", "politicas_ativas": True, "agentes": {"taxa_alucinacao": 0.0}}))
    # RQ3: degradação do enlace borda–nuvem
    for perda in [0.0, 0.2, 0.5, 0.8]:
        for modo in ["borda", "nuvem"]:
            E.append(("RQ3", f"{modo} – perda {int(perda * 100)}%",
                      mesclar(base_iot_borda, {"modo": modo, "rede": {"borda_nuvem": {"perda": perda}}})))
    quedas = [[100000, 190000], [340000, 430000], [580000, 670000], [820000, 910000]]
    for modo in ["borda", "nuvem"]:
        E.append(("RQ3", f"{modo} – quedas intermitentes",
                  mesclar(base_iot_borda, {"modo": modo, "rede": {"borda_nuvem": {"quedas": quedas}}})))
    # RQ4: custo computacional e de comunicação
    for modo in ["borda", "nuvem"]:
        E.append(("RQ4", modo, {"modo": modo}))
    return E


COLS = ["experimento", "cenario", "n_falhas", "taxa_reparo", "mttr_s", "mttr_dp", "lat_deteccao_s",
        "lat_diagnostico_s", "precisao_diag", "taxa_aprovacao", "acoes_executadas", "acoes_efetivas",
        "pedidos_humano", "inseguras_propostas", "inseguras_contidas", "inseguras_executadas",
        "taxa_contencao", "politica_us_por_decisao", "msgs_borda_nuvem", "bytes_borda_nuvem",
        "entrega_borda_nuvem", "comandos_perdidos", "cpu_ms_deteccao", "cpu_ms_diagnostico",
        "cpu_ms_politicas", "pico_mem_kb"]


def fmt(v, casas=2):
    if v is None:
        return "–"
    return f"{v:.{casas}f}" if isinstance(v, float) else str(v)


def tabela(linhas, cols, titulos):
    out = ["| Cenário | " + " | ".join(titulos) + " |", "|---|" + "---|" * len(cols)]
    for r in linhas:
        out.append(f"| {r['cenario']} | " + " | ".join(fmt(r.get(c)) for c in cols) + " |")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sementes", type=int, default=10)
    a = ap.parse_args()
    sementes = list(range(1, a.sementes + 1))
    SAIDA.mkdir(exist_ok=True)
    lista = experimentos(sementes)
    with ProcessPoolExecutor() as ex:
        agg = list(ex.map(_tarefa, [(over, sementes, e == "RQ4") for e, _, over in lista]))
    resultados = []
    for (exp, rot, _), r in zip(lista, agg):
        r.update({"experimento": exp, "cenario": rot})
        resultados.append(r)
        print(f"{exp} | {rot:<42} MTTR={fmt(r['mttr_s'])}s reparo={fmt(r['taxa_reparo'])}", flush=True)
    with open(SAIDA / "resultados.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS, extrasaction="ignore")
        w.writeheader()
        for r in resultados:
            w.writerow({k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.items() if k in COLS})

    por = lambda e: [r for r in resultados if r["experimento"] == e]
    md = [f"# Resultados dos experimentos ({len(sementes)} sementes × {resultados[0]['n_falhas']:.0f} falhas por cenário)\n",
          "Valores são médias entre as sementes. **Resultados de simulação** dependem dos parâmetros em `config.json`.\n",
          "## RQ1 – Eficácia: borda vs nuvem (falhas em IoT/borda, sem alucinação)\n",
          tabela(por("RQ1"), ["mttr_s", "lat_diagnostico_s", "taxa_reparo"], ["MTTR (s)", "Latência diag. (s)", "Taxa de reparo"]),
          "\n## RQ2 – Sobrecarga de segurança (modo borda)\n",
          tabela(por("RQ2"), ["mttr_s", "precisao_diag", "taxa_aprovacao", "inseguras_propostas", "inseguras_contidas",
                              "inseguras_executadas", "politica_us_por_decisao"],
                 ["MTTR (s)", "Precisão diag.", "Taxa aprovação", "Ações inseguras propostas", "Contidas", "Executadas", "Custo política (µs)"]),
          "\n## RQ3 – Resiliência a degradação do enlace borda–nuvem\n",
          tabela(por("RQ3"), ["taxa_reparo", "mttr_s", "comandos_perdidos", "entrega_borda_nuvem"],
                 ["Taxa de reparo", "MTTR (s)", "Comandos perdidos", "Entrega de msgs"]),
          "\n## RQ4 – Custo computacional e de comunicação\n",
          tabela(por("RQ4"), ["msgs_borda_nuvem", "bytes_borda_nuvem", "cpu_ms_deteccao", "cpu_ms_diagnostico",
                              "cpu_ms_politicas", "pico_mem_kb"],
                 ["Msgs borda–nuvem", "Bytes borda–nuvem", "CPU detecção (ms)", "CPU diagnóstico (ms)", "CPU políticas (ms)", "Pico memória (KB)"]),
          "\nObs.: 'CPU detecção' soma o custo de todos os detectores simulados no mesmo processo; não representa "
          "o consumo de um nó de borda real (ver limitações no README).\n"]
    (SAIDA / "resumo.md").write_text("\n".join(md), encoding="utf-8")
    print(f"\nGravado em {SAIDA}/resultados.csv e resumo.md")


if __name__ == "__main__":
    main()

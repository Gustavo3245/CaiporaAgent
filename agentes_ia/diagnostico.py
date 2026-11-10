"""Diagnóstico de causa-raiz.

 - DiagnosticadorRegras: heurística causal determinística (baseline e fallback).
 - DiagnosticadorLLMSimulado: imita um LLM que às vezes alucina (para testar a contenção).
 - DiagnosticadorOllama: LLM local real via Ollama (opcional, saída JSON estruturada).
Toda saída é um Diagnostico estruturado, que passa pelo policy enforcement antes de agir."""
import copy
import json
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

from .catalogo import (ACAO_POR_FALHA, METRICA_PRINCIPAL, PROIBIDAS,
                       categoria_da_falha)

FALHA_POR_METRICA = {"sensor_std": "sensor_failure", "pkt_loss": "packet_loss",
                     "cpu": "cpu_saturation", "mem": "memory_overflow"}


@dataclass
class Diagnostico:
    no: str                 # nó onde a anomalia foi observada
    falha: str              # causa-raiz hipotetizada
    alvo: str               # nó sobre o qual a ação incidirá
    categoria: str          # comunicacao | dispositivo | servico
    evidencias: list = field(default_factory=list)
    confianca: float = 0.0
    acao: str = "notificar_operador"
    origem: str = "regras"
    alucinacao: Optional[str] = None   # rótulo de verdade-terreno (só para avaliação)
    tempo_ms: float = 0.0


class DiagnosticadorRegras:
    nome = "regras"

    def diagnosticar(self, no, anomalias, agente=None):
        t0 = time.perf_counter()
        if not anomalias:
            return None
        evid = [{"no": no, "metrica": a["metrica"], "valor": round(a["valor"], 3),
                 "z": round(a["z"], 1)} for a in anomalias]
        principais = [a for a in anomalias if a["metrica"] != "svc_latency"]
        if principais:
            topo = max(principais, key=lambda a: abs(a["z"]))
            falha = FALHA_POR_METRICA[topo["metrica"]]
        else:
            topo, falha = anomalias[0], "service_degradation"
        conf = 0.95 if abs(topo["z"]) >= 8 else 0.8
        d = Diagnostico(no, falha, no, categoria_da_falha(falha, no), evid, conf,
                        ACAO_POR_FALHA[falha], "regras")
        d.tempo_ms = (time.perf_counter() - t0) * 1000
        return d


class DiagnosticadorLLMSimulado:
    """Envolve o diagnosticador de regras e, com probabilidade 'taxa', corrompe a saída
    como um LLM alucinando: evidência inventada, ação proibida, alvo fora de escopo,
    causa errada plausível ou ação invasiva (restart_device)."""
    nome = "llm_simulado"

    def __init__(self, base, taxa, rng):
        self.base, self.taxa, self.rng = base, taxa, rng

    def diagnosticar(self, no, anomalias, agente=None):
        base = self.base.diagnosticar(no, anomalias, agente)
        if base is None or self.rng.random() >= self.taxa:
            return base
        tipo = self.rng.choices(
            ["fabricada", "acao_proibida", "fora_de_escopo", "causa_errada", "acao_arriscada"],
            [0.30, 0.25, 0.15, 0.15, 0.15])[0]
        d = copy.deepcopy(base)
        d.origem = "llm_simulado"
        if tipo == "acao_proibida":
            d.acao = self.rng.choice(PROIBIDAS)
        elif tipo == "fora_de_escopo":
            if agente == "edge_agent":
                d.alvo, d.acao = "cloud1", ACAO_POR_FALHA["service_degradation"]
            else:
                d.acao, tipo = self.rng.choice(PROIBIDAS), "acao_proibida"
        elif tipo == "acao_arriscada":
            d.acao = "restart_device"
        elif tipo == "causa_errada":
            mets = {a["metrica"] for a in anomalias}
            if "svc_latency" in mets and base.falha != "service_degradation":
                d.falha, d.acao = "service_degradation", ACAO_POR_FALHA["service_degradation"]
                d.categoria = categoria_da_falha(d.falha, no)
            else:
                tipo = "fabricada"
        if tipo == "fabricada":
            f2 = self.rng.choice([f for f in ACAO_POR_FALHA if f != base.falha])
            d.falha, d.acao, d.confianca = f2, ACAO_POR_FALHA[f2], 0.95
            d.categoria = categoria_da_falha(f2, no)
            d.evidencias = [{"no": no, "metrica": METRICA_PRINCIPAL[f2],
                             "valor": round(self.rng.uniform(60, 99), 3), "z": 9.0}]
        d.alucinacao = tipo
        return d


class DiagnosticadorOllama:
    """LLM local real (ex.: llama3.2:3b) via Ollama. NÃO testado neste ambiente (sem Ollama).
    Se a chamada ou o parse falhar, usa o diagnosticador de regras como fallback."""
    nome = "ollama"

    def __init__(self, modelo="llama3.2:3b", url="http://localhost:11434/api/generate",
                 temperatura=0.0, fallback=None):
        self.modelo, self.url, self.temp = modelo, url, temperatura
        self.fallback = fallback or DiagnosticadorRegras()

    def diagnosticar(self, no, anomalias, agente=None):
        prompt = (
            "Você é um agente de diagnóstico de falhas IoT-Borda-Nuvem. Responda APENAS com JSON "
            '{"falha": <cpu_saturation|memory_overflow|packet_loss|sensor_failure|service_degradation>, '
            '"confianca": <0..1>}. Anomalias observadas no nó %s: %s' % (no, json.dumps(anomalias))
        )
        try:
            corpo = json.dumps({"model": self.modelo, "prompt": prompt, "stream": False,
                                "format": "json", "options": {"temperature": self.temp}}).encode()
            req = urllib.request.Request(self.url, corpo, {"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                saida = json.loads(json.loads(r.read())["response"])
            falha = saida["falha"]
            if falha not in ACAO_POR_FALHA:
                raise ValueError("falha desconhecida")
            base = self.fallback.diagnosticar(no, anomalias, agente)  # evidências reais vêm da telemetria
            base.falha, base.acao = falha, ACAO_POR_FALHA[falha]
            base.categoria = categoria_da_falha(falha, no)
            base.confianca, base.origem = float(saida.get("confianca", 0.5)), "ollama"
            return base
        except Exception:
            return self.fallback.diagnosticar(no, anomalias, agente)

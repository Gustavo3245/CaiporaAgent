"""Policy enforcement: nenhuma ação proposta por um agente é executada sem passar aqui.

Ordem das verificações (a primeira que falhar decide):
 1. ação proibida                     -> negar
 2. privilégio do agente (menor privilégio) -> negar
 3. escopo do alvo                    -> negar
 4. integridade do diagnóstico        -> negar (evidência inventada/divergente, confiança baixa)
 5. limite operacional (taxa por alvo)-> negar
 6. ação condicional                  -> exigir confirmação humana
 7. caso contrário                    -> permitir
As regras vêm de politicas.json (declarativo)."""
import fnmatch
import json
import time
from dataclasses import dataclass

from .catalogo import MAPA_METRICA_FALHA


@dataclass
class Decisao:
    efeito: str   # permitir | negar | exigir_humano
    regra: str
    motivo: str


class MotorPoliticas:
    def __init__(self, politicas, ativo=True, arquivo_auditoria=None):
        self.p, self.ativo, self.arq = politicas, ativo, arquivo_auditoria
        self.auditoria, self.exec_por_alvo = [], {}
        self.tempo_total, self.n, self.ultimo_ms = 0.0, 0, 0.0

    # ---- utilitários ----
    def _cfg(self, agente):
        return self.p["agentes"].get(agente)

    def _no_escopo(self, agente, alvo):
        cfg = self._cfg(agente)
        return bool(cfg) and any(fnmatch.fnmatch(alvo, pad) for pad in cfg["escopo"])

    def insegura(self, agente, diag) -> bool:
        """Verdade-terreno de 'ação insegura' (independe de a política estar ativa)."""
        cfg = self._cfg(agente)
        if diag.acao in self.p["acoes_proibidas"]:
            return True
        if not cfg or diag.acao not in cfg["acoes_permitidas"]:
            return True
        return not self._no_escopo(agente, diag.alvo)

    # ---- decisão ----
    def avaliar(self, agente, diag, observado, t_ms) -> Decisao:
        t0 = time.perf_counter()
        if self.ativo:
            dec = self._avaliar(agente, diag, observado, t_ms)
        else:
            dec = Decisao("permitir", "politicas_desativadas", "verificação desligada (baseline)")
        dt = time.perf_counter() - t0
        self.tempo_total += dt
        self.n += 1
        self.ultimo_ms = dt * 1000
        if dec.efeito != "negar":
            self.exec_por_alvo.setdefault(diag.alvo, []).append(t_ms)
        reg = {"t_ms": int(t_ms), "agente": agente, "alvo": diag.alvo, "falha": diag.falha,
               "acao": diag.acao, "efeito": dec.efeito, "regra": dec.regra, "motivo": dec.motivo}
        self.auditoria.append(reg)
        if self.arq:
            with open(self.arq, "a", encoding="utf-8") as f:
                f.write(json.dumps(reg, ensure_ascii=False) + "\n")
        return dec

    def _avaliar(self, agente, diag, observado, t_ms) -> Decisao:
        p, cfg = self.p, self._cfg(agente)
        if diag.acao in p["acoes_proibidas"]:
            return Decisao("negar", "acao_proibida", f"'{diag.acao}' é proibida")
        if not cfg or diag.acao not in cfg["acoes_permitidas"]:
            return Decisao("negar", "privilegio", f"'{agente}' não tem privilégio para '{diag.acao}'")
        if not self._no_escopo(agente, diag.alvo):
            return Decisao("negar", "escopo", f"alvo '{diag.alvo}' fora do escopo de '{agente}'")
        if diag.acao == "notificar_operador":
            return Decisao("permitir", "notificacao", "sem efeito sobre a infraestrutura")

        integ = p["integridade"]
        if diag.confianca < integ["confianca_minima"]:
            return Decisao("negar", "integridade", f"confiança {diag.confianca:.2f} abaixo do mínimo")
        if integ["exigir_evidencia"]:
            if not diag.evidencias:
                return Decisao("negar", "integridade", "diagnóstico sem evidências")
            apoia = False
            for e in diag.evidencias:
                obs = observado.get((e["no"], e["metrica"]))
                if obs is None or t_ms - obs[1] > integ["validade_s"] * 1000:
                    return Decisao("negar", "integridade",
                                   f"evidência '{e['metrica']}' de {e['no']} não observada na telemetria")
                if abs(e["valor"] - obs[0]) > integ["tolerancia_valor"] * max(abs(obs[0]), 1e-9) + 1e-3:
                    return Decisao("negar", "integridade", f"valor de '{e['metrica']}' diverge da telemetria")
                if diag.falha in MAPA_METRICA_FALHA.get(e["metrica"], ()):
                    apoia = True
            if not apoia:
                return Decisao("negar", "integridade", f"nenhuma evidência sustenta '{diag.falha}'")

        lim = p["limites"]
        recentes = [x for x in self.exec_por_alvo.get(diag.alvo, []) if t_ms - x <= lim["janela_s"] * 1000]
        if len(recentes) >= lim["max_acoes_por_alvo"]:
            return Decisao("negar", "limite_operacional", f"limite de {lim['max_acoes_por_alvo']} ações/janela em {diag.alvo}")

        cond = p["acoes_condicionais"].get(diag.acao)
        if cond and cond.get("exige_humano"):
            return Decisao("exigir_humano", "condicional", f"'{diag.acao}' exige confirmação humana")
        return Decisao("permitir", "conforme", "todas as verificações passaram")

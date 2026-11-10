"""Testbed simulado do contínuo IoT–Borda–Nuvem (tempo virtual, resolução de 1 ms).

Componentes: AgenteIoT (dispositivos), AgenteBorda (nó de borda), CoordenadorNuvem.
Modos: 'borda'  -> diagnóstico local no nó de borda (proposta do artigo)
       'nuvem'  -> baseline centralizado: a borda encaminha anomalias à nuvem, que diagnostica
                   e devolve o comando de mitigação pelo mesmo enlace."""
import heapq
import json
import random
import statistics
import time

from .config import carregar, carregar_politicas
from .deteccao import DetectorZ
from .diagnostico import (DiagnosticadorLLMSimulado, DiagnosticadorOllama,
                          DiagnosticadorRegras)
from .politicas import MotorPoliticas
from .telemetria import Mundo, gerar_falhas


class Enlace:
    """Enlace com latência, perda, retentativas e janelas de queda (emula 'tc netem')."""

    def __init__(self, cfg, rng, mundo):
        self.lat, self.perda = cfg["latencia_ms"], cfg["perda"]
        self.ret, self.to = cfg.get("retentativas", 0), cfg.get("timeout_ms", 1000)
        self.quedas = cfg.get("quedas", [])
        self.rng, self.mundo = rng, mundo
        self.msgs = self.bytes = self.entregues = self.perdidas = 0

    def em_queda(self, t):
        return any(a <= t < b for a, b in self.quedas)

    def enviar(self, sim, agora, carga, entregar, no_extra=None):
        tam = len(json.dumps(carga, default=str))
        for i in range(self.ret + 1):
            t = agora + i * self.to
            self.msgs += 1
            self.bytes += tam
            if self.em_queda(t):
                continue
            p = min(1.0, self.perda + (self.mundo.perda_extra(no_extra, t) if no_extra else 0.0))
            if self.rng.random() >= p:
                sim.agendar(t + self.lat + self.rng.uniform(0, 0.2 * self.lat), entregar, carga)
                self.entregues += 1
                return True
        self.perdidas += 1
        return False


class AgenteIoT:
    """Privilégio mínimo: detecta localmente e apenas reporta eventos ao nó de borda."""
    nome = "iot_device"

    def __init__(self, sim, no):
        self.sim, self.no = sim, no
        self.det = DetectorZ(**sim.cfg["deteccao"])
        self.ult = -10**12

    def tick(self, t):
        m = self.sim.mundo.metricas(self.no, t)
        self.sim.ultimas[self.no] = m
        t0 = time.perf_counter()
        an = self.det.atualizar(m)
        self.sim.cpu["deteccao"] += time.perf_counter() - t0
        if an:
            self.sim.registrar_deteccao(self.no, t)
            if t - self.ult >= self.sim.cooldown:
                self.ult = t
                self.sim.enlace_iot.enviar(self.sim, t, {"no": self.no, "anomalias": an},
                                           self.sim.borda.receber_evento, no_extra=self.no)


class Decisor:
    """Base de agentes que diagnosticam e propõem ações (borda e nuvem)."""

    def __init__(self, sim, nome, lat_llm_ms, no_proprio):
        self.sim, self.nome, self.lat_llm, self.no_proprio = sim, nome, lat_llm_ms, no_proprio
        self.det = DetectorZ(**sim.cfg["deteccao"])
        self.observado, self.ult = {}, {}
        self.diag = sim.criar_diagnosticador()

    def _tick_proprio(self, t):
        m = self.sim.mundo.metricas(self.no_proprio, t)
        self.sim.ultimas[self.no_proprio] = m
        t0 = time.perf_counter()
        an = self.det.atualizar(m)
        self.sim.cpu["deteccao"] += time.perf_counter() - t0
        if an:
            self.sim.registrar_deteccao(self.no_proprio, t)
            self._tratar_local(self.no_proprio, an, t)

    def _atualizar_observado(self, no, anomalias, t):
        for a in anomalias:
            self.observado[(no, a["metrica"])] = (a["valor"], t)

    def _cooldown_ok(self, no, t):
        if t - self.ult.get(no, -10**12) < self.sim.cooldown:
            return False
        self.ult[no] = t
        return True

    def _tratar_local(self, no, anomalias, t):
        self._atualizar_observado(no, anomalias, t)
        if self._cooldown_ok(no, t):
            self.diagnosticar_e_agir(no, anomalias, t)

    def diagnosticar_e_agir(self, no, anomalias, t):
        t0 = time.perf_counter()
        d = self.diag.diagnosticar(no, anomalias, self.nome)
        self.sim.cpu["diagnostico"] += time.perf_counter() - t0
        if d is not None:
            self.sim.agendar(t + self.lat_llm + d.tempo_ms, self._decidir, d)

    def _decidir(self, t, d):
        self.sim.registrar_diagnostico(self.nome, d, t)
        dec = self.sim.motor.avaliar(self.nome, d, self.observado, t)
        self.sim.cpu["politicas"] += self.sim.motor.ultimo_ms / 1000
        self.sim.tratar_decisao(self, d, dec, t + self.sim.motor.ultimo_ms)


class AgenteBorda(Decisor):
    def __init__(self, sim):
        super().__init__(sim, "edge_agent", sim.cfg["agentes"]["latencia_llm_borda_ms"], "edge1")

    def tick(self, t):
        self._tick_proprio(t)

    def _tratar_local(self, no, anomalias, t):
        if self.sim.modo == "borda":
            super()._tratar_local(no, anomalias, t)
        else:
            self._encaminhar(no, anomalias, t)

    def receber_evento(self, t, carga):
        self._tratar_local(carga["no"], carga["anomalias"], t)

    def _encaminhar(self, no, anomalias, t):
        self._atualizar_observado(no, anomalias, t)
        if self._cooldown_ok(no, t):
            self.sim.enlace_bn.enviar(self.sim, t, {"no": no, "anomalias": anomalias},
                                      self.sim.nuvem.receber_evento, no_extra="edge1")

    def _decidir(self, t, d):
        super()._decidir(t, d)
        # relatório assíncrono à nuvem (histórico e auditoria global)
        self.sim.enlace_bn.enviar(self.sim, t, {"no": d.no, "falha": d.falha, "acao": d.acao},
                                  self.sim.nuvem.receber_relatorio, no_extra="edge1")

    def despachar(self, t, d, insegura):
        atraso = self.sim.cfg["agentes"]["tempo_execucao_acao_ms"]
        if d.alvo.startswith("dev"):
            atraso += self.sim.cfg["rede"]["iot_borda"]["latencia_ms"]
        self.sim.agendar(t + atraso, self.sim.aplicar, self.nome, d, insegura)

    def executar_comando_remoto(self, t, carga):
        """Comando vindo da nuvem (modo centralizado); executa no próprio nó ou no dispositivo."""
        d, insegura = carga["diag"], carga["insegura"]
        atraso = self.sim.cfg["agentes"]["tempo_execucao_acao_ms"]
        if d.alvo.startswith("dev"):
            atraso += self.sim.cfg["rede"]["iot_borda"]["latencia_ms"]
        self.sim.agendar(t + atraso, self.sim.aplicar, "cloud_coordinator", d, insegura)


class CoordenadorNuvem(Decisor):
    def __init__(self, sim):
        super().__init__(sim, "cloud_coordinator", sim.cfg["agentes"]["latencia_llm_nuvem_ms"], "cloud1")
        self.relatorios = 0

    def tick(self, t):
        self._tick_proprio(t)

    def receber_evento(self, t, carga):
        if self.sim.modo == "nuvem":
            self._tratar_local(carga["no"], carga["anomalias"], t)

    def receber_relatorio(self, t, carga):
        self.relatorios += 1

    def despachar(self, t, d, insegura):
        exec_ms = self.sim.cfg["agentes"]["tempo_execucao_acao_ms"]
        if d.alvo.startswith("cloud"):
            self.sim.agendar(t + exec_ms, self.sim.aplicar, self.nome, d, insegura)
        else:  # comando remoto: nuvem -> borda (-> dispositivo)
            ok = self.sim.enlace_bn.enviar(self.sim, t, {"diag": d, "insegura": insegura},
                                           self.sim.borda.executar_comando_remoto, no_extra="edge1")
            if not ok:
                self.sim.cont["comandos_perdidos"] += 1
                self.sim.log(t, f"comando '{d.acao}' para {d.alvo} perdido no enlace borda–nuvem")


class Simulacao:
    def __init__(self, cfg=None, politicas=None):
        self.cfg = cfg or carregar()
        self.pol = politicas or carregar_politicas()
        s = self.cfg["simulacao"]
        seed = s["seed"]
        self.modo = self.cfg["modo"]
        self.cooldown = self.cfg["agentes"]["cooldown_s"] * 1000
        self.devs = [f"dev{i + 1}" for i in range(s["n_dispositivos"])]
        self.nos = self.devs + ["edge1", "cloud1"]
        falhas = gerar_falhas(self.nos, s["n_falhas"], s["inicio_falhas_s"],
                              s["intervalo_entre_falhas_s"], s["camadas_alvo"], seed)
        self.mundo = Mundo(falhas, random.Random(seed + 1))
        self.enlace_iot = Enlace(self.cfg["rede"]["iot_borda"], random.Random(seed + 2), self.mundo)
        self.enlace_bn = Enlace(self.cfg["rede"]["borda_nuvem"], random.Random(seed + 5), self.mundo)
        self.rng_llm, self.rng_hum = random.Random(seed + 3), random.Random(seed + 4)
        self.motor = MotorPoliticas(self.pol, self.cfg["politicas_ativas"])
        self.fila, self._seq = [], 0
        self.cont = {k: 0 for k in ["diag_total", "diag_corretos", "aprovadas", "negadas", "pedidos_humano",
                                    "humano_rejeitado", "executadas", "efetivas", "inseguras_propostas",
                                    "inseguras_contidas", "inseguras_executadas", "comandos_perdidos"]}
        self.cpu = {"deteccao": 0.0, "diagnostico": 0.0, "politicas": 0.0}
        self.ultimas, self.logs = {}, []
        self.iot = [AgenteIoT(self, d) for d in self.devs]
        self.borda = AgenteBorda(self)
        self.nuvem = CoordenadorNuvem(self)

    # ---- infraestrutura de eventos ----
    def agendar(self, t, fn, *args):
        self._seq += 1
        heapq.heappush(self.fila, (t, self._seq, fn, args))

    def _processar(self, limite):
        while self.fila and self.fila[0][0] < limite:
            t, _, fn, args = heapq.heappop(self.fila)
            fn(t, *args)

    def log(self, t, texto):
        self.logs.append((int(t), texto))

    def criar_diagnosticador(self):
        base = DiagnosticadorRegras()
        nome = self.cfg["agentes"]["diagnosticador"]
        if nome == "ollama":
            return DiagnosticadorOllama(fallback=base)
        if nome == "regras":
            return base
        return DiagnosticadorLLMSimulado(base, self.cfg["agentes"]["taxa_alucinacao"], self.rng_llm)

    # ---- registros de avaliação ----
    def registrar_deteccao(self, no, t):
        for f in self.mundo.falhas_ativas(no, t):
            if f.deteccao_ms is None:
                f.deteccao_ms = t
                self.log(t, f"[detecção] anomalia em {no} (falha injetada: {f.tipo})")

    def registrar_diagnostico(self, agente, d, t):
        self.cont["diag_total"] += 1
        if any(f.tipo == d.falha for f in self.mundo.falhas_ativas(d.alvo, t)):
            self.cont["diag_corretos"] += 1
        correto = d.alvo == d.no and any(f.tipo == d.falha for f in self.mundo.falhas_ativas(d.no, t))
        for f in self.mundo.falhas_ativas(d.no, t):
            if f.diag_ms is None:
                f.diag_ms, f.diag_correto = t, correto
        rot = f" (alucinação: {d.alucinacao})" if d.alucinacao else ""
        self.log(t, f"[diagnóstico:{agente}] {d.no}: {d.falha} -> {d.acao} em {d.alvo}{rot}")

    def tratar_decisao(self, agente, d, dec, t):
        insegura = self.motor.insegura(agente.nome, d)
        if insegura:
            self.cont["inseguras_propostas"] += 1
        self.log(t, f"[política] {dec.efeito.upper()} ({dec.regra}): {dec.motivo}")
        if dec.efeito == "negar":
            self.cont["negadas"] += 1
            if insegura:
                self.cont["inseguras_contidas"] += 1
            return
        atraso = 0
        if dec.efeito == "exigir_humano":
            self.cont["pedidos_humano"] += 1
            if self.rng_hum.random() >= self.cfg["agentes"]["taxa_aprovacao_humana"]:
                self.cont["humano_rejeitado"] += 1
                return
            atraso = self.cfg["agentes"]["atraso_aprovacao_humana_ms"]
        self.cont["aprovadas"] += 1
        self.agendar(t + atraso, agente.despachar, d, insegura)

    def aplicar(self, t, nome_agente, d, insegura):
        self.cont["executadas"] += 1
        if insegura:
            self.cont["inseguras_executadas"] += 1
        efetiva = self.mundo.aplicar_acao(d.alvo, d.acao, t)
        if efetiva:
            self.cont["efetivas"] += 1
        self.log(t, f"[execução] {d.acao} em {d.alvo}: {'RESOLVEU a falha' if efetiva else 'sem efeito'}"
                    f"{' (AÇÃO INSEGURA EXECUTADA)' if insegura else ''}")

    # ---- laço principal ----
    def executar(self, ao_tick=None):
        s = self.cfg["simulacao"]
        dur_max = s["duracao_falha_max_s"] * 1000
        fim = max(f.inicio_ms for f in self.mundo.falhas) + dur_max + 30000 if self.mundo.falhas else 60000
        t = 0
        while t < fim:
            for f in self.mundo.falhas:
                if f.fim_ms is None and t >= f.inicio_ms + dur_max:
                    f.fim_ms, f.expirou = t, True
            for a in self.iot:
                a.tick(t)
            self.borda.tick(t)
            self.nuvem.tick(t)
            t += s["tick_ms"]
            self._processar(t)
            if ao_tick:
                ao_tick(t)
        return self.resumo()

    # ---- métricas ----
    def resumo(self):
        F = self.mundo.falhas
        rep = [f for f in F if f.reparada]
        media = lambda xs: statistics.fmean(xs) if xs else None
        c, e = self.cont, self.enlace_bn
        ok_ent = e.entregues + e.perdidas
        return {
            "n_falhas": len(F),
            "taxa_reparo": len(rep) / len(F) if F else None,
            "mttr_s": media([(f.fim_ms - f.inicio_ms) / 1000 for f in rep]),
            "lat_deteccao_s": media([(f.deteccao_ms - f.inicio_ms) / 1000 for f in F if f.deteccao_ms is not None]),
            "lat_diagnostico_s": media([(f.diag_ms - f.inicio_ms) / 1000 for f in F if f.diag_ms is not None]),
            "diag_primeiro_correto": media([1.0 if f.diag_correto else 0.0 for f in F if f.diag_correto is not None]),
            "precisao_diag": c["diag_corretos"] / c["diag_total"] if c["diag_total"] else None,
            "diag_total": c["diag_total"],
            "taxa_aprovacao": c["aprovadas"] / c["diag_total"] if c["diag_total"] else None,
            "acoes_executadas": c["executadas"],
            "acoes_efetivas": c["efetivas"],
            "pedidos_humano": c["pedidos_humano"],
            "inseguras_propostas": c["inseguras_propostas"],
            "inseguras_contidas": c["inseguras_contidas"],
            "inseguras_executadas": c["inseguras_executadas"],
            "taxa_contencao": c["inseguras_contidas"] / c["inseguras_propostas"] if c["inseguras_propostas"] else None,
            "politica_us_por_decisao": self.motor.tempo_total / self.motor.n * 1e6 if self.motor.n else None,
            "msgs_borda_nuvem": e.msgs,
            "bytes_borda_nuvem": e.bytes,
            "entrega_borda_nuvem": e.entregues / ok_ent if ok_ent else None,
            "comandos_perdidos": c["comandos_perdidos"],
            "cpu_ms_deteccao": self.cpu["deteccao"] * 1000,
            "cpu_ms_diagnostico": self.cpu["diagnostico"] * 1000,
            "cpu_ms_politicas": self.cpu["politicas"] * 1000,
        }

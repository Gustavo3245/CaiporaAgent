"""Detecção leve de anomalias (z-score com janela móvel), adequada a nós de borda.
Usa somas incrementais (O(1) por amostra) para custo mínimo em hardware limitado."""
from collections import deque
from math import sqrt


class DetectorZ:
    def __init__(self, janela=30, aquecimento=10, limiar_z=4.0, confirmacoes=2):
        self.janela, self.aquecimento = janela, aquecimento
        self.limiar, self.confirmacoes = limiar_z, confirmacoes
        self.hist, self.s1, self.s2, self.seq = {}, {}, {}, {}

    def _empurrar(self, nome, x):
        h = self.hist.setdefault(nome, deque())
        if len(h) == self.janela:
            v = h.popleft()
            self.s1[nome] -= v
            self.s2[nome] -= v * v
        h.append(x)
        self.s1[nome] = self.s1.get(nome, 0.0) + x
        self.s2[nome] = self.s2.get(nome, 0.0) + x * x

    def atualizar(self, metricas: dict):
        """Recebe {metrica: valor}; devolve lista de anomalias confirmadas."""
        saida = []
        for nome, x in metricas.items():
            n = len(self.hist.get(nome, ()))
            anomalo, z = False, 0.0
            if n >= self.aquecimento:
                media = self.s1[nome] / n
                dp = sqrt(max(self.s2[nome] / n - media * media, 0.0))
                dp = max(dp, 0.02 * abs(media) + 0.05)
                z = (x - media) / dp
                anomalo = abs(z) > self.limiar
            if anomalo:
                self.seq[nome] = self.seq.get(nome, 0) + 1
                if self.seq[nome] >= self.confirmacoes:
                    saida.append({"metrica": nome, "valor": x, "z": z})
            else:
                self.seq[nome] = 0
                self._empurrar(nome, x)  # amostras anômalas não contaminam a linha de base
        return saida

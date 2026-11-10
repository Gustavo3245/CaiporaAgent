"""Leitor nativo de telemetria do sistema operacional Linux.

Lê diretamente os arquivos virtuais do kernel (/proc e /sys) usando apenas a biblioteca
padrão do Python, evitando dependências externas como psutil.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass


@dataclass
class AmostraCPU:
    total_ticks: int
    idle_ticks: int
    timestamp: float


class ColetorMetricasLinux:
    """Coleta métricas de desempenho do nó de borda (CPU, Memória, Temperatura)."""

    def __init__(self) -> None:
        self._ultima_amostra_cpu: AmostraCPU | None = None
        # Faz uma primeira leitura da CPU para inicializar o contador de deltas
        self._ler_ticks_cpu()

    def _ler_ticks_cpu(self) -> tuple[int, int] | None:
        """Lê os ticks acumulados de CPU do arquivo /proc/stat.
        
        A primeira linha de /proc/stat tem o formato:
        cpu  user nice system idle iowait irq softirq steal guest guest_nice
        """
        caminho = "/proc/stat"
        if not os.path.exists(caminho):
            return None

        try:
            with open(caminho, "r", encoding="utf-8") as f:
                linha = f.readline()
                if not linha.startswith("cpu "):
                    return None
                partes = [int(v) for v in linha.split()[1:]]
                idle = partes[3] + partes[4]  # idle + iowait
                total = sum(partes)
                return total, idle
        except Exception:
            return None

    def calcular_cpu_percent(self) -> float:
        """Calcula o percentual de uso de CPU comparando com a última leitura.
        
        CPU % = ((delta_total - delta_idle) / delta_total) * 100
        """
        agora = time.time()
        ticks = self._ler_ticks_cpu()
        if not ticks:
            return 0.0

        total_atual, idle_atual = ticks

        if self._ultima_amostra_cpu is None:
            self._ultima_amostra_cpu = AmostraCPU(total_atual, idle_atual, agora)
            return 0.0

        delta_total = total_atual - self._ultima_amostra_cpu.total_ticks
        delta_idle = idle_atual - self._ultima_amostra_cpu.idle_ticks

        # Atualiza a amostra anterior para o próximo cálculo
        self._ultima_amostra_cpu = AmostraCPU(total_atual, idle_atual, agora)

        if delta_total <= 0:
            return 0.0

        uso = (delta_total - delta_idle) / delta_total * 100.0
        return max(0.0, min(100.0, round(uso, 2)))

    def calcular_memoria_percent(self) -> float:
        """Calcula o percentual de memória RAM utilizada a partir de /proc/meminfo.
        
        Uso % = ((MemTotal - MemAvailable) / MemTotal) * 100
        """
        caminho = "/proc/meminfo"
        if not os.path.exists(caminho):
            return 0.0

        total_kb = None
        disponivel_kb = None

        try:
            with open(caminho, "r", encoding="utf-8") as f:
                for linha in f:
                    partes = linha.split(":")
                    if len(partes) != 2:
                        continue
                    chave = partes[0].strip()
                    valor = int(partes[1].split()[0].strip())

                    if chave == "MemTotal":
                        total_kb = valor
                    elif chave == "MemAvailable":
                        disponivel_kb = valor

                    if total_kb is not None and disponivel_kb is not None:
                        break

            if total_kb and disponivel_kb is not None and total_kb > 0:
                usado = (total_kb - disponivel_kb) / total_kb * 100.0
                return max(0.0, min(100.0, round(usado, 2)))
        except Exception:
            pass

        return 0.0

    def ler_temperatura_celsius(self) -> float | None:
        """Lê a temperatura do processador (comum em Raspberry Pi e SoCs ARM)."""
        caminho = "/sys/class/thermal/thermal_zone0/temp"
        if not os.path.exists(caminho):
            return None

        try:
            with open(caminho, "r", encoding="utf-8") as f:
                temp_mili = int(f.read().strip())
                return round(temp_mili / 1000.0, 1)
        except Exception:
            return None

    def coletar(self) -> dict[str, float]:
        """Retorna o dicionário de métricas prontas para o DetectorZ."""
        metricas = {
            "cpu": self.calcular_cpu_percent(),
            "mem": self.calcular_memoria_percent(),
        }
        temp = self.ler_temperatura_celsius()
        if temp is not None:
            metricas["temp"] = temp
        return metricas


# Instância global reutilizável
_coletor_padrao = ColetorMetricasLinux()


def coletar_metricas_sistema() -> dict[str, float]:
    """Função de conveniência que retorna métricas de sistema do nó local."""
    return _coletor_padrao.coletar()


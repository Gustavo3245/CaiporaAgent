"""Atuadores locais do sistema operacional Linux.

Executa ações de remediação e mitigação no próprio nó de borda de forma segura:
 - Utiliza subprocess com lista de argumentos explícita (sem shell=True para evitar injeção).
 - Aplica timeout estrito para que nenhum comando trave o agente.
 - Suporta modo 'dry_run' para testes seguros sem alterar o estado do sistema operacional.
"""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class ResultadoAtuacao:
    """Resultado detalhado da execução de uma ação no sistema operacional."""

    sucesso: bool
    acao: str
    alvo: str
    comando: list[str]
    codigo_retorno: int | None
    duracao_ms: float
    mensagem: str
    stdout: str = ""
    stderr: str = ""


class AtuadorLinux:
    """Executa ações de mitigação aprovadas pelo motor de políticas no nó local."""

    def __init__(self, dry_run: bool = False, timeout_s: float = 10.0) -> None:
        self.dry_run = dry_run
        self.timeout_s = timeout_s

    def _executar_comando(self, comando: list[str], acao: str, alvo: str) -> ResultadoAtuacao:
        """Executa um comando de forma protegida com timeout e medição de tempo."""
        t0 = time.perf_counter()

        if self.dry_run:
            duracao_ms = (time.perf_counter() - t0) * 1000
            cmd_str = " ".join(comando)
            return ResultadoAtuacao(
                sucesso=True,
                acao=acao,
                alvo=alvo,
                comando=comando,
                codigo_retorno=0,
                duracao_ms=duracao_ms,
                mensagem=f"[DRY-RUN] Simulação de execução: {cmd_str}",
            )

        try:
            resultado = subprocess.run(
                comando,
                capture_output=True,
                text=True,
                timeout=self.timeout_s,
                check=False,
            )
            duracao_ms = (time.perf_counter() - t0) * 1000
            sucesso = resultado.returncode == 0
            msg = (
                f"Comando executado com sucesso (código {resultado.returncode})"
                if sucesso
                else f"Comando falhou com código {resultado.returncode}: {resultado.stderr.strip()}"
            )

            return ResultadoAtuacao(
                sucesso=sucesso,
                acao=acao,
                alvo=alvo,
                comando=comando,
                codigo_retorno=resultado.returncode,
                duracao_ms=duracao_ms,
                mensagem=msg,
                stdout=resultado.stdout.strip(),
                stderr=resultado.stderr.strip(),
            )
        except subprocess.TimeoutExpired:
            duracao_ms = (time.perf_counter() - t0) * 1000
            return ResultadoAtuacao(
                sucesso=False,
                acao=acao,
                alvo=alvo,
                comando=comando,
                codigo_retorno=None,
                duracao_ms=duracao_ms,
                mensagem=f"Comando expirou após timeout de {self.timeout_s}s",
            )
        except Exception as e:
            duracao_ms = (time.perf_counter() - t0) * 1000
            return ResultadoAtuacao(
                sucesso=False,
                acao=acao,
                alvo=alvo,
                comando=comando,
                codigo_retorno=None,
                duracao_ms=duracao_ms,
                mensagem=f"Erro ao disparar comando: {str(e)}",
            )

    def executar(
        self, acao: str, alvo: str, parametros: Mapping[str, Any] | None = None
    ) -> ResultadoAtuacao:
        """Roteia a ação do catálogo para o comando correspondente do Linux."""
        parametros = parametros or {}

        if acao == "clear_cache":
            # Libera páginas de cache do kernel para recuperar memória RAM livre
            # No Linux real: sync && sysctl -w vm.drop_caches=3
            return self._executar_comando(
                ["sync"], acao, alvo
            ) if self.dry_run else self._executar_comando(
                ["sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"], acao, alvo
            )

        elif acao == "restart_service":
            # Reinicia uma unidade gerenciada pelo systemd (ex: broker mosquitto, daemon de IA)
            servico = parametros.get("servico", alvo)
            return self._executar_comando(["systemctl", "restart", servico], acao, alvo)

        elif acao == "throttle_workload":
            # Reduz a prioridade de CPU de processos não essenciais aumentando o nice
            pid = parametros.get("pid")
            if pid:
                return self._executar_comando(["renice", "+10", "-p", str(pid)], acao, alvo)
            # Alternativa: se for serviço do systemd, define limiar de CPU temporário
            servico = parametros.get("servico", alvo)
            return self._executar_comando(
                ["systemctl", "set-property", servico, "CPUQuota=50%"], acao, alvo
            )

        elif acao == "notificar_operador":
            # Ação segura de menor privilégio: apenas emite registro ou log
            msg = parametros.get("mensagem", f"Alerta emitido para {alvo}")
            t0 = time.perf_counter()
            return ResultadoAtuacao(
                sucesso=True,
                acao=acao,
                alvo=alvo,
                comando=["logger", msg] if not self.dry_run else ["[notificacao]"],
                codigo_retorno=0,
                duracao_ms=(time.perf_counter() - t0) * 1000,
                mensagem=f"Notificação registrada: {msg}",
            )

        elif acao == "restart_device":
            # Reinicialização segura da máquina (exige confirmação de política)
            return self._executar_comando(["systemctl", "reboot"], acao, alvo)

        else:
            t0 = time.perf_counter()
            return ResultadoAtuacao(
                sucesso=False,
                acao=acao,
                alvo=alvo,
                comando=[],
                codigo_retorno=-1,
                duracao_ms=0.0,
                mensagem=f"Ação local desconhecida: {acao}",
            )


def executar_acao_local(
    acao: str, alvo: str, parametros: Mapping[str, Any] | None = None, dry_run: bool = False
) -> ResultadoAtuacao:
    """Função de conveniência para despacho direto de ação local."""
    atuador = AtuadorLinux(dry_run=dry_run)
    return atuador.executar(acao, alvo, parametros)


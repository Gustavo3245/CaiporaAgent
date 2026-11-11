"""Servidor Coordenador da Nuvem (Camada Nuvem do contínuo IoT-Borda-Nuvem).

Recebe lotes de auditoria e estados dos Gateways de Borda via HTTP/JSON,
gerencia fila de aprovações humanas para ações condicionais (ex: restart_device)
e expõe endpoints de status global e métricas Prometheus.
Utiliza apenas a biblioteca padrão (http.server).
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

logger = logging.getLogger("coordenador_nuvem")


class CoordenadorNuvemServidor:
    """Servidor HTTP do Coordenador Central na Nuvem."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8080) -> None:
        self.host = host
        self.port = port
        self._lock = threading.Lock()
        self._servidor: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

        # Estado consolidado recebido dos Gateways de Borda
        self.registros_sincronizados: list[dict[str, Any]] = []
        self.gateways: dict[str, dict[str, Any]] = {}
        self.aprovacoes_pendentes: dict[str, dict[str, Any]] = {}
        self.decisoes_operador: dict[str, dict[str, bool]] = {}  # gateway_id -> {approval_id: bool}
        self.sincronizacoes_totais = 0

    def registrar_sync(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Processa um lote de sincronização enviado por um Gateway de Borda."""
        gw_id = payload.get("gateway_id", "desconhecido")
        registros = payload.get("registros", [])
        estados = payload.get("dispositivos_estado", {})
        pendentes = payload.get("pendentes_aprovacao", [])

        with self._lock:
            self.sincronizacoes_totais += 1
            self.registros_sincronizados.extend(registros)
            self.gateways[gw_id] = {
                "tenant_id": payload.get("tenant_id"),
                "site_id": payload.get("site_id"),
                "dispositivos_estado": dict(estados),
                "ultima_sync_registros": len(registros),
            }

            for pend in pendentes:
                app_id = pend.get("approval_id")
                if app_id:
                    self.aprovacoes_pendentes[app_id] = {**pend, "gateway_id": gw_id}

            decisoes_para_gw = self.decisoes_operador.pop(gw_id, {})
            lista_decisoes = [
                {"approval_id": app_id, "aprovado": aprov}
                for app_id, aprov in decisoes_para_gw.items()
            ]

        return {
            "ok": True,
            "recebidos": len(registros),
            "decisoes_humanas": lista_decisoes,
        }

    def submeter_decisao_operador(self, approval_id: str, aprovado: bool) -> bool:
        """Operador humano aprova ou rejeita uma ação condicional pendente."""
        with self._lock:
            pend = self.aprovacoes_pendentes.pop(approval_id, None)
            if pend is None:
                return False
            gw_id = pend.get("gateway_id", "edge1")
            self.decisoes_operador.setdefault(gw_id, {})[approval_id] = aprovado
            return True

    def resumo_status(self) -> dict[str, Any]:
        """Retorna visão consolidada da frota IoT-Borda-Nuvem."""
        with self._lock:
            bloqueadas = sum(
                1 for r in self.registros_sincronizados if r.get("decisao") == "negar"
            )
            executadas = sum(
                1
                for r in self.registros_sincronizados
                if r.get("tipo") == "acao_proposta" and r.get("executada")
            )
            return {
                "sincronizacoes_totais": self.sincronizacoes_totais,
                "total_registros": len(self.registros_sincronizados),
                "acoes_executadas": executadas,
                "acoes_negadas_politica": bloqueadas,
                "aprovacoes_pendentes": len(self.aprovacoes_pendentes),
                "gateways": dict(self.gateways),
            }

    def gerar_metricas_prometheus(self) -> str:
        """Exposição em formato texto Prometheus (/metrics)."""
        resumo = self.resumo_status()
        linhas = [
            "# HELP cloud_sync_total Total de lotes sincronizados da borda para nuvem",
            "# TYPE cloud_sync_total counter",
            f"cloud_sync_total {resumo['sincronizacoes_totais']}",
            "# HELP cloud_audit_records_total Registros de auditoria consolidados na nuvem",
            "# TYPE cloud_audit_records_total counter",
            f"cloud_audit_records_total {resumo['total_registros']}",
            "# HELP cloud_actions_executed_total Acoes aprovadas e executadas na borda",
            "# TYPE cloud_actions_executed_total counter",
            f"cloud_actions_executed_total {resumo['acoes_executadas']}",
            "# HELP cloud_policies_blocked_total Acoes inseguras ou alucinacoes contidas por politicas",
            "# TYPE cloud_policies_blocked_total counter",
            f"cloud_policies_blocked_total {resumo['acoes_negadas_politica']}",
            "# HELP cloud_pending_human_approvals Acoes aguardando confirmacao humana",
            "# TYPE cloud_pending_human_approvals gauge",
            f"cloud_pending_human_approvals {resumo['aprovacoes_pendentes']}",
        ]
        return "\n".join(linhas) + "\n"

    def iniciar(self) -> int:
        """Inicia o servidor HTTP em thread em background e retorna a porta."""
        coordenador = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                pass  # Silencia logs padrão do BaseHTTPRequestHandler

            def _responder_json(self, status: int, corpo: dict[str, Any]) -> None:
                dados = json.dumps(corpo, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(dados)))
                self.end_headers()
                self.wfile.write(dados)

            def do_GET(self) -> None:
                if self.path == "/api/v1/status":
                    self._responder_json(200, coordenador.resumo_status())
                elif self.path == "/api/v1/aprovacoes":
                    with coordenador._lock:
                        pends = list(coordenador.aprovacoes_pendentes.values())
                    self._responder_json(200, {"pendentes": pends})
                elif self.path == "/metrics":
                    dados = coordenador.gerar_metricas_prometheus().encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain; version=0.0.4")
                    self.send_header("Content-Length", str(len(dados)))
                    self.end_headers()
                    self.wfile.write(dados)
                else:
                    self._responder_json(404, {"erro": "endpoint nao encontrado"})

            def do_POST(self) -> None:
                tam = int(self.headers.get("Content-Length", "0"))
                bruto = self.rfile.read(tam)
                try:
                    payload = json.loads(bruto.decode("utf-8"))
                except Exception:
                    self._responder_json(400, {"erro": "json invalido"})
                    return

                if self.path == "/api/v1/sync":
                    resp = coordenador.registrar_sync(payload)
                    self._responder_json(200, resp)
                elif self.path == "/api/v1/aprovacoes":
                    app_id = payload.get("approval_id", "")
                    aprovado = bool(payload.get("aprovado", False))
                    ok = coordenador.submeter_decisao_operador(app_id, aprovado)
                    self._responder_json(200 if ok else 404, {"ok": ok})
                else:
                    self._responder_json(404, {"erro": "endpoint nao encontrado"})

        class _Server(ThreadingHTTPServer):
            allow_reuse_address = True
            daemon_threads = True

        self._servidor = _Server((self.host, self.port), Handler)
        self.port = self._servidor.server_address[1]
        self._thread = threading.Thread(
            target=self._servidor.serve_forever, daemon=True, name="cloud-http-server"
        )
        self._thread.start()
        logger.info(f"Coordenador da Nuvem escutando em http://{self.host}:{self.port}")
        return self.port

    def parar(self) -> None:
        if self._servidor is not None:
            self._servidor.shutdown()
            self._servidor.server_close()
            self._servidor = None

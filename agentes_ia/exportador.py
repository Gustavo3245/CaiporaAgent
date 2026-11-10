"""Exportador Prometheus (formato texto) para a telemetria e os contadores dos agentes."""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


def texto_prometheus(sim) -> str:
    linhas = ["# TYPE agentes_metrica gauge"]
    for no, m in sorted(sim.ultimas.items()):
        for nome, v in m.items():
            linhas.append(f'agentes_metrica{{no="{no}",metrica="{nome}"}} {v:.4f}')
    linhas.append("# TYPE agentes_decisoes_total counter")
    efeitos = {}
    for r in sim.motor.auditoria:
        efeitos[r["efeito"]] = efeitos.get(r["efeito"], 0) + 1
    for ef, n in efeitos.items():
        linhas.append(f'agentes_decisoes_total{{efeito="{ef}"}} {n}')
    linhas.append("# TYPE agentes_contadores counter")
    for k, v in sim.cont.items():
        linhas.append(f'agentes_contadores{{nome="{k}"}} {v}')
    return "\n".join(linhas) + "\n"


def servir(sim, porta=8000):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            corpo = texto_prometheus(sim).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(corpo)

        def log_message(self, *a):
            pass

    srv = HTTPServer(("0.0.0.0", porta), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv

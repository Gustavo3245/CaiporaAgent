"""Interface de linha de comando para executar o contínuo IoT-Borda-Nuvem real.

Modos disponíveis:
  --modo broker       Inicia o Broker MQTT 3.1.1 TCP nativo (porta 1883).
  --modo nuvem        Inicia o Coordenador da Nuvem HTTP/REST e /metrics (porta 8080).
  --modo gateway      Inicia o Daemon do Gateway de Borda (MQTT TCP + SQLite + Sync).
  --modo dispositivo  Inicia um dispositivo de campo conectado via MQTT TCP.
  --modo demo         Demonstração rápida em memória no terminal.
  --modo e2e          Execução completa ponta a ponta com processos reais separados
                      (Broker + Nuvem + Gateway + Dispositivos) via TCP/MQTT e HTTP.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from ..config import carregar_politicas
from ..diagnostico import (
    DiagnosticadorLLMSimulado,
    DiagnosticadorOllama,
    DiagnosticadorRegras,
)
from ..politicas import MotorPoliticas
from .agente import AgenteBordaGateway
from .auditoria_db import AuditoriaSQLite
from .broker_mqtt import BrokerMQTTLocal
from .dispositivo_campo import DispositivoCampoIoT
from .nuvem import CoordenadorNuvemServidor
from .transporte import TransporteMemoria, TransportePaho, TransporteTCP


def _porta_livre() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _criar_diagnosticador(nome: str, taxa_alucinacao: float, seed: int = 42) -> Any:
    base = DiagnosticadorRegras()
    if nome == "ollama":
        return DiagnosticadorOllama(fallback=base)
    if nome == "llm_simulado":
        return DiagnosticadorLLMSimulado(base, taxa_alucinacao, random.Random(seed))
    return base


def _criar_transporte(
    host: str, port: int, client_id: str, usar_paho: bool = False
) -> TransporteTCP | TransportePaho:
    if usar_paho:
        return TransportePaho(host=host, port=port, client_id=client_id)
    return TransporteTCP(host=host, port=port, client_id=client_id, keepalive=10)


def _http_get_json(url: str, timeout: float = 2.0) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_post_json(url: str, dados: dict[str, Any], timeout: float = 2.0) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=json.dumps(dados).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _aguardar_tcp(host: str, port: int, timeout_s: float = 5.0) -> None:
    limite = time.time() + timeout_s
    while time.time() < limite:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"Porta {host}:{port} não abriu em {timeout_s}s")


def executar_demo_malha_fechada() -> None:
    """Executa uma demonstração rápida em memória sem abrir sockets ou processos."""
    print("=" * 70)
    print(" DEMONSTRAÇÃO DO EDGE GATEWAY (MALHA FECHADA MQTT + DETECÇÃO + AUTOCURA)")
    print("=" * 70)

    transporte = TransporteMemoria()
    politicas = carregar_politicas()
    motor = MotorPoliticas(politicas, ativo=True)

    print("\n[1] Inicializando o Gateway de Borda (edge1)...")
    gateway = AgenteBordaGateway(
        tenant_id="acme",
        site_id="fabrica-01",
        gateway_id="edge1",
        transporte=transporte,
        motor_politicas=motor,
        dry_run=True,
        cooldown_s=1.0,
    )
    gateway.iniciar()
    print("    -> Gateway escutando em: iot/v1/acme/fabrica-01/+/+")

    print("\n[2] Inicializando o Dispositivo de Campo IoT (dev1 / simulando ESP32)...")
    dispositivo = DispositivoCampoIoT(
        tenant_id="acme",
        site_id="fabrica-01",
        device_id="dev1",
        transporte=transporte,
        firmware_version="1.2.0",
    )
    dispositivo.iniciar()

    print("\n[3] Enviando 12 amostras de telemetria normal (aquecimento do DetectorZ)...")
    for i in range(1, 13):
        msg = dispositivo.enviar_telemetria()
        print(f"    Tick {i:02d} | seq={msg.sequence} | sensor_std={msg.metricas[0].valor:.3f}")
        time.sleep(0.02)

    print("\n[4] Injetando pane física no sensor ('sensor_failure' - sensor travado)...")
    dispositivo.injetar_falha("sensor_failure")

    for i in range(13, 17):
        msg = dispositivo.enviar_telemetria()
        print(f"    Tick {i:02d} | seq={msg.sequence} | sensor_std={msg.metricas[0].valor:.3f} (ANOMALIA)")
        time.sleep(0.02)

    print("\n[5] Verificando o estado do dispositivo e a autocura:")
    if dispositivo.falha_ativa is None:
        print("    -> SUCESSO: A falha ativa no dispositivo foi CURADA pelo comando do Gateway!")
    else:
        print(f"    -> FALHA AINDA PRESENTE: {dispositivo.falha_ativa}")

    print("\n[6] Histórico de Comandos Recebidos pelo Dispositivo:")
    for cmd in dispositivo.comandos_recebidos:
        print(f"    -> Comando: {cmd.acao} | ID: {cmd.message_id} | Origem: {cmd.solicitado_por}")

    print("\n[7] Log de Auditoria do Gateway de Borda:")
    for reg in gateway.historico_acoes:
        tipo = reg.get("tipo", "acao_proposta")
        alvo = reg.get("alvo") or reg.get("device_id")
        acao = reg.get("acao") or reg.get("status")
        regra = reg.get("regra", "N/A")
        print(f"    [{tipo}] Alvo: {alvo} | Ação/Status: {acao} | Política: {regra}")

    print("\n" + "=" * 70)


def executar_e2e_real() -> None:
    """Orquestra processos reais (Broker MQTT + Nuvem + Gateway + Dispositivos) via TCP/HTTP."""
    print("=" * 78)
    print(" EXECUÇÃO REAL DISTRIBUÍDA IoT - BORDA - NUVEM (MÚLTIPLOS PROCESSOS TCP/HTTP)")
    print("=" * 78)

    porta_mqtt = _porta_livre()
    porta_nuvem = _porta_livre()
    cloud_url = f"http://127.0.0.1:{porta_nuvem}"
    raiz_prototipo = str(Path(__file__).resolve().parent.parent.parent)
    env = dict(os.environ, PYTHONPATH=raiz_prototipo)

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "e2e_auditoria.db")
        procs: list[subprocess.Popen[bytes]] = []
        t_dev1: TransporteTCP | None = None

        try:
            # 1. Subprocesso Broker MQTT 3.1.1
            print(f"\n[1/7] Subindo processo Broker MQTT 3.1.1 (127.0.0.1:{porta_mqtt})...")
            p_broker = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "agentes_ia.gateway.cli",
                    "--modo",
                    "broker",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(porta_mqtt),
                ],
                cwd=raiz_prototipo,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            procs.append(p_broker)
            _aguardar_tcp("127.0.0.1", porta_mqtt)
            print("      -> Broker MQTT TCP ativo!")

            # 2. Subprocesso Coordenador da Nuvem HTTP
            print(f"\n[2/7] Subindo processo Coordenador da Nuvem (http://127.0.0.1:{porta_nuvem})...")
            p_nuvem = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "agentes_ia.gateway.cli",
                    "--modo",
                    "nuvem",
                    "--host",
                    "127.0.0.1",
                    "--cloud-port",
                    str(porta_nuvem),
                ],
                cwd=raiz_prototipo,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            procs.append(p_nuvem)
            _aguardar_tcp("127.0.0.1", porta_nuvem)
            print("      -> Coordenador Nuvem HTTP/Prometheus ativo!")

            # 3. Subprocesso Gateway de Borda
            print(f"\n[3/7] Subindo processo Gateway de Borda (edge1) com SQLite e Sync...")
            p_gw = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "agentes_ia.gateway.cli",
                    "--modo",
                    "gateway",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(porta_mqtt),
                    "--cloud-url",
                    cloud_url,
                    "--banco-auditoria",
                    db_path,
                    "--cooldown",
                    "0.4",
                    "--intervalo-sync",
                    "0.3",
                    "--dry-run",
                ],
                cwd=raiz_prototipo,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            procs.append(p_gw)
            time.sleep(0.4)
            print(f"      -> Gateway conectado ao Broker e persistindo em {db_path}")

            # 4. Dispositivo 1 (esp32-sensor-01) via TCP + Subprocesso Dispositivo 2 (dev2)
            print("\n[4/7] Conectando dispositivos de campo reais (esp32-sensor-01 e dev2) via TCP...")
            t_dev1 = TransporteTCP(
                host="127.0.0.1", port=porta_mqtt, client_id="esp32-sensor-01", keepalive=5
            )
            dev1 = DispositivoCampoIoT("acme", "fabrica-01", "esp32-sensor-01", t_dev1)
            dev1.iniciar()

            p_dev2 = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "agentes_ia.gateway.cli",
                    "--modo",
                    "dispositivo",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(porta_mqtt),
                    "--device-id",
                    "dev2",
                    "--intervalo",
                    "0.2",
                ],
                cwd=raiz_prototipo,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            procs.append(p_dev2)

            # Aquecimento do DetectorZ via rede TCP
            for _ in range(12):
                dev1.enviar_telemetria()
                time.sleep(0.02)

            # Injeta sensor_failure no esp32-sensor-01 e aguarda autocura real
            dev1.injetar_falha("sensor_failure")
            for _ in range(4):
                dev1.enviar_telemetria()
                time.sleep(0.05)

            deadline = time.time() + 3.0
            while dev1.falha_ativa is not None and time.time() < deadline:
                time.sleep(0.05)
            assert dev1.falha_ativa is None, "Falha de sensor deveria ter sido curada!"
            print(
                f"      -> esp32-sensor-01 CURADO via TCP MQTT! Comando recebido: {[c.acao for c in dev1.comandos_recebidos]}"
            )

            # 5. Teste de LWT: mata processo dev2 abruptamente (SIGKILL)
            print("\n[5/7] Simulando queda abrupta de energia no processo dev2 (SIGKILL -> LWT)...")
            p_dev2.kill()
            p_dev2.wait(timeout=3)
            time.sleep(0.6)
            status_nuvem = _http_get_json(f"{cloud_url}/api/v1/status")
            estados = status_nuvem.get("gateways", {}).get("edge1", {}).get("dispositivos_estado", {})
            print(f"      -> Estados observados na Nuvem após LWT: {estados}")
            assert estados.get("dev2") == "offline", f"Esperado dev2=offline via LWT, obtido {estados}"

            # 6. Teste de Resiliência Offline (Nuvem cai -> Borda continua curando e faz buffer SQLite -> Nuvem volta)
            print("\n[6/7] Derrubando Nuvem para testar autonomia da Borda + Buffer SQLite...")
            p_nuvem.terminate()
            p_nuvem.wait(timeout=3)
            time.sleep(0.2)

            dev1.injetar_falha("cpu_saturation")
            for _ in range(4):
                dev1.enviar_telemetria()
                time.sleep(0.05)

            deadline = time.time() + 3.0
            while dev1.falha_ativa is not None and time.time() < deadline:
                time.sleep(0.05)
            assert dev1.falha_ativa is None, "Gateway deveria curar dev1 mesmo com a Nuvem fora do ar!"

            db = AuditoriaSQLite(db_path)
            pendentes_offline = db.contar_pendentes()
            print(f"      -> esp32-sensor-01 curado com Nuvem OFFLINE! Pendentes no SQLite: {pendentes_offline}")
            assert pendentes_offline > 0, "Deveria haver registros pendentes no SQLite!"

            print("      -> Religando processo Coordenador da Nuvem e aguardando flush do SQLite...")
            p_nuvem_2 = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "agentes_ia.gateway.cli",
                    "--modo",
                    "nuvem",
                    "--host",
                    "127.0.0.1",
                    "--cloud-port",
                    str(porta_nuvem),
                ],
                cwd=raiz_prototipo,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            procs.append(p_nuvem_2)
            _aguardar_tcp("127.0.0.1", porta_nuvem)

            deadline = time.time() + 4.0
            while db.contar_pendentes() > 0 and time.time() < deadline:
                time.sleep(0.1)
            assert db.contar_pendentes() == 0, "Buffer SQLite deveria ter sido totalmente sincronizado!"
            status_final = _http_get_json(f"{cloud_url}/api/v1/status")
            print(
                f"      -> Flush concluído! Pendentes SQLite=0 | Registros recuperados na Nuvem={status_final['total_registros']}"
            )

            # 7. Contenção de Alucinações de LLM + Aprovação Humana + Auto-Monitoramento
            print(
                "\n[7/7] Validando Contenção de Alucinação LLM, Aprovação Humana (/api/v1/aprovacoes), Auto-cura Linux e /metrics..."
            )
            motor_pol = MotorPoliticas(carregar_politicas(), ativo=True)
            t_mem = TransporteMemoria()
            gw_pol = AgenteBordaGateway(
                "acme",
                "fabrica-01",
                "edge-sec",
                t_mem,
                motor_pol,
                diagnosticador=DiagnosticadorLLMSimulado(DiagnosticadorRegras(), 1.0, random.Random(7)),
                dry_run=True,
                cooldown_s=0.0,
                banco_auditoria=db,
            )
            gw_pol.iniciar()
            dev_pol = DispositivoCampoIoT("acme", "fabrica-01", "dev3", t_mem)
            dev_pol.iniciar()
            for _ in range(12):
                dev_pol.enviar_telemetria()
            dev_pol.injetar_falha("memory_overflow")
            for _ in range(3):
                dev_pol.enviar_telemetria()

            # Aprovação humana end-to-end via Cloud HTTP (/api/v1/aprovacoes)
            class _DiagnosticadorRestart:
                nome = "human_gate"

                def __init__(self) -> None:
                    self._base = DiagnosticadorRegras()

                def diagnosticar(self, no, anomalias, agente=None):
                    diag = self._base.diagnosticar(no, anomalias, agente)
                    if diag is not None:
                        diag.acao = "restart_device"
                    return diag

            t_hum = TransporteMemoria()
            gw_hum = AgenteBordaGateway(
                "acme",
                "fabrica-01",
                "edge-hum",
                t_hum,
                motor_pol,
                diagnosticador=_DiagnosticadorRestart(),
                dry_run=True,
                cooldown_s=1.0,
                banco_auditoria=db,
            )
            gw_hum.iniciar()
            dev_hum = DispositivoCampoIoT("acme", "fabrica-01", "dev4", t_hum)
            dev_hum.iniciar()
            boot_antes = dev_hum.boot_id
            for _ in range(12):
                dev_hum.enviar_telemetria()
            dev_hum.injetar_falha("sensor_failure")
            for _ in range(3):
                dev_hum.enviar_telemetria()
            assert dev_hum.boot_id == boot_antes, "restart_device não deveria executar antes da aprovação humana!"

            gw_hum.sincronizar_com_nuvem(cloud_url)
            pendentes_http = _http_get_json(f"{cloud_url}/api/v1/aprovacoes").get("pendentes", [])
            pend_dev4 = next((p for p in pendentes_http if p.get("alvo") == "dev4"), None)
            assert pend_dev4 is not None, "Aprovação pendente de restart_device deveria aparecer na Nuvem!"
            resp_aprov = _http_post_json(
                f"{cloud_url}/api/v1/aprovacoes",
                {"approval_id": pend_dev4["approval_id"], "aprovado": True},
            )
            assert resp_aprov.get("ok") is True, "Nuvem deveria aceitar confirmação do operador!"
            gw_hum.sincronizar_com_nuvem(cloud_url)
            assert dev_hum.boot_id != boot_antes, "dev4 deveria ter reiniciado após aprovação humana!"
            assert dev_hum.falha_ativa is None, "dev4 deveria estar saudável após reboot aprovado!"

            # Auto-monitoramento real do SO Linux (/proc -> DetectorZ -> AtuadorLinux em dry_run)
            gw_auto = AgenteBordaGateway(
                "acme",
                "fabrica-01",
                "edge-auto",
                TransporteMemoria(),
                motor_pol,
                diagnosticador=DiagnosticadorRegras(),
                dry_run=True,
                cooldown_s=0.0,
                banco_auditoria=db,
            )
            for _ in range(12):
                gw_auto.tick_auto_monitoramento()
                time.sleep(0.01)
            gw_auto.tick_auto_monitoramento({"cpu": 99.5})
            gw_auto.tick_auto_monitoramento({"cpu": 99.8})
            acoes_auto = [r.get("acao") for r in gw_auto.historico_acoes if r.get("executada")]
            assert "throttle_workload" in acoes_auto, (
                f"Auto-monitoramento deveria executar throttle_workload, obtido {gw_auto.historico_acoes}"
            )

            gw_pol.sincronizar_com_nuvem(cloud_url)
            gw_hum.sincronizar_com_nuvem(cloud_url)
            gw_auto.sincronizar_com_nuvem(cloud_url)
            status_pos_pol = _http_get_json(f"{cloud_url}/api/v1/status")
            assert status_pos_pol["aprovacoes_pendentes"] == 0, (
                f"Todas as aprovações deveriam ter sido liquidadas, obtido {status_pos_pol}"
            )
            assert status_pos_pol["acoes_negadas_politica"] >= 1
            with urllib.request.urlopen(f"{cloud_url}/metrics", timeout=2.0) as resp:
                metricas_prom = resp.read().decode("utf-8").strip()

            print(
                f"      -> Contenção de segurança: {status_pos_pol['acoes_negadas_politica']} ação(ões) insegura(s)/alucinada(s) bloqueada(s)!"
            )
            print(
                f"      -> Aprovação humana via HTTP OK: dev4 reiniciou (boot_id {boot_antes[:8]} -> {dev_hum.boot_id[:8]})"
            )
            print(
                f"      -> Auto-cura do Gateway Linux OK: {acoes_auto} | Total SQLite: {db.contar_total()}"
            )
            print("\n--- Saída real do endpoint /metrics (Prometheus) ---")
            print(metricas_prom)
            print("=" * 78)
            print(" RESULTADO E2E: TODOS OS 7 OBJETIVOS EXECUTADOS E VALIDADOS COM SUCESSO!")
            print("=" * 78)

        finally:
            if t_dev1 is not None:
                t_dev1.desconectar()
            for proc in reversed(procs):
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        proc.kill()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="CLI do Contínuo IoT-Borda-Nuvem Real")
    parser.add_argument(
        "--modo",
        choices=["broker", "nuvem", "gateway", "dispositivo", "demo", "e2e"],
        default="demo",
        help="Modo de execução",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host do broker MQTT / Nuvem")
    parser.add_argument("--port", type=int, default=1883, help="Porta do broker MQTT")
    parser.add_argument("--cloud-port", type=int, default=8080, help="Porta HTTP da Nuvem")
    parser.add_argument("--cloud-url", default=None, help="URL base do Coordenador Nuvem")
    parser.add_argument("--tenant", default="acme", help="Identificador do tenant")
    parser.add_argument("--site", default="fabrica-01", help="Identificador do site")
    parser.add_argument("--device-id", default="dev1", help="Identificador do dispositivo/gateway")
    parser.add_argument("--banco-auditoria", default="resultados/auditoria.db", help="Arquivo SQLite")
    parser.add_argument(
        "--diagnosticador",
        choices=["regras", "llm_simulado", "ollama"],
        default="regras",
        help="Motor de diagnóstico do Gateway",
    )
    parser.add_argument("--alucinacao", type=float, default=0.1, help="Taxa de alucinação simulada")
    parser.add_argument("--cooldown", type=float, default=10.0, help="Cooldown entre diagnósticos (s)")
    parser.add_argument("--intervalo", type=float, default=1.0, help="Intervalo de telemetria (s)")
    parser.add_argument("--intervalo-sync", type=float, default=5.0, help="Intervalo de sync com a Nuvem (s)")
    parser.add_argument("--usar-paho", action="store_true", help="Usa paho-mqtt em vez do TransporteTCP nativo")
    parser.add_argument(
        "--dry-run", action="store_true", help="Executa ações locais do SO em modo seguro dry-run"
    )

    args = parser.parse_args()

    if args.modo == "demo":
        executar_demo_malha_fechada()
        return

    if args.modo == "e2e":
        executar_e2e_real()
        return

    if args.modo == "broker":
        broker = BrokerMQTTLocal(host=args.host, port=args.port)
        porta = broker.iniciar()
        print(f"Broker MQTT 3.1.1 nativo rodando em {args.host}:{porta}. Ctrl+C para parar.")
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            broker.parar()
            print("Broker encerrado.")
        return

    if args.modo == "nuvem":
        nuvem = CoordenadorNuvemServidor(host=args.host, port=args.cloud_port)
        porta = nuvem.iniciar()
        print(f"Coordenador da Nuvem ativo em http://{args.host}:{porta}/api/v1/status")
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            nuvem.parar()
            print("Coordenador da Nuvem encerrado.")
        return

    # Modos clientes MQTT (gateway ou dispositivo)
    transporte = _criar_transporte(
        host=args.host,
        port=args.port,
        client_id=f"{args.modo}-{args.device_id}",
        usar_paho=args.usar_paho,
    )

    if args.modo == "gateway":
        politicas = carregar_politicas()
        motor = MotorPoliticas(politicas, ativo=True)
        diag = _criar_diagnosticador(args.diagnosticador, args.alucinacao)
        agente = AgenteBordaGateway(
            tenant_id=args.tenant,
            site_id=args.site,
            gateway_id="edge1",
            transporte=transporte,
            motor_politicas=motor,
            diagnosticador=diag,
            dry_run=args.dry_run,
            cooldown_s=args.cooldown,
            banco_auditoria=args.banco_auditoria,
        )
        agente.iniciar()
        print(f"Gateway de borda conectado em {args.host}:{args.port}. Ctrl+C para parar.")
        ultima_sync = 0.0
        try:
            while True:
                agente.tick_auto_monitoramento()
                agora = time.time()
                if args.cloud_url and (agora - ultima_sync >= args.intervalo_sync):
                    agente.sincronizar_com_nuvem(args.cloud_url)
                    ultima_sync = agora
                time.sleep(args.intervalo)
        except KeyboardInterrupt:
            agente.parar()
            print("Gateway finalizado.")

    elif args.modo == "dispositivo":
        disp = DispositivoCampoIoT(
            tenant_id=args.tenant,
            site_id=args.site,
            device_id=args.device_id,
            transporte=transporte,
        )
        disp.iniciar()
        print(f"Dispositivo {args.device_id} conectado em {args.host}:{args.port}. Ctrl+C para parar.")
        try:
            while True:
                disp.enviar_telemetria()
                time.sleep(args.intervalo)
        except KeyboardInterrupt:
            disp.parar()
            print("Dispositivo finalizado.")


if __name__ == "__main__":
    main()

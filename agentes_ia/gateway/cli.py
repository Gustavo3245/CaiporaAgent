"""Interface de linha de comando para executar o Gateway de Borda e Dispositivos.

Exemplos de uso:
  # Executa o Gateway de Borda monitorando o site e hardware local:
  python -m agentes_ia.gateway.cli --modo gateway --dry-run

  # Executa um dispositivo de campo (emula um ESP32 enviando telemetria a cada 1s):
  python -m agentes_ia.gateway.cli --modo dispositivo --device-id dev1

  # Executa uma demonstração em malha fechada no terminal com injeção de falha:
  python -m agentes_ia.gateway.cli --modo demo
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

from ..config import carregar_politicas
from ..politicas import MotorPoliticas
from .agente import AgenteBordaGateway
from .dispositivo_campo import DispositivoCampoIoT
from .transporte import TransporteMemoria, TransportePaho


def executar_demo_malha_fechada() -> None:
    """Executa uma demonstração interativa completa sem necessidade de broker externo."""
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
        time.sleep(0.05)

    print("\n[4] Injetando pane física no sensor ('sensor_failure' - sensor travado)...")
    dispositivo.injetar_falha("sensor_failure")

    for i in range(13, 17):
        msg = dispositivo.enviar_telemetria()
        print(f"    Tick {i:02d} | seq={msg.sequence} | sensor_std={msg.metricas[0].valor:.3f} (ANOMALIA)")
        time.sleep(0.05)

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


def main() -> None:
    parser = argparse.ArgumentParser(description="CLI do Gateway de Borda e Dispositivos IoT")
    parser.add_argument(
        "--modo",
        choices=["gateway", "dispositivo", "demo"],
        default="demo",
        help="Modo de execução: 'gateway', 'dispositivo' ou 'demo' fechada",
    )
    parser.add_argument("--host", default="localhost", help="Host do broker Mosquitto")
    parser.add_argument("--port", type=int, default=1883, help="Porta do broker Mosquitto")
    parser.add_argument("--tenant", default="acme", help="Identificador do tenant")
    parser.add_argument("--site", default="fabrica-01", help="Identificador do site")
    parser.add_argument("--device-id", default="dev1", help="Identificador do dispositivo")
    parser.add_argument(
        "--dry-run", action="store_true", help="Executa ações locais do SO em modo seguro dry-run"
    )

    args = parser.parse_args()

    if args.modo == "demo":
        executar_demo_malha_fechada()
        return

    # Modos para conexão real via Mosquitto
    try:
        transporte = TransportePaho(host=args.host, port=args.port, client_id=f"cli-{args.modo}-{args.device_id}")
        transporte.conectar()
    except Exception as e:
        print(f"Erro ao conectar ao broker MQTT ({args.host}:{args.port}): {e}")
        print("Dica: Para rodar sem broker externo, use '--modo demo'.")
        sys.exit(1)

    if args.modo == "gateway":
        politicas = carregar_politicas()
        motor = MotorPoliticas(politicas, ativo=True)
        agente = AgenteBordaGateway(
            tenant_id=args.tenant,
            site_id=args.site,
            gateway_id="edge1",
            transporte=transporte,
            motor_politicas=motor,
            dry_run=args.dry_run,
        )
        agente.iniciar()
        print(f"Gateway de borda iniciado em {args.host}:{args.port}. Pressione Ctrl+C para parar.")
        try:
            while True:
                agente.tick_auto_monitoramento()
                time.sleep(1.0)
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
        print(f"Dispositivo {args.device_id} conectado. Enviando telemetria a cada 1s...")
        try:
            while True:
                disp.enviar_telemetria()
                time.sleep(1.0)
        except KeyboardInterrupt:
            disp.parar()
            print("Dispositivo finalizado.")


if __name__ == "__main__":
    main()


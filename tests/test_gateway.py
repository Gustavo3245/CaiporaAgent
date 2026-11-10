"""Testes automatizados da camada de Gateway de Borda e Dispositivos de Campo."""

import unittest
from datetime import datetime, timezone

from agentes_ia.config import carregar_politicas
from agentes_ia.gateway import (
    AgenteBordaGateway,
    AuditoriaSQLite,
    DispositivoCampoIoT,
    ResultadoAtuacao,
    TransporteMemoria,
    coletar_metricas_sistema,
    executar_acao_local,
    topico_corresponde,
)
from agentes_ia.mqtt import (
    EnderecoMQTT,
    EstadoDispositivo,
    LeituraMetrica,
    MensagemTelemetria,
    TipoMensagem,
    decodificar_publicacao,
)
from agentes_ia.politicas import MotorPoliticas


class TestMetricasEAtuadores(unittest.TestCase):
    def test_coleta_metricas_sistema_retorna_valores_validos(self):
        m = coletar_metricas_sistema()
        self.assertIn("cpu", m)
        self.assertIn("mem", m)
        self.assertGreaterEqual(m["cpu"], 0.0)
        self.assertLessEqual(m["cpu"], 100.0)
        self.assertGreaterEqual(m["mem"], 0.0)
        self.assertLessEqual(m["mem"], 100.0)

    def test_atuador_dry_run_nao_altera_so(self):
        r = executar_acao_local("restart_service", "edge1", {"servico": "mosquitto"}, dry_run=True)
        self.assertTrue(r.sucesso)
        self.assertIn("DRY-RUN", r.mensagem)

    def test_atuador_notificar_operador(self):
        r = executar_acao_local("notificar_operador", "edge1", {"mensagem": "teste"})
        self.assertTrue(r.sucesso)


class TestTransporteEMQTT(unittest.TestCase):
    def test_topico_corresponde_curingas(self):
        self.assertTrue(topico_corresponde("iot/v1/+/+/+/telemetry", "iot/v1/a/b/c/telemetry"))
        self.assertTrue(topico_corresponde("iot/v1/a/#", "iot/v1/a/b/c/command"))
        self.assertFalse(topico_corresponde("iot/v1/+/+/+/telemetry", "iot/v1/a/b/c/command"))

    def test_transporte_memoria_filtra_assinatura(self):
        t = TransporteMemoria()
        t.conectar()
        recebidos = []
        t.assinar("iot/v1/+/+/+/telemetry")
        t.registrar_callback(lambda top, pay: recebidos.append(top))

        t.publicar("iot/v1/a/b/c/telemetry", b"ok")
        t.publicar("iot/v1/a/b/c/command", b"ignorar")

        self.assertEqual(len(recebidos), 1)
        self.assertEqual(recebidos[0], "iot/v1/a/b/c/telemetry")


class TestLoopFechadoGateway(unittest.TestCase):
    def setUp(self):
        self.transporte = TransporteMemoria()
        self.politicas = carregar_politicas()
        self.motor = MotorPoliticas(self.politicas, ativo=True)
        self.gateway = AgenteBordaGateway(
            tenant_id="acme",
            site_id="fabrica-01",
            gateway_id="edge1",
            transporte=self.transporte,
            motor_politicas=self.motor,
            dry_run=True,
            cooldown_s=0.5,
        )
        self.gateway.iniciar()

        self.dispositivo = DispositivoCampoIoT(
            tenant_id="acme",
            site_id="fabrica-01",
            device_id="dev1",
            transporte=self.transporte,
        )
        self.dispositivo.iniciar()

    def test_autocura_falha_sensor_via_mqtt(self):
        # 1. Aquecimento normal
        for _ in range(12):
            self.dispositivo.enviar_telemetria()

        # 2. Injeta falha
        self.dispositivo.injetar_falha("sensor_failure")
        for _ in range(3):
            self.dispositivo.enviar_telemetria()

        # 3. Dispositivo deve ter sido curado pelo comando reset_sensor do Gateway
        self.assertIsNone(self.dispositivo.falha_ativa)
        self.assertEqual(len(self.dispositivo.comandos_recebidos), 1)
        self.assertEqual(self.dispositivo.comandos_recebidos[0].acao, "reset_sensor")

        # 4. Gateway deve ter registrado a auditoria do comando e da resposta
        acoes = [a for a in self.gateway.historico_acoes if a.get("acao") == "reset_sensor"]
        self.assertTrue(len(acoes) > 0)
        self.assertTrue(acoes[0]["executada"])


class TestAuditoriaSQLite(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = f"{self.tmp_dir.name}/teste_auditoria.db"
        self.db = AuditoriaSQLite(self.db_path)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_registro_e_consulta_pendentes(self):
        reg = {
            "alvo": "dev1",
            "acao": "reset_sensor",
            "falha": "sensor_failure",
            "decisao": "permitir",
            "executada": True,
        }
        id_inserido = self.db.registrar(reg)
        self.assertGreater(id_inserido, 0)
        self.assertEqual(self.db.contar_total(), 1)
        self.assertEqual(self.db.contar_pendentes(), 1)

        pendentes = self.db.obter_pendentes_sincronizacao()
        self.assertEqual(len(pendentes), 1)
        self.assertEqual(pendentes[0]["alvo"], "dev1")
        self.assertEqual(pendentes[0]["sincronizado"], 0)

        # Marca como sincronizado (simula envio para nuvem)
        self.db.marcar_como_sincronizado([id_inserido])
        self.assertEqual(self.db.contar_pendentes(), 0)
        self.assertEqual(len(self.db.obter_pendentes_sincronizacao()), 0)


if __name__ == "__main__":
    unittest.main()


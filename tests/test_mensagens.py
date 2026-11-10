import json
import sys
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agentes_ia.mqtt import (
    EnderecoMQTT,
    ErroMensagem,
    EstadoDispositivo,
    LeituraMetrica,
    MensagemComando,
    MensagemEstado,
    MensagemEvento,
    MensagemLog,
    MensagemResultadoComando,
    MensagemTelemetria,
    NivelLog,
    QualidadeMetrica,
    StatusComando,
    TipoMensagem,
    decodificar_mensagem,
    decodificar_publicacao,
)


AGORA = datetime(2026, 11, 4, 17, 30, tzinfo=timezone.utc)
COMUNS = {
    "device_id": "sensor-01",
    "sequence": 42,
    "boot_id": "boot-a1",
    "timestamp": AGORA,
}


class TestTopicos(unittest.TestCase):
    def test_constroi_topico_e_assinatura(self):
        endereco = EnderecoMQTT("acme", "fabrica-01", "sensor-01")
        self.assertEqual(
            endereco.topico(TipoMensagem.TELEMETRIA),
            "iot/v1/acme/fabrica-01/sensor-01/telemetry",
        )
        self.assertEqual(
            EnderecoMQTT.assinatura_site("acme", "fabrica-01", TipoMensagem.LOG),
            "iot/v1/acme/fabrica-01/+/log",
        )

    def test_rejeita_wildcard_em_identificador(self):
        with self.assertRaises(ErroMensagem):
            EnderecoMQTT("acme", "fabrica-01", "sensor-+")

    def test_decodifica_topico(self):
        endereco, tipo = EnderecoMQTT.de_topico("iot/v1/acme/fabrica-01/sensor-01/state")
        self.assertEqual(endereco, EnderecoMQTT("acme", "fabrica-01", "sensor-01"))
        self.assertEqual(tipo, TipoMensagem.ESTADO)


class TestMensagens(unittest.TestCase):
    def mensagens(self):
        comando_id = str(uuid.uuid4())
        return [
            MensagemTelemetria(
                **COMUNS,
                metricas=(
                    LeituraMetrica(
                        nome="temperature", valor=37.2, unidade="Cel",
                        qualidade=QualidadeMetrica.BOA, tags={"sensor": "internal"},
                    ),
                    LeituraMetrica(nome="cpu", valor=81.4, unidade="percent"),
                ),
                intervalo_amostragem_ms=1000,
            ),
            MensagemLog(
                **COMUNS, nivel=NivelLog.ERRO, componente="temperature_sensor",
                mensagem="sensor did not answer", codigo="read_timeout", contexto={"timeout_ms": 500},
            ),
            MensagemEvento(
                **COMUNS, evento="device_restarted", severidade=NivelLog.AVISO,
                estado="watchdog", atributos={"restart_count": 2},
            ),
            MensagemEstado(
                **COMUNS, status=EstadoDispositivo.ONLINE, uptime_s=3600,
                firmware_version="1.2.0", capacidades=("temperature", "restart_sensor"),
            ),
            MensagemComando(
                **COMUNS, message_id=comando_id, acao="restart_sensor",
                parametros={"sensor": "internal"}, expira_em=AGORA + timedelta(minutes=1),
                solicitado_por="edge-agent", politica_id="safe-restart-v1",
            ),
            MensagemResultadoComando(
                **COMUNS, command_id=comando_id, correlation_id=comando_id,
                status=StatusComando.SUCESSO, iniciado_em=AGORA,
                concluido_em=AGORA + timedelta(seconds=2), resultado={"restarted": True},
            ),
        ]

    def test_round_trip_de_todos_os_tipos(self):
        for original in self.mensagens():
            with self.subTest(tipo=original.tipo.value):
                recuperada = decodificar_mensagem(original.para_bytes())
                self.assertEqual(recuperada, original)
                self.assertEqual(recuperada.para_dict(), original.para_dict())

    def test_json_tem_envelope_versionado(self):
        mensagem = self.mensagens()[0]
        dados = json.loads(mensagem.para_json())
        self.assertEqual(dados["schema_version"], "1.0")
        self.assertEqual(dados["message_type"], "telemetry")
        self.assertEqual(dados["device_id"], "sensor-01")
        self.assertEqual(dados["timestamp"], "2026-11-04T17:30:00.000Z")

    def test_rejeita_valor_nao_finito(self):
        with self.assertRaises(ErroMensagem):
            LeituraMetrica(nome="temperature", valor=float("nan"), unidade="Cel")

    def test_rejeita_timestamp_sem_fuso(self):
        with self.assertRaises(ErroMensagem):
            MensagemEstado(
                device_id="sensor-01", sequence=1, boot_id="boot-a1",
                timestamp=datetime(2026, 11, 4, 10, 0), status=EstadoDispositivo.ONLINE,
            )

    def test_rejeita_campos_desconhecidos(self):
        dados = self.mensagens()[0].para_dict()
        dados["payload"]["unexpected"] = True
        with self.assertRaisesRegex(ErroMensagem, "campos desconhecidos"):
            decodificar_mensagem(json.dumps(dados))

    def test_rejeita_schema_incompativel(self):
        dados = self.mensagens()[0].para_dict()
        dados["schema_version"] = "2.0"
        with self.assertRaisesRegex(ErroMensagem, "não suportada"):
            decodificar_mensagem(json.dumps(dados))

    def test_comando_expirado(self):
        comando = self.mensagens()[4]
        self.assertFalse(comando.expirado(AGORA))
        self.assertTrue(comando.expirado(AGORA + timedelta(minutes=2)))

    def test_valida_topico_e_payload_conjuntamente(self):
        mensagem = self.mensagens()[0]
        endereco, recuperada = decodificar_publicacao(
            "iot/v1/acme/fabrica-01/sensor-01/telemetry", mensagem.para_bytes()
        )
        self.assertEqual(endereco.device_id, recuperada.device_id)
        with self.assertRaisesRegex(ErroMensagem, "device_id"):
            decodificar_publicacao(
                "iot/v1/acme/fabrica-01/outro-device/telemetry", mensagem.para_bytes()
            )
        with self.assertRaisesRegex(ErroMensagem, "message_type"):
            decodificar_publicacao(
                "iot/v1/acme/fabrica-01/sensor-01/log", mensagem.para_bytes()
            )

    def test_rejeita_chaves_json_duplicadas(self):
        mensagem = self.mensagens()[0].para_json()
        adulterada = mensagem.replace('"sequence":42', '"sequence":41,"sequence":42')
        with self.assertRaisesRegex(ErroMensagem, "duplicado"):
            decodificar_mensagem(adulterada)

    def test_limita_tamanho_da_mensagem(self):
        with self.assertRaisesRegex(ErroMensagem, "excede"):
            decodificar_mensagem(b"{}", tamanho_max=1)


if __name__ == "__main__":
    unittest.main()

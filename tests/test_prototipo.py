import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agentes_ia.config import carregar, carregar_politicas
from agentes_ia.deteccao import DetectorZ
from agentes_ia.diagnostico import Diagnostico, DiagnosticadorRegras
from agentes_ia.politicas import MotorPoliticas
from agentes_ia.simulacao import Simulacao


def diag(**kw):
    base = dict(no="dev1", falha="cpu_saturation", alvo="dev1", categoria="dispositivo",
                evidencias=[{"no": "dev1", "metrica": "cpu", "valor": 95.0, "z": 12.0}],
                confianca=0.95, acao="throttle_workload")
    base.update(kw)
    return Diagnostico(**base)


OBS = {("dev1", "cpu"): (95.0, 1000)}


class TestDetector(unittest.TestCase):
    def test_detecta_salto(self):
        d = DetectorZ()
        for _ in range(20):
            self.assertEqual(d.atualizar({"cpu": 35.0}), [])
        d.atualizar({"cpu": 95.0})
        self.assertTrue(d.atualizar({"cpu": 95.0}))  # confirma na 2ª amostra


class TestDiagnostico(unittest.TestCase):
    def test_regras_classificam(self):
        d = DiagnosticadorRegras().diagnosticar("dev1", [{"metrica": "pkt_loss", "valor": 40.0, "z": 20.0}])
        self.assertEqual((d.falha, d.categoria, d.acao), ("packet_loss", "comunicacao", "reroute_link"))


class TestPoliticas(unittest.TestCase):
    def setUp(self):
        self.m = MotorPoliticas(carregar_politicas())

    def test_permite_acao_conforme(self):
        self.assertEqual(self.m.avaliar("edge_agent", diag(), OBS, 1500).efeito, "permitir")

    def test_nega_acao_proibida(self):
        self.assertEqual(self.m.avaliar("edge_agent", diag(acao="delete_data"), OBS, 1500).regra, "acao_proibida")

    def test_nega_fora_de_escopo(self):
        self.assertEqual(self.m.avaliar("edge_agent", diag(alvo="cloud1", acao="restart_service"), OBS, 1500).regra, "escopo")

    def test_menor_privilegio_iot(self):
        self.assertEqual(self.m.avaliar("iot_device", diag(), OBS, 1500).regra, "privilegio")

    def test_nega_evidencia_inventada(self):
        d = diag(evidencias=[{"no": "dev1", "metrica": "sensor_std", "valor": 0.0, "z": 9.0}], falha="sensor_failure", acao="reset_sensor")
        self.assertEqual(self.m.avaliar("edge_agent", d, OBS, 1500).regra, "integridade")

    def test_restart_device_exige_humano(self):
        self.assertEqual(self.m.avaliar("edge_agent", diag(acao="restart_device"), OBS, 1500).efeito, "exigir_humano")

    def test_limite_operacional(self):
        for t in (1500, 1600):
            self.m.avaliar("edge_agent", diag(), OBS, t)
        self.assertEqual(self.m.avaliar("edge_agent", diag(), OBS, 1700).regra, "limite_operacional")

    def test_desativado_permite_tudo(self):
        m = MotorPoliticas(carregar_politicas(), ativo=False)
        self.assertEqual(m.avaliar("edge_agent", diag(acao="delete_data"), OBS, 1500).efeito, "permitir")


class TestSimulacao(unittest.TestCase):
    def test_reparo_sem_alucinacao(self):
        r = Simulacao(carregar(over={"agentes": {"taxa_alucinacao": 0.0}, "simulacao": {"n_falhas": 8}})).executar()
        self.assertEqual(r["taxa_reparo"], 1.0)
        self.assertEqual(r["inseguras_executadas"], 0)

    def test_politicas_contem_acoes_inseguras(self):
        over = {"agentes": {"taxa_alucinacao": 0.5}, "simulacao": {"n_falhas": 12}}
        on = Simulacao(carregar(over=over)).executar()
        off = Simulacao(carregar(over={**over, "politicas_ativas": False})).executar()
        self.assertEqual(on["inseguras_executadas"], 0)
        self.assertGreater(off["inseguras_executadas"], 0)

    def test_deterministico(self):
        a = Simulacao(carregar()).executar(); b = Simulacao(carregar()).executar()
        self.assertAlmostEqual(a["mttr_s"], b["mttr_s"], places=2)  # tempo real da política entra no relógio virtual
        self.assertEqual(a["diag_total"], b["diag_total"])


if __name__ == "__main__":
    unittest.main()

# Protótipo — Agentes de IA para Diagnóstico de Falhas no Contínuo IoT–Borda–Nuvem com Restrições de Segurança

Etapa 6 (*Desenvolvimento da Solução*) do artigo da disciplina Tópicos em Sistemas Distribuídos (Projeto 8).
Implementa os agentes **IoT Device**, **Edge Node** e **Cloud Coordinator**, o módulo de **policy enforcement**
(políticas declarativas em JSON), a **injeção controlada de falhas** e os experimentos das questões de pesquisa RQ1–RQ4.

## Como executar (Python ≥ 3.10, só biblioteca padrão)

```bash
python -m agentes_ia.demo                       # demonstração com log legível (diagnóstico na borda)
python -m agentes_ia.demo --modo nuvem          # baseline centralizado (nuvem)
python -m agentes_ia.demo --sem-politicas --alucinacao 0.5   # sem policy enforcement, LLM alucinando
python -m agentes_ia.run_experiments            # RQ1–RQ4 -> resultados/resultados.csv e resumo.md
python -m unittest discover -s tests -v         # testes automatizados
```

## Arquitetura

| Componente | Arquivo | Responsabilidade |
|---|---|---|
| Agente IoT Device | `simulacao.py` (`AgenteIoT`) | Detecção local (z-score) e envio de eventos à borda. Privilégio mínimo: só `notificar_operador`. |
| Agente Edge Node | `simulacao.py` (`AgenteBorda`) | Agrega eventos, diagnostica a causa-raiz, submete a ação ao policy enforcement e executa. Funciona sem a nuvem. |
| Cloud Coordinator | `simulacao.py` (`CoordenadorNuvem`) | Recebe relatórios, diagnostica falhas da própria nuvem e, no modo `nuvem`, faz o papel de baseline centralizado. |
| Detecção de anomalias | `deteccao.py` | Z-score com janela móvel, custo O(1) por amostra, confirmação em 2 amostras consecutivas. |
| Diagnóstico | `diagnostico.py` | Regras causais (baseline), **LLM simulado** com alucinações (para testar a contenção) e adaptador **Ollama** (LLM local real). Saída estruturada: causa, categoria, evidências, confiança, ação. |
| Policy enforcement | `politicas.py` + `politicas.json` | Ordem: ação proibida → privilégio → escopo → integridade (evidência verificada contra a telemetria) → limite operacional → confirmação humana. Log de auditoria. |
| Testbed | `telemetria.py`, `simulacao.py` | Telemetria sintética, falhas injetadas (CPU, memória, perda de pacotes, sensor, serviço) e enlaces com latência/perda/quedas. |
| Observabilidade | `exportador.py` | Endpoint `/metrics` em formato Prometheus (`demo --servir`). |

Fluxo: `detecção → diagnóstico estruturado → policy enforcement → (humano, se condicional) → execução → telemetria volta ao normal`.

## Contrato MQTT para dispositivos reais

O primeiro componente da evolução para dispositivos reais está no pacote `agentes_ia/mqtt`.
Ele define envelopes versionados, validação, serialização JSON e tópicos para telemetria,
logs, eventos, estado, comandos e resultados de comandos, sem acoplar o domínio a um
cliente MQTT específico. O contrato completo e exemplos estão em
[`docs/protocolo_mqtt.md`](docs/protocolo_mqtt.md).

## Configurações e parâmetros (registro para reprodutibilidade)

Todos em `config.json` (override por cenário em `run_experiments.py`). Sementes fixas: resultados reprodutíveis.

| Parâmetro | Valor padrão | Observação |
|---|---|---|
| Dispositivos IoT / nó de borda / nuvem | 4 / 1 / 1 | `simulacao.n_dispositivos` |
| Falhas por execução / intervalo / duração máx. | 20 / 120 s / 90 s | falhas não reparadas expiram |
| Tick de telemetria | 1 s (tempo virtual, resolução de 1 ms) | equivale a `scrape_interval` |
| Detecção | janela 30, aquecimento 10, \|z\| > 4, 2 confirmações | `deteccao` |
| Cooldown entre diagnósticos do mesmo nó | 10 s | `agentes.cooldown_s` |
| Latência do LLM (borda / nuvem) | 900 ms / 400 ms | **suposição** (modelo pequeno local vs. modelo maior na nuvem) |
| Taxa de alucinação do LLM simulado | 10% (0–50% nos experimentos) | tipos: evidência inventada, ação proibida, fora de escopo, causa errada, ação invasiva |
| Tempo de execução da ação | 500 ms | `agentes.tempo_execucao_acao_ms` |
| Aprovação humana | atraso 30 s, aprovação 100% | só para `restart_device` |
| Enlace IoT→borda | 5 ms, perda 0%, 2 retentativas / 500 ms | |
| Enlace borda↔nuvem | 80 ms, perda 1%, 3 retentativas / 1,5 s | RQ1 varia a latência; RQ3 varia perda e quedas |
| Políticas | `politicas.json` | máx. 2 ações por alvo/60 s; confiança mínima 0,6; validade da evidência 30 s |
| Sementes dos experimentos | 1–10 | 20 falhas por semente |

## Limitações (importante para a seção de ameaças à validade)

- **Rede simulada, não containers reais.** O artigo prevê Docker + MQTT/gRPC + `tc netem`. Aqui a comunicação é simulada em processo
  único com tempo virtual (enlaces com latência, perda, retentativas e quedas). Isso torna os experimentos rápidos e reprodutíveis,
  mas **não mede** latência de rede real. A arquitetura permite trocar `Enlace` por MQTT/HTTP.
- **LLM simulado por padrão.** O `DiagnosticadorLLMSimulado` imita erros de LLM; o adaptador `DiagnosticadorOllama`
  (`agentes.diagnosticador = "ollama"`) existe, mas **não foi testado** neste ambiente (sem Ollama).
- **Políticas em JSON**, não em Rego/OPA. O artigo cita linguagem declarativa; a troca por OPA exigiria apenas reescrever `politicas.py`.
- **Dano não modelado.** Uma ação insegura executada (política desligada) é *contada*, mas não causa efeitos colaterais na simulação.
- **Custos de CPU/memória** (RQ4) são do processo Python do simulador, não de um nó de borda real (ex.: Raspberry Pi). Para RQ4, meça em hardware real.
- **Os resultados dependem dos parâmetros acima**, em especial as latências de LLM (suposição). Não há teste de significância estatística.
- `docker-compose.yml` e `prometheus.yml` **não foram executados** no ambiente de desenvolvimento.

## Docker / Prometheus (opcional, não testado)

```bash
docker compose up --build     # simulação em tempo real + Prometheus em http://localhost:9090
```

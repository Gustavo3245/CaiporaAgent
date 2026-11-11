# Resultados dos experimentos (10 sementes × 20 falhas por cenário)

Valores são médias entre as sementes. **Resultados de simulação** dependem dos parâmetros em `config.json`.

## RQ1 – Eficácia: borda vs nuvem (falhas em IoT/borda, sem alucinação)

| Cenário | MTTR (s) | Latência diag. (s) | Taxa de reparo |
|---|---|---|---|
| borda – latência 20 ms | 2.85 | 2.35 | 1.00 |
| nuvem – latência 20 ms | 2.59 | 2.03 | 1.00 |
| borda – latência 80 ms | 2.85 | 2.35 | 1.00 |
| nuvem – latência 80 ms | 2.76 | 2.04 | 1.00 |
| borda – latência 200 ms | 2.85 | 2.35 | 1.00 |
| nuvem – latência 200 ms | 3.02 | 2.17 | 1.00 |
| borda – latência 500 ms | 2.85 | 2.35 | 1.00 |
| nuvem – latência 500 ms | 3.64 | 2.56 | 1.00 |
| borda – latência 1000 ms | 2.85 | 2.35 | 1.00 |
| nuvem – latência 1000 ms | 4.78 | 3.05 | 1.00 |
| borda – latência 2000 ms | 2.85 | 2.35 | 1.00 |
| nuvem – latência 2000 ms | 6.92 | 4.19 | 1.00 |

## RQ2 – Sobrecarga de segurança (modo borda)

| Cenário | MTTR (s) | Precisão diag. | Taxa aprovação | Ações inseguras propostas | Contidas | Executadas | Custo política (µs) |
|---|---|---|---|---|---|---|---|
| políticas OFF | 6.73 | 0.86 | 1.00 | 2.80 | 0.00 | 2.80 | 1.16 |
| políticas ON | 7.23 | 0.85 | 0.79 | 2.90 | 2.90 | 0.00 | 29.56 |
| políticas ON, sem alucinação (custo puro) | 2.68 | 0.99 | 1.00 | 0.00 | 0.00 | 0.00 | 18.12 |

## RQ3 – Resiliência a degradação do enlace borda–nuvem

| Cenário | Taxa de reparo | MTTR (s) | Comandos perdidos | Entrega de msgs |
|---|---|---|---|---|
| borda – perda 0% | 1.00 | 2.85 | 0.00 | 1.00 |
| nuvem – perda 0% | 1.00 | 2.73 | 0.10 | 1.00 |
| borda – perda 20% | 1.00 | 2.85 | 0.00 | 0.99 |
| nuvem – perda 20% | 0.99 | 3.80 | 0.40 | 0.97 |
| borda – perda 50% | 1.00 | 2.85 | 0.00 | 0.92 |
| nuvem – perda 50% | 0.97 | 8.97 | 2.70 | 0.84 |
| borda – perda 80% | 1.00 | 2.85 | 0.00 | 0.55 |
| nuvem – perda 80% | 0.84 | 24.97 | 10.50 | 0.56 |
| borda – quedas intermitentes | 1.00 | 2.85 | 0.00 | 0.81 |
| nuvem – quedas intermitentes | 1.00 | 5.32 | 0.10 | 0.91 |

## RQ4 – Custo computacional e de comunicação

| Cenário | Msgs borda–nuvem | Bytes borda–nuvem | CPU detecção (ms) | CPU diagnóstico (ms) | CPU políticas (ms) | Pico memória (KB) |
|---|---|---|---|---|---|---|
| borda | 20.30 | 1343.30 | 433.08 | 0.89 | 0.83 | 96.98 |
| nuvem | 39.70 | 7966.80 | 430.10 | 0.83 | 0.81 | 97.85 |

Obs.: 'CPU detecção' soma o custo de todos os detectores simulados no mesmo processo; não representa o consumo de um nó de borda real (ver limitações no README).

# Guia de Implantação em Dispositivos Físicos Reais (IoT–Borda–Nuvem)

Este guia orienta o processo de montagem de bancada e implantação dos agentes de IA em dispositivos reais utilizando o pacote [`agentes_ia.gateway`](../agentes_ia/gateway/).

---

## 1. Arquitetura da Bancada Física

```
┌───────────────────────────────────────────────┐
│              GATEWAY DE BORDA                 │
│   (Raspberry Pi 4/5 ou PC Linux na rede)      │
│                                               │
│  [Broker Eclipse Mosquitto (Porta 1883)]      │
│                     ▲                         │
│                     │ MQTT (Local / Wi-Fi)    │
│                     ▼                         │
│  [AgenteBordaGateway Daemon (Python)]         │
│   ├── Leitor Nativo de Métricas (/proc, /sys) │
│   ├── DetectorZ (Anomalias O(1))              │
│   ├── Diagnosticador (Regras / Ollama Local)  │
│   ├── Motor de Políticas (politicas.py)       │
│   ├── Atuador Linux (systemctl, drop_caches)  │
│   └── Banco SQLite Local (auditoria.db)       │
└─────────────────────▲─────────────────────────┘
                      │ Wi-Fi Doméstico / Industrial
        ┌─────────────┴─────────────┐
        │                           │
┌───────▼──────────────┐   ┌────────▼──────────────┐
│  DISPOSITIVO ESP32   │   │  DISPOSITIVO LINUX    │
│ (firmware_esp32.ino) │   │ (Python / cli.py)     │
│  - Sensor no GPIO 34 │   │  - Leituras de SO     │
│  - Reset via GPIO 4  │   │  - Emulação de campo  │
└──────────────────────┘   └───────────────────────┘
```

---

## 2. Configuração do Gateway de Borda (Raspberry Pi ou PC)

### A. Subir o Broker MQTT (Mosquitto)
Você pode rodar o broker nativamente ou via Docker:

* **Opção 1: Via Docker Compose (Recomendado)**:
  ```bash
  docker compose up -d mosquitto
  ```
* **Opção 2: Nativo no Linux**:
  ```bash
  sudo apt update && sudo apt install -y mosquitto mosquitto-clients
  sudo cp mosquitto.conf /etc/mosquitto/conf.d/edge.conf
  sudo systemctl restart mosquitto
  ```

### B. Executar o Agente de Borda
* **Execução em Primeiro Plano (para testes e depuração)**:
  ```bash
  PYTHONPATH=. python3 -m agentes_ia.gateway.cli --modo gateway --tenant acme --site fabrica-01 --dry-run
  ```
* **Instalação como Serviço Automático de Sistema (Produção)**:
  ```bash
  sudo bash deploy/install_service.sh
  ```
  Para acompanhar os logs em tempo real:
  ```bash
  sudo journalctl -u edge-gateway -f
  ```

---

## 3. Configuração do Dispositivo de Campo (ESP32)

1. Conecte a placa ESP32 ao computador via USB.
2. Abra a pasta [`firmware_esp32/firmware_esp32.ino`](../firmware_esp32/firmware_esp32.ino) no Arduino IDE ou PlatformIO.
3. Configure o SSID e senha do seu Wi-Fi:
   ```cpp
   const char* WIFI_SSID     = "SUA_REDE_WIFI";
   const char* WIFI_PASSWORD = "SUA_SENHA_WIFI";
   const char* MQTT_BROKER   = "192.168.1.100"; // IP do Raspberry Pi / Gateway na rede
   ```
4. Grave o firmware na placa e abra o Serial Monitor a `115200` baud.

---

## 4. Testes Práticos de Autocura e Resiliência na Bancada

### Teste 1: Autocura de Sensor Travado (Falha de Hardware)
1. O ESP32 envia amostras normais de telemetria periodicamente.
2. Simule uma falha desconectando o pino de sinal do sensor ou forçando o valor para um nível constante.
3. O `DetectorZ` no Gateway detecta variância zero contínua.
4. O `Diagnosticador` identifica `sensor_failure` e propõe `reset_sensor`.
5. O `MotorPoliticas` valida a integridade com base na telemetria recente e aprova.
6. O Gateway despacha o comando via MQTT para `iot/v1/acme/fabrica-01/esp32-sensor-01/command`.
7. O ESP32 recebe a instrução, pulsa o pino GPIO 4 cortando e restaurando a energia do sensor, e responde com `command-result` (`succeeded`).

### Teste 2: Desconexão Abrupta e Last Will and Testament (LWT)
1. Desconecte o cabo de alimentação do ESP32 da tomada.
2. Como o dispositivo não enviou `DISCONNECT` gracioso, o broker Mosquitto identifica a queda do keep-alive TCP e publica automaticamente o LWT:
   * Tópico: `iot/v1/acme/fabrica-01/esp32-sensor-01/state`
   * Payload: `{"status": "offline"}`
3. O Agente de Borda recebe a notificação e registra imediatamente o estado `degradado` do site na auditoria.

### Teste 3: Auto-Recuperação do Gateway Linux
1. Em um terminal do gateway, gere uma sobrecarga de memória ou simule falha de serviço:
   ```bash
   sudo systemctl stop mosquitto
   ```
2. O ciclo `tick_auto_monitoramento()` identifica a degradação local.
3. O Atuador Linux (`atuadores.py`) executa a remediação aprovada (`systemctl restart mosquitto`) restabelecendo o nó.

---

## 5. Auditoria Local e Operação Offline (SQLite)

Todas as ações decididas pelo agente são gravadas no banco SQLite local em `resultados/auditoria.db`.

Para inspecionar as decisões tomadas:
```bash
sqlite3 resultados/auditoria.db "SELECT timestamp, alvo, falha, acao, decisao, sincronizado FROM auditoria ORDER BY id DESC LIMIT 10;"
```

Quando a conexão com a nuvem é restabelecida, os registros com `sincronizado = 0` podem ser transmitidos em lote para o Coordenador Central através do método `obter_pendentes_sincronizacao()`.


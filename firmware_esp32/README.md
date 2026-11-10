# Firmware ESP32 — Dispositivo de Campo IoT

Implementação em C++ (Arduino IDE / PlatformIO) do contrato de dispositivo IoT versão 1.0 definido em [`docs/protocolo_mqtt.md`](../docs/protocolo_mqtt.md).

## Requisitos de Hardware

- Placa **ESP32 DevKit v1** (ou qualquer módulo ESP32-WROOM / ESP32-WROVER)
- Sensor analógico ou I2C/SPI (ex.: DHT11/DHT22, BMP280, ou potenciômetro no pino ADC GPIO 34)
- Transistor MOSFET ou relé no pino GPIO 4 (opcional, para corte físico de alimentação no comando `reset_sensor`)
- Cabo micro-USB / USB-C para gravação

## Dependências de Software (Bibliotecas)

Instale através do Library Manager do Arduino IDE ou no `platformio.ini`:

1. **PubSubClient** (por Nick O'Leary) — versão ≥ 2.8
2. **ArduinoJson** (por Benoît Blanchon) — versão 6 ou 7

> [!IMPORTANT]
> No arquivo `PubSubClient.h`, o tamanho padrão do buffer de pacote é de 256 bytes (`MQTT_MAX_PACKET_SIZE 256`).
> Como o envelope JSON do contrato IoT pode chegar a ~500 bytes com métricas e metadados, defina no seu código ou no cabeçalho:
> ```cpp
> client.setBufferSize(1024);
> ```

## Configuração

Abra [`firmware_esp32.ino`](firmware_esp32.ino) e configure:

```cpp
const char* WIFI_SSID     = "SUA_REDE_WIFI";
const char* WIFI_PASSWORD = "SUA_SENHA_WIFI";
const char* MQTT_BROKER   = "IP_DO_SEU_GATEWAY_OU_PC"; // ex: 192.168.1.100
const int   MQTT_PORT     = 1883;

const char* TENANT_ID     = "acme";
const char* SITE_ID       = "fabrica-01";
const char* DEVICE_ID     = "esp32-sensor-01";
```

## Como Gravar

1. Abra a pasta `firmware_esp32` no Arduino IDE.
2. Selecione a placa: **DOIT ESP32 DEVKIT V1** (ou modelo correspondente).
3. Selecione a porta serial correspondente (`/dev/ttyUSB0` no Linux ou `COMx` no Windows).
4. Clique em **Upload**.
5. Abra o Serial Monitor a 115200 baud para visualizar o status da conexão Wi-Fi, MQTT e telemetrias enviadas.

## Comportamento do Firmware

1. **Last Will and Testament (LWT):** Ao conectar, registra LWT no tópico `iot/v1/acme/fabrica-01/esp32-sensor-01/state` com `status=offline` e `retain=true`. Se o ESP32 for desligado abruptamente da tomada, o Broker Mosquitto automaticamente avisa o Gateway.
2. **Online State:** Publica `status=online` assim que o handshake MQTT é concluído.
3. **Telemetria:** Publica métricas periodicamente em `.../telemetry` com UUID e contador `sequence`.
4. **Comandos:** Assina `.../command`. Ao receber `reset_sensor`, pulsa o GPIO 4 e publica a confirmação com `command_id` correspondente em `.../command-result`.


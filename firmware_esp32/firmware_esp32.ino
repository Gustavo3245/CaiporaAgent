/*
 * Firmware ESP32 para Dispositivo de Campo IoT
 *
 * Implementa estritamente o contrato MQTT v1.0 (docs/protocolo_mqtt.md):
 *  - Sincronização NTP para timestamps ISO 8601 UTC (com fallback relativo ao boot)
 *  - Configuração de Last Will and Testament (LWT) válido no tópico 'state'
 *  - Envio periódico de telemetria com message_id (UUIDv4), timestamp, sequence e boot_id
 *  - Recepção de comandos (ex: reset_sensor, restart_device)
 *  - Publicação de confirmação completa em 'command-result'
 *
 * Bibliotecas necessárias (no Arduino IDE ou PlatformIO):
 *  - PubSubClient (por Nick O'Leary)
 *  - ArduinoJson (por Benoit Blanchon - versão 6 ou 7)
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <time.h>

// ============================================================================
// CONFIGURAÇÕES DE REDE E MQTT
// ============================================================================
const char* WIFI_SSID     = "SUA_REDE_WIFI";
const char* WIFI_PASSWORD = "SUA_SENHA_WIFI";

// IP do Gateway de Borda (Raspberry Pi ou PC na mesma rede local)
const char* MQTT_BROKER   = "192.168.1.100";
const int   MQTT_PORT     = 1883;

// Identificadores de Endereçamento do Contrato MQTT v1.0
const char* TENANT_ID     = "acme";
const char* SITE_ID       = "fabrica-01";
const char* DEVICE_ID     = "esp32-sensor-01";

// Pino GPIO para reset de energia do sensor (se aplicável)
const int PINO_ALIMENTACAO_SENSOR = 4;

// ============================================================================
// VARIÁVEIS DE ESTADO DO DISPOSITIVO
// ============================================================================
WiFiClient espClient;
PubSubClient client(espClient);

char topicoTelemetria[128];
char topicoEstado[128];
char topicoComando[128];
char topicoResultadoComando[128];

char bootId[16];
uint32_t sequence = 0;
unsigned long ultimoEnvioTelemetria = 0;
const unsigned long INTERVALO_TELEMETRIA_MS = 1000;

// ============================================================================
// GERAÇÃO DE UUID V4 E TIMESTAMP ISO 8601 UTC PARA O ENVELOPE
// ============================================================================
void gerarUUID(char* buffer) {
  uint32_t r1 = esp_random();
  uint32_t r2 = esp_random();
  uint32_t r3 = esp_random();
  uint32_t r4 = esp_random();
  sprintf(buffer, "%08x-%04x-4%03x-%04x-%04x%08x",
          r1,
          (r2 >> 16) & 0xFFFF,
          r2 & 0x0FFF,
          0x8000 | ((r3 >> 16) & 0x3FFF),
          r3 & 0xFFFF,
          r4);
}

void obterTimestampISO(char* buffer, size_t tamanho) {
  struct tm timeinfo;
  if (getLocalTime(&timeinfo, 50) && timeinfo.tm_year > (2020 - 1900)) {
    strftime(buffer, tamanho, "%Y-%m-%dT%H:%M:%SZ", &timeinfo);
  } else {
    // Fallback UTC determinístico relativo ao tempo de atividade caso NTP ainda não tenha sincronizado
    unsigned long seg = millis() / 1000;
    snprintf(buffer, tamanho, "1970-01-01T%02lu:%02lu:%02luZ",
             (seg / 3600) % 24, (seg / 60) % 60, seg % 60);
  }
}

void preencherEnvelope(JsonDocument& doc, const char* tipoMensagem) {
  char msgId[40];
  char ts[32];
  gerarUUID(msgId);
  obterTimestampISO(ts, sizeof(ts));

  doc["schema_version"] = "1.0";
  doc["message_type"]   = tipoMensagem;
  doc["message_id"]     = msgId;
  doc["device_id"]      = DEVICE_ID;
  doc["timestamp"]      = ts;
  doc["sequence"]       = ++sequence;
  doc["boot_id"]        = bootId;
}

// ============================================================================
// MONTAGEM DOS TÓPICOS
// ============================================================================
void configurarTopicos() {
  snprintf(topicoTelemetria, sizeof(topicoTelemetria), "iot/v1/%s/%s/%s/telemetry", TENANT_ID, SITE_ID, DEVICE_ID);
  snprintf(topicoEstado, sizeof(topicoEstado), "iot/v1/%s/%s/%s/state", TENANT_ID, SITE_ID, DEVICE_ID);
  snprintf(topicoComando, sizeof(topicoComando), "iot/v1/%s/%s/%s/command", TENANT_ID, SITE_ID, DEVICE_ID);
  snprintf(topicoResultadoComando, sizeof(topicoResultadoComando), "iot/v1/%s/%s/%s/command-result", TENANT_ID, SITE_ID, DEVICE_ID);
}

// ============================================================================
// PUBLICAÇÃO DE ESTADO (ONLINE / OFFLINE)
// ============================================================================
void publicarEstado(const char* status) {
  StaticJsonDocument<512> doc;
  preencherEnvelope(doc, "state");

  JsonObject payload = doc.createNestedObject("payload");
  payload["status"]           = status;
  payload["uptime_s"]         = millis() / 1000;
  payload["firmware_version"] = "1.0.0-esp32";

  JsonArray cap = payload.createNestedArray("capabilities");
  cap.add("reset_sensor");
  cap.add("restart_device");

  char buffer[512];
  serializeJson(doc, buffer);
  client.publish(topicoEstado, buffer, true); // retain = true
}

// ============================================================================
// PUBLICAÇÃO DE TELEMETRIA
// ============================================================================
void enviarTelemetria() {
  StaticJsonDocument<640> doc;
  preencherEnvelope(doc, "telemetry");

  JsonObject payload = doc.createNestedObject("payload");
  payload["sample_interval_ms"] = INTERVALO_TELEMETRIA_MS;
  JsonArray metrics = payload.createNestedArray("metrics");

  // Exemplo de leitura real de sensor (ADC no pino 34)
  float leituraSensor = (analogRead(34) / 4095.0) * 100.0;
  float desvioPadrao  = 0.5; // Substituir pelo cálculo em janela móvel local

  JsonObject m1 = metrics.createNestedObject();
  m1["name"]    = "sensor_std";
  m1["value"]   = desvioPadrao;
  m1["unit"]    = "none";
  m1["quality"] = "good";

  JsonObject m2 = metrics.createNestedObject();
  m2["name"]    = "temperatura";
  m2["value"]   = leituraSensor;
  m2["unit"]    = "Cel";
  m2["quality"] = "good";

  char buffer[640];
  serializeJson(doc, buffer);
  client.publish(topicoTelemetria, buffer, false); // QoS 0, retain = false
}

// ============================================================================
// PROCESSAMENTO DE COMANDOS DO GATEWAY
// ============================================================================
void processarComando(char* topic, byte* payload, unsigned int length) {
  StaticJsonDocument<1024> doc;
  DeserializationError error = deserializeJson(doc, payload, length);

  if (error) {
    Serial.print("Erro no parse do comando: ");
    Serial.println(error.c_str());
    return;
  }

  const char* cmdId = doc["message_id"];
  const char* acao  = doc["payload"]["action"];
  if (!cmdId || !acao) {
    return;
  }

  Serial.printf("[COMANDO RECEBIDO] ID: %s | Ação: %s\n", cmdId, acao);

  char tsInicio[32];
  obterTimestampISO(tsInicio, sizeof(tsInicio));

  StaticJsonDocument<640> res;
  preencherEnvelope(res, "command-result");
  res["correlation_id"] = cmdId;

  JsonObject resPayload = res.createNestedObject("payload");
  resPayload["command_id"] = cmdId;
  resPayload["started_at"] = tsInicio;

  if (strcmp(acao, "reset_sensor") == 0) {
    // Ação física: desliga e liga o pino de alimentação do sensor por 200ms
    digitalWrite(PINO_ALIMENTACAO_SENSOR, LOW);
    delay(200);
    digitalWrite(PINO_ALIMENTACAO_SENSOR, HIGH);

    char tsFim[32];
    obterTimestampISO(tsFim, sizeof(tsFim));
    resPayload["completed_at"] = tsFim;
    resPayload["status"]       = "succeeded";

    JsonObject resultData = resPayload.createNestedObject("result");
    resultData["sensor_resetado"] = true;
  }
  else if (strcmp(acao, "restart_device") == 0) {
    char tsFim[32];
    obterTimestampISO(tsFim, sizeof(tsFim));
    resPayload["completed_at"] = tsFim;
    resPayload["status"]       = "succeeded";

    JsonObject resultData = resPayload.createNestedObject("result");
    resultData["reiniciando"] = true;

    char buffer[640];
    serializeJson(res, buffer);
    client.publish(topicoResultadoComando, buffer, false);
    delay(500);
    ESP.restart(); // Reinicia o microcontrolador
    return;
  }
  else {
    char tsFim[32];
    obterTimestampISO(tsFim, sizeof(tsFim));
    resPayload["completed_at"]  = tsFim;
    resPayload["status"]        = "rejected";
    resPayload["error_code"]    = "unsupported_action";
    resPayload["error_message"] = "Acao nao suportada pelo hardware";
  }

  char buffer[640];
  serializeJson(res, buffer);
  client.publish(topicoResultadoComando, buffer, false);
}

// ============================================================================
// CONEXÃO WI-FI E MQTT COM LWT CONFORME CONTRATO v1.0
// ============================================================================
void conectarWiFi() {
  Serial.print("Conectando ao Wi-Fi: ");
  Serial.println(WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWi-Fi Conectado!");
  Serial.print("IP do ESP32: ");
  Serial.println(WiFi.localIP());

  // Inicia sincronização NTP (UTC) para timestamps válidos no contrato
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
}

void reconectarMQTT() {
  while (!client.connected()) {
    Serial.print("Tentando conectar ao broker MQTT no Gateway... ");

    // Mensagem de Last Will and Testament (LWT) completa e válida pelo codec.py
    StaticJsonDocument<512> lwtDoc;
    char lwtMsgId[40];
    char lwtTs[32];
    gerarUUID(lwtMsgId);
    obterTimestampISO(lwtTs, sizeof(lwtTs));

    lwtDoc["schema_version"] = "1.0";
    lwtDoc["message_type"]   = "state";
    lwtDoc["message_id"]     = lwtMsgId;
    lwtDoc["device_id"]      = DEVICE_ID;
    lwtDoc["timestamp"]      = lwtTs;
    lwtDoc["sequence"]       = 0;
    lwtDoc["boot_id"]        = bootId;

    JsonObject lwtPayload = lwtDoc.createNestedObject("payload");
    lwtPayload["status"]  = "offline";
    lwtPayload["reason"]  = "lwt_unexpected_disconnect";

    char lwtBuffer[512];
    serializeJson(lwtDoc, lwtBuffer);

    // Conecta com LWT configurado no tópico state (retain=true, QoS=1)
    if (client.connect(DEVICE_ID, NULL, NULL, topicoEstado, 1, true, lwtBuffer)) {
      Serial.println("Conectado ao Broker!");
      publicarEstado("online");
      client.subscribe(topicoComando, 1);
    } else {
      Serial.printf("Falha, rc=%d. Tentando em 5 segundos...\n", client.state());
      delay(5000);
    }
  }
}

// ============================================================================
// SETUP E LOOP PRINCIPAL
// ============================================================================
void setup() {
  Serial.begin(115200);
  pinMode(PINO_ALIMENTACAO_SENSOR, OUTPUT);
  digitalWrite(PINO_ALIMENTACAO_SENSOR, HIGH);

  // Gera um boot_id único para esta inicialização
  snprintf(bootId, sizeof(bootId), "boot-%08x", esp_random());
  configurarTopicos();

  conectarWiFi();
  client.setServer(MQTT_BROKER, MQTT_PORT);
  client.setBufferSize(1024); // Permite envelopes JSON de até 1024 bytes
  client.setCallback(processarComando);
}

void loop() {
  if (!client.connected()) {
    reconectarMQTT();
  }
  client.loop();

  unsigned long agora = millis();
  if (agora - ultimoEnvioTelemetria >= INTERVALO_TELEMETRIA_MS) {
    ultimoEnvioTelemetria = agora;
    enviarTelemetria();
  }
}

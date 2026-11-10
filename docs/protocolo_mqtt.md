# Contrato MQTT dos dispositivos IoT

Versão do contrato: **1.0**.

Este documento define os tópicos e o formato JSON trocado por dispositivos, gateways,
agentes de borda e serviços de nuvem. O JSON usa nomes em inglês para facilitar a
interoperabilidade com firmware e ferramentas externas. As classes Python equivalentes
estão no pacote `agentes_ia/mqtt`.

### Organização do código

| Módulo | Responsabilidade |
|---|---|
| `tipos.py` | Enumerações, versão, limites e política de QoS/retain |
| `topicos.py` | Construção e interpretação da hierarquia de tópicos |
| `modelos.py` | Objetos imutáveis de telemetria, logs, estado e comandos |
| `codec.py` | Conversão de JSON não confiável para objetos validados |
| `validacao.py` | Validadores reutilizados pelos modelos e pelo codec |

Consumidores devem importar pela API pública `agentes_ia.mqtt`, sem depender dos
módulos internos. Essa fronteira permite trocar a biblioteca cliente MQTT sem alterar
os modelos do domínio.

## Tópicos

Formato geral:

```text
iot/v1/{tenant_id}/{site_id}/{device_id}/{message_type}
```

| Tipo | Direção usual | QoS | Retain | Finalidade |
|---|---|---:|---:|---|
| `telemetry` | dispositivo → borda | 0 | não | Amostras frequentes de sensores e recursos |
| `log` | dispositivo → borda | 1 | não | Registros diagnósticos importantes |
| `event` | dispositivo → borda | 1 | não | Mudanças discretas de estado |
| `state` | dispositivo → borda | 1 | sim | Estado atual e disponibilidade |
| `command` | borda → dispositivo | 1 | não | Solicitação de ação |
| `command-result` | dispositivo → borda | 1 | não | Aceite, progresso ou resultado da ação |

Exemplo:

```text
iot/v1/acme/fabrica-01/sensor-01/telemetry
```

O dispositivo deve configurar uma Last Will and Testament no tópico `state`, com
`status=offline`, QoS 1 e `retain=true`. Ao conectar, publica `status=online` no mesmo
tópico. Comandos não usam `retain`, evitando que um dispositivo execute um comando
antigo após se reconectar.

## Envelope comum

Toda mensagem possui o mesmo envelope:

```json
{
  "schema_version": "1.0",
  "message_type": "telemetry",
  "message_id": "a3d20ace-c739-48a8-890d-47eb765467ed",
  "device_id": "sensor-01",
  "timestamp": "2026-11-04T17:30:00.000Z",
  "sequence": 1842,
  "boot_id": "boot-a1",
  "gateway_id": "gateway-01",
  "correlation_id": "optional-operation-id",
  "payload": {}
}
```

- `message_id`: UUID único, usado para deduplicar entregas QoS 1.
- `sequence`: contador crescente durante uma inicialização do dispositivo.
- `boot_id`: muda a cada inicialização, permitindo detectar reinícios e reinício da sequência.
- `timestamp`: data ISO 8601 com fuso; na serialização Python é normalizada para UTC.
- `gateway_id`: opcional, identifica o gateway que encaminhou a mensagem.
- `correlation_id`: opcional, relaciona mensagens da mesma operação.

O consumidor deve considerar a tupla `(device_id, boot_id, sequence)` para ordenação e
`message_id` para deduplicação. Relógios de dispositivos podem apresentar desvio; o
receptor deverá registrar separadamente o horário de recebimento no broker.

O receptor também deve validar tópico e payload conjuntamente: `device_id` precisa ser
igual ao dispositivo indicado no tópico, e `message_type` precisa coincidir com o último
segmento. A função `decodificar_publicacao` realiza essa validação.

## Telemetria

```json
{
  "metrics": [
    {
      "name": "temperature",
      "value": 37.2,
      "unit": "Cel",
      "quality": "good",
      "tags": {"sensor": "internal"}
    },
    {
      "name": "cpu",
      "value": 81.4,
      "unit": "percent",
      "quality": "good"
    }
  ],
  "sample_interval_ms": 1000
}
```

Valores devem ser numéricos finitos. `quality` aceita `good`, `uncertain` ou `bad`.
Uma métrica pode ter seu próprio `timestamp` quando foi coletada em momento diferente
do envelope.

## Log

```json
{
  "level": "error",
  "component": "temperature_sensor",
  "code": "read_timeout",
  "message": "sensor did not answer",
  "context": {"timeout_ms": 500, "attempt": 3}
}
```

## Evento

```json
{
  "event": "device_restarted",
  "severity": "warning",
  "state": "watchdog",
  "attributes": {"restart_count": 2}
}
```

## Estado

```json
{
  "status": "online",
  "uptime_s": 3600,
  "firmware_version": "1.2.0",
  "capabilities": ["temperature", "restart_sensor"]
}
```

`status` aceita `online`, `offline`, `degraded` ou `maintenance`.

## Comando

O `message_id` do envelope é também a identidade do comando:

```json
{
  "action": "restart_sensor",
  "parameters": {"sensor": "internal"},
  "expires_at": "2026-11-04T17:31:00.000Z",
  "requested_by": "edge-agent",
  "requires_ack": true,
  "policy_id": "safe-restart-v1"
}
```

Antes da execução, o dispositivo verifica expiração, ação permitida e duplicidade do
`message_id`. O recebimento deve gerar ao menos um `command-result` com estado `accepted`
ou `rejected`.

## Resultado de comando

```json
{
  "command_id": "a3d20ace-c739-48a8-890d-47eb765467ed",
  "status": "succeeded",
  "started_at": "2026-11-04T17:30:02.000Z",
  "completed_at": "2026-11-04T17:30:04.000Z",
  "result": {"restarted": true}
}
```

Estados aceitos: `accepted`, `running`, `succeeded`, `failed`, `rejected` e `timed-out`.
O `correlation_id` do envelope deve receber o `message_id` do comando original.

## Requisitos de segurança

- MQTT sobre TLS em produção.
- Identidade individual por dispositivo, preferencialmente certificado cliente.
- ACL do broker: dispositivo publica somente seus próprios dados e lê somente seu tópico de comando.
- Limite de tamanho aplicado antes do parse; o padrão da implementação é 256 KiB.
- Campos desconhecidos, números não finitos, UTF-8 inválido e versões incompatíveis são rejeitados.
- Segredos, tokens e senhas nunca são enviados em logs ou atributos.

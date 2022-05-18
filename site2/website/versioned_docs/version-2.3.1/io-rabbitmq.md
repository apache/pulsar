---
id: io-rabbitmq
title: RabbitMQ Connector
sidebar_label: "RabbitMQ Connector"
original_id: io-rabbitmq
---

## Source

The RabbitMQ Source connector is used for receiving messages from a RabbitMQ cluster and writing
messages to Pulsar topics.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `connectionName` | `true` | `null` | The connection name used for connecting to RabbitMQ. |
| `host` | `true` | `null` | The RabbitMQ host to connect to. |
| `port` | `true` | `5672` | The RabbitMQ port to connect to. |
| `virtualHost` | `true` | `/` | The virtual host used for connecting to RabbitMQ. |
| `username` | `false` | `guest` | The username used to authenticate to RabbitMQ. |
| `password` | `false` | `guest` | The password used to authenticate to RabbitMQ. |
| `queueName` | `true` | `null` | The RabbitMQ queue name from which messages should be read from or written to. |
| `requestedChannelMax` | `false` | `0` | Initially requested maximum channel number. 0 for unlimited. |
| `requestedFrameMax` | `false` | `0` | Initially requested maximum frame size, in octets. 0 for unlimited. |
| `connectionTimeout` | `false` | `60000` | Connection TCP establishment timeout in milliseconds. 0 for infinite. |
| `handshakeTimeout` | `false` | `10000` | The AMQP0-9-1 protocol handshake timeout in milliseconds. |
| `requestedHeartbeat` | `false` | `60` | The requested heartbeat timeout in seconds. |
| `prefetchCount` | `false` | `0` | Maximum number of messages that the server will deliver, 0 for unlimited. |
| `prefetchGlobal` | `false` | `false` | Set true if the settings should be applied to the entire channel rather than each consumer. |

## Sink

The RabbitMQ Sink connector is used to pull messages from Pulsar topics and persist the messages
to a RabbitMQ queue.

### Sink Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `connectionName` | `true` | `null` | The connection name used for connecting to RabbitMQ. |
| `host` | `true` | `null` | The RabbitMQ host to connect to. |
| `port` | `true` | `5672` | The RabbitMQ port to connect to. |
| `virtualHost` | `true` | `/` | The virtual host used for connecting to RabbitMQ. |
| `username` | `false` | `guest` | The username used to authenticate to RabbitMQ. |
| `password` | `false` | `guest` | The password used to authenticate to RabbitMQ. |
| `queueName` | `true` | `null` | The RabbitMQ queue name from which messages should be read from or written to. |
| `requestedChannelMax` | `false` | `0` | Initially requested maximum channel number. 0 for unlimited. |
| `requestedFrameMax` | `false` | `0` | Initially requested maximum frame size, in octets. 0 for unlimited. |
| `connectionTimeout` | `false` | `60000` | Connection TCP establishment timeout in milliseconds. 0 for infinite. |
| `handshakeTimeout` | `false` | `10000` | The AMQP0-9-1 protocol handshake timeout in milliseconds. |
| `requestedHeartbeat` | `false` | `60` | The requested heartbeat timeout in seconds. |
| `exchangeName` | `true` | `null` | The exchange to publish the messages on. |
| `routingKey` | `true` | `null` | The routing key used for publishing the messages. |


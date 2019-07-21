---
id: version-2.2.1-io-rabbitmq
title: RabbitMQ Connector
sidebar_label: RabbitMQ Connector
original_id: io-rabbitmq
---

## Source

The RabbitMQ Source connector is used for receiving messages from a RabbitMQ cluster and writing
messages to Pulsar topics.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `connectionName` | `true` | `null` | A new broker connection name. |
| `amqUri` | `true` | `null` | An AMQP URI: host, port, username, password and virtual host. |
| `queueName` | `true` | `null` | RabbitMQ queue name. |


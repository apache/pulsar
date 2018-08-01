---
id: io-rabbitmq
title: RabbitMQ Connector
sidebar_label: RabittMQ Connector
---

## Source

The RabittMQ Source connector is used for receiving messages from a RabittMQ cluster and writing
messages to Pulsar topics.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `connectionName` | `true` | `null` | A new broker connection name. |
| `amqUri` | `true` | `null` | An AMQP URI: host, port, username, password and virtual host. |
| `queueName` | `true` | `null` | RabbitMQ queue name. |


---
id: version-2.3.1-io-mongo
title: MongoDB Connector
sidebar_label: MongoDB Connector
original_id: io-mongo
---

## Sink

The MongoDB Sink Connector is used to pull messages from Pulsar topics and persist the messages
to a collection.

## Sink Configuration Options

| Name | Default | Required | Description |
|------|---------|----------|-------------|
| `mongoUri` | `null` | `true` | The uri of mongodb that the connector connects to (see: https://docs.mongodb.com/manual/reference/connection-string/). |
| `database` | `null` | `true` | The name of the database to which the collection belongs to. |
| `collection` | `null` | `true` | The collection name that the connector writes messages to. |
| `batchSize` | `100` | `false` | The batch size of write to the collection. |
| `batchTimeMs` | `1000` | `false` | The batch operation interval in milliseconds. |
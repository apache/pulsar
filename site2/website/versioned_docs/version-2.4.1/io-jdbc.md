---
id: io-jdbc
title: JDBC Connector
sidebar_label: "JDBC Connector"
original_id: io-jdbc
---

## Sink

The JDBC Sink Connector is used to pull messages from Pulsar topics and persist the messages to an MySQL or Sqlite.
Current support INSERT, DELETE and UPDATE.

### Sink Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| userName | `false` | `` | Username used to connect to the database specified by `jdbcUrl`. |
| password | `false` | `` | Password used to connect to the database specified by `jdbcUrl`. |
| jdbcUrl | `true` | `` | The JDBC url of the database this connector connects to. |
| tableName | `true` | `` | The name of the table this connector writes messages to. |
| nonKey | `false` | `` | Fields used in update events. A comma-separated list. |
| key | `false` | `` | Fields used in where condition of update and delete Events. A comma-separated list. |
| timeoutMs | `false` | `500` | The jdbc operation timeout in milliseconds. |
| batchSize | `false` | `200` | The batch size of updates made to the database. |
---
id: io-mongo-source
title: MongoDB source connector
sidebar_label: "MongoDB source connector"
---

The MongoDB source connector pulls documents from MongoDB and persists the messages to Pulsar topics.

This guide explains how to configure and use the MongoDB source connector.

## Configuration

The configuration of the MongoDB source connector has the following properties.

### Property

| Name          | Type   | Required | Default            | Description                                                                                                                                                                                    |
|---------------|--------|----------|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mongoUri`    | String | true     | " " (empty string) | The MongoDB URI to which the connector connects. <br /><br />For more information, see [connection string URI format](https://docs.mongodb.com/manual/reference/connection-string/).           |
| `database`    | String | false    | " " (empty string) | The name of the watched database. <br /><br />If this field is not set, the source connector will watch the entire MongoDB for all changes.                                                    |
| `collection`  | String | false    | " " (empty string) | The name of the watched collection. <br /><br />If this field is not set, the source connector will watch the database for all changes.                                                        |
| `syncType`    | String | false    | "INCR_SYNC"        | The synchronization type between MongoDB and Pulsar: full synchronization or incremental synchronization. <br /><br /> Valid values are `full_sync`, `FULL_SYNC`, `incr_sync` and `INCR_SYNC`. |
| `batchSize`   | int    | false    | 100                | The batch size of pulling documents from collections.                                                                                                                                          |
| `batchTimeMs` | long   | false    | 1000               | The batch operation interval in milliseconds.                                                                                                                                                  |

### Example

Before using the Mongo source connector, you need to create a configuration file through one of the following methods.

* JSON

  ```json
  {
     "configs": {
        "mongoUri": "mongodb://localhost:27017",
        "database": "pulsar",
        "collection": "messages",
        "syncType": "full_sync",
        "batchSize": "2",
        "batchTimeMs": "500"
     }
  }
  ```

* YAML

  ```yaml
  configs:
      mongoUri: "mongodb://localhost:27017"
      database: "pulsar"
      collection: "messages"
      syncType: "full_sync",
      batchSize: 2
      batchTimeMs: 500
  ```

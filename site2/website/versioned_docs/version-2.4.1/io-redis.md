---
id: io-redis
title: redis Connector
sidebar_label: "redis Connector"
original_id: io-redis
---

## Sink

The redis Sink Connector is used to pull messages from Pulsar topics and persist the messages
to a redis database.

## Sink Configuration Options

| Name | Default | Required | Description |
|------|---------|----------|-------------|
| `redisHosts` | `null` | `true` | A comma separated list of Redis hosts to connect to. |
| `redisPassword` | `null` | `false` | The password used to connect to Redis. |
| `redisDatabase` | `0` | `true` | The Redis database to connect to. |
| `clientMode` | `Standalone` | `false` | The client mode to use when interacting with the Redis cluster. Possible values [Standalone, Cluster]. |
| `autoReconnect` | `true` | `false` | Flag to determine if the Redis client should automatically reconnect. |
| `requestQueue` | `2147483647` | `false` | The maximum number of queued requests to Redis. |
| `tcpNoDelay` | `false` | `false` | Flag to enable TCP no delay should be used. |
| `keepAlive` | `false` | `false` | Flag to enable a keepalive to Redis. |
| `connectTimeout` | `10000` | `false` | The amount of time in milliseconds to wait before timing out when connecting. |
| `operationTimeout` | `10000` | `false` | The amount of time in milliseconds before an operation is marked as timed out. |
| `batchTimeMs` | `1000` | `false` | The Redis operation time in milliseconds. |
| `batchSize` | `1000` | `false` | The batch size of write to Redis database. |
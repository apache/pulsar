---
id: version-2.3.1-io-elasticsearch
title: ElasticSearch Connector
sidebar_label: ElasticSearch Connector
original_id: io-elasticsearch
---

## Sink

The ElasticSearch Sink Connector is used to pull messages from Pulsar topics and persist the messages
to a index.

## Sink Configuration Options

| Name | Default | Required | Description |
|------|---------|----------|-------------|
| `elasticSearchUrl` | `null` | `true` | The url of elastic search cluster that the connector connects to. |
| `indexName` | `null` | `true` | The index name that the connector writes messages to. |
| `indexNumberOfShards` | `1` | `false` | The number of shards of the index. |
| `indexNumberOfReplicas` | `1` | `false` | The number of replicas of the index. |
| `username` | `null` | `false` | The username used by the connector to connect to the elastic search cluster. If username is set, a password should also be provided. |
| `password` | `null` | `false` | The password used by the connector to connect to the elastic search cluster. If password is set, a username should also be provided. |
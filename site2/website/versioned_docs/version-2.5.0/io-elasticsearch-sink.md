---
id: io-elasticsearch-sink
title: ElasticSearch sink connector
sidebar_label: "ElasticSearch sink connector"
original_id: io-elasticsearch-sink
---

The ElasticSearch sink connector pulls messages from Pulsar topics and persists the messages to indexes.

## Configuration

The configuration of the ElasticSearch sink connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `elasticSearchUrl` | String| true |" " (empty string)| The URL of elastic search cluster to which the connector connects. |
| `indexName` | String| true |" " (empty string)| The index name to which the connector writes messages. |
| `indexNumberOfShards` | int| false |1| The number of shards of the index. |
| `indexNumberOfReplicas` | int| false |1 | The number of replicas of the index. |
| `username` | String| false |" " (empty string)| The username used by the connector to connect to the elastic search cluster. <br /><br />If `username` is set, then `password` should also be provided. |
| `password` | String| false | " " (empty string)|The password used by the connector to connect to the elastic search cluster. <br /><br />If `username` is set, then `password` should also be provided.  |

### Example

Before using the ElasticSearch sink connector, you need to create a configuration file through one of the following methods.

* JSON 

  ```json
  
  {
      "elasticSearchUrl": "http://localhost:90902",
      "indexName": "myIndex",
      "username": "scooby",
      "password": "doobie"
  }
  
  ```

* YAML

  ```yaml
  
  configs:
      elasticSearchUrl: "http://localhost:90902"
      indexName: "myIndex"
      username: "scooby"
      password: "doobie"
  
  ```


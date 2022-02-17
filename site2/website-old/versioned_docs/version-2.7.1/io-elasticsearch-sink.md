---
id: version-2.7.1-io-elasticsearch-sink
title: ElasticSearch sink connector
sidebar_label: ElasticSearch sink connector
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
| `typeName` | String | false | "_doc" | The type name to which the connector writes messages to. <br><br> The value should be set explicitly to a valid type name other than "_doc" for Elasticsearch version before 6.2, and left to default otherwise. |
| `indexNumberOfShards` | int| false |1| The number of shards of the index. |
| `indexNumberOfReplicas` | int| false |1 | The number of replicas of the index. |
| `username` | String| false |" " (empty string)| The username used by the connector to connect to the elastic search cluster. <br><br>If `username` is set, then `password` should also be provided. |
| `password` | String| false | " " (empty string)|The password used by the connector to connect to the elastic search cluster. <br><br>If `username` is set, then `password` should also be provided.  |

## Example

Before using the ElasticSearch sink connector, you need to create a configuration file through one of the following methods.

### Configuration

#### For Elasticsearch After 6.2

* JSON 

    ```json
    {
        "elasticSearchUrl": "http://localhost:9200",
        "indexName": "my_index",
        "username": "scooby",
        "password": "doobie"
    }
    ```

* YAML

    ```yaml
    configs:
        elasticSearchUrl: "http://localhost:9200"
        indexName: "my_index"
        username: "scooby"
        password: "doobie"
    ```

#### For Elasticsearch Before 6.2

* JSON 

    ```json
    {
        "elasticSearchUrl": "http://localhost:9200",
        "indexName": "my_index",
        "typeName": "doc",
        "username": "scooby",
        "password": "doobie"
    }
    ```

* YAML

    ```yaml
    configs:
        elasticSearchUrl: "http://localhost:9200"
        indexName: "my_index"
        typeName: "doc"
        username: "scooby"
        password: "doobie"
    ```

### Usage

1. Start a single node Elasticsearch cluster.

    ```bash
    $ docker run -p 9200:9200 -p 9300:9300 \
        -e "discovery.type=single-node" \
        docker.elastic.co/elasticsearch/elasticsearch:7.5.1
    ```

2. Start a Pulsar service locally in standalone mode.
    ```bash
    $ bin/pulsar standalone
    ```
    Make sure the NAR file is available at `connectors/pulsar-io-elastic-search-{{pulsar:version}}.nar`.

3. Start the Pulsar Elasticsearch connector in local run mode using one of the following methods.
    * Use the **JSON** configuration as shown previously. 
        ```bash
        $ bin/pulsar-admin sinks localrun \
            --archive connectors/pulsar-io-elastic-search-{{pulsar:version}}.nar \
            --tenant public \
            --namespace default \
            --name elasticsearch-test-sink \
            --sink-config '{"elasticSearchUrl":"http://localhost:9200","indexName": "my_index","username": "scooby","password": "doobie"}' \
            --inputs elasticsearch_test
        ```
    * Use the **YAML** configuration file as shown previously.
    
        ```bash
        $ bin/pulsar-admin sinks localrun \
            --archive connectors/pulsar-io-elastic-search-{{pulsar:version}}.nar \
            --tenant public \
            --namespace default \
            --name elasticsearch-test-sink \
            --sink-config-file elasticsearch-sink.yml \
            --inputs elasticsearch_test
        ```

4. Publish records to the topic.

    ```bash
    $ bin/pulsar-client produce elasticsearch_test --messages "{\"a\":1}"
    ```

5. Check documents in Elasticsearch.
    
    * refresh the index
        ```bash
            $ curl -s http://localhost:9200/my_index/_refresh
        ``` 
    * search documents
        ```bash
            $ curl -s http://localhost:9200/my_index/_search
        ```
        You can see the record that published earlier has been successfully written into Elasticsearch.
        ```json
        {"took":2,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"max_score":1.0,"hits":[{"_index":"my_index","_type":"_doc","_id":"FSxemm8BLjG_iC0EeTYJ","_score":1.0,"_source":{"a":1}}]}}
        ```

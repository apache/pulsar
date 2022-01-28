---
id: io-elasticsearch-sink
title: Elasticsearch sink connector
sidebar_label: "Elasticsearch sink connector"
---

The Elasticsearch sink connector pulls messages from Pulsar topics and persists the messages to indexes.


## Feature 

### Handle data
		
Since Pulsar 2.9.0, the Elasticsearch sink connector has the following ways of
working. You can choose one of them.

Name | Description
---|---|
Raw processing | The sink reads from topics and passes the raw content to Elasticsearch. <br /><br /> This is the **default** behavior. <br /><br /> Raw processing was already available **in Pulsar 2.8.x**.
Schema aware | The sink uses the schema and handles AVRO, JSON, and KeyValue schema types while mapping the content to the Elasticsearch document.<br /><br /> If you set `schemaEnable` to `true`, the sink interprets the contents of the message and you can define a **primary key** that in turn used as the special `_id` field on Elasticsearch.
<br /><br /> This allows you to perform `UPDATE`, `INSERT`, and `DELETE` operations
to Elasticsearch driven by the logical primary key of the message.<br /><br /> This
is very useful in a typical Change Data Capture scenario in which you follow the
changes on your database, write them to Pulsar (using the Debezium adapter for
instance), and then you write to Elasticsearch.<br /><br /> You configure the
mapping of the primary key using the `primaryFields` configuration
entry.<br /><br />The `DELETE` operation can be performed when the primary key is
not empty and the remaining value is empty. Use the `nullValueAction` to
configure this behaviour. The default configuration simply ignores such empty
values.
		
### Map multiple indexes
		
Since Pulsar 2.9.0, the `indexName` property is no more required. If you omit it, the sink writes to an index name after the Pulsar topic name.
		
### Enable bulk writes
		
Since Pulsar 2.9.0, you can use bulk writes by setting the `bulkEnabled` property to `true`.
		
### Enable secure connections via TLS
		
Since Pulsar 2.9.0, you can enable secure connections with TLS.  

## Configuration

The configuration of the Elasticsearch sink connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `elasticSearchUrl` | String| true |" " (empty string)| The URL of elastic search cluster to which the connector connects. |
| `indexName` | String| true |" " (empty string)| The index name to which the connector writes messages. |
| `schemaEnable` | Boolean | false | false | Turn on the Schema Aware mode. |
| `createIndexIfNeeded` | Boolean | false | false | Manage index if missing. |
| `maxRetries` | Integer | false | 1 | The maximum number of retries for elasticsearch requests. Use -1 to disable it.  |
| `retryBackoffInMs` | Integer | false | 100 | The base time to wait when retrying an Elasticsearch request (in milliseconds). |
| `maxRetryTimeInSec` | Integer| false | 86400 | The maximum retry time interval in seconds for retrying an elasticsearch request. |
| `bulkEnabled` | Boolean | false | false | Enable the elasticsearch bulk processor to flush write requests based on the number or size of requests, or after a given period. |
| `bulkActions` | Integer | false | 1000 | The maximum number of actions per elasticsearch bulk request. Use -1 to disable it. |
| `bulkSizeInMb` | Integer | false |5 | The maximum size in megabytes of elasticsearch bulk requests. Use -1 to disable it. |
| `bulkConcurrentRequests` | Integer | false | 0 | The maximum number of in flight elasticsearch bulk requests. The default 0 allows the execution of a single request. A value of 1 means 1 concurrent request is allowed to be executed while accumulating new bulk requests. |
| `bulkFlushIntervalInMs` | Integer | false | -1 | The maximum period of time to wait for flushing pending writes when bulk writes are enabled. Default is -1 meaning not set. |
| `compressionEnabled` | Boolean | false |false | Enable elasticsearch request compression. |
| `connectTimeoutInMs` | Integer | false |5000 | The elasticsearch client connection timeout in milliseconds. |
| `connectionRequestTimeoutInMs` | Integer | false |1000 | The time in milliseconds for getting a connection from the elasticsearch connection pool. |
| `connectionIdleTimeoutInMs` | Integer | false |5 | Idle connection timeout to prevent a read timeout. |
| `keyIgnore` | Boolean | false |true | Whether to ignore the record key to build the Elasticsearch document `_id`. If primaryFields is defined, the connector extract the primary fields from the payload to build the document `_id` If no primaryFields are provided, elasticsearch auto generates a random document `_id`. |
| `primaryFields` | String | false | "id" | The comma separated ordered list of field names used to build the Elasticsearch document `_id` from the record value. If this list is a singleton, the field is converted as a string. If this list has 2 or more fields, the generated `_id` is a string representation of a JSON array of the field values. |
| `nullValueAction` | enum (IGNORE,DELETE,FAIL) | false | IGNORE | How to handle records with null values, possible options are IGNORE, DELETE or FAIL. Default is IGNORE the message. |
| `malformedDocAction` | enum (IGNORE,WARN,FAIL) | false | FAIL | How to handle elasticsearch rejected documents due to some malformation. Possible options are IGNORE, DELETE or FAIL. Default is FAIL the Elasticsearch document. |
| `stripNulls` | Boolean | false |true | If stripNulls is false, elasticsearch _source includes 'null' for empty fields (for example {"foo": null}), otherwise null fields are stripped. |
| `socketTimeoutInMs` | Integer | false |60000 | The socket timeout in milliseconds waiting to read the elasticsearch response. |
| `typeName` | String | false | "_doc" | The type name to which the connector writes messages to. <br /><br /> The value should be set explicitly to a valid type name other than "_doc" for Elasticsearch version before 6.2, and left to default otherwise. |
| `indexNumberOfShards` | int| false |1| The number of shards of the index. |
| `indexNumberOfReplicas` | int| false |1 | The number of replicas of the index. |
| `username` | String| false |" " (empty string)| The username used by the connector to connect to the elastic search cluster. <br /><br />If `username` is set, then `password` should also be provided. |
| `password` | String| false | " " (empty string)|The password used by the connector to connect to the elastic search cluster. <br /><br />If `username` is set, then `password` should also be provided.  |
| `ssl` | ElasticSearchSslConfig | false |  | Configuration for TLS encrypted communication |

### Definition of ElasticSearchSslConfig structure:

| Name | Type|Required | Default | Description
|------|----------|----------|---------|-------------|
| `enabled` | Boolean| false | false | Enable SSL/TLS. |
| `hostnameVerification` | Boolean| false | true | Whether or not to validate node hostnames when using SSL. |
| `truststorePath` | String| false |" " (empty string)| The path to the truststore file. |
| `truststorePassword` | String| false |" " (empty string)| Truststore password. |
| `keystorePath` | String| false |" " (empty string)| The path to the keystore file. |
| `keystorePassword` | String| false |" " (empty string)| Keystore password. |
| `cipherSuites` | String| false |" " (empty string)| SSL/TLS cipher suites. |
| `protocols` | String| false |"TLSv1.2" | Comma separated list of enabled SSL/TLS protocols. |

## Example

Before using the Elasticsearch sink connector, you need to create a configuration file through one of the following methods.

### Configuration

#### For Elasticsearch After 6.2

* JSON 

  ```json
  
  {
     "configs": {
        "elasticSearchUrl": "http://localhost:9200",
        "indexName": "my_index",
        "username": "scooby",
        "password": "doobie"
     }
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
       docker.elastic.co/elasticsearch/elasticsearch:7.13.3
   
   ```

2. Start a Pulsar service locally in standalone mode.

   ```bash
   
   $ bin/pulsar standalone
   
   ```

   Make sure the NAR file is available at `connectors/pulsar-io-elastic-search-@pulsar:version@.nar`.

3. Start the Pulsar Elasticsearch connector in local run mode using one of the following methods.
   * Use the **JSON** configuration as shown previously. 

       ```bash
       
       $ bin/pulsar-admin sinks localrun \
           --archive connectors/pulsar-io-elastic-search-@pulsar:version@.nar \
           --tenant public \
           --namespace default \
           --name elasticsearch-test-sink \
           --sink-config '{"elasticSearchUrl":"http://localhost:9200","indexName": "my_index","username": "scooby","password": "doobie"}' \
           --inputs elasticsearch_test
       
       ```

   * Use the **YAML** configuration file as shown previously.

       ```bash
       
       $ bin/pulsar-admin sinks localrun \
           --archive connectors/pulsar-io-elastic-search-@pulsar:version@.nar \
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


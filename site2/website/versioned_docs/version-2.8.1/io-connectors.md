---
id: version-2.8.1-io-connectors
title: Built-in connector
sidebar_label: Built-in connector
original_id: io-connectors
---

Pulsar distribution includes a set of common connectors that have been packaged and tested with the rest of Apache Pulsar. These connectors import and export data from some of the most commonly used data systems. 

Using any of these connectors is as easy as writing a simple connector and running the connector locally or submitting the connector to a Pulsar Functions cluster.

## Source connector

Pulsar has various source connectors, which are sorted alphabetically as below.

### Canal

* [Configuration](io-canal-source.md#configuration)

* [Example](io-canal-source.md#usage)

* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/canal/src/main/java/org/apache/pulsar/io/canal/CanalStringSource.java)


### Debezium MySQL

* [Configuration](io-debezium-source.md#configuration)

* [Example](io-debezium-source.md#example-of-mysql)
 
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/mysql/src/main/java/org/apache/pulsar/io/debezium/mysql/DebeziumMysqlSource.java)

### Debezium PostgreSQL

* [Configuration](io-debezium-source.md#configuration)

* [Example](io-debezium-source.md#example-of-postgresql)

* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/postgres/src/main/java/org/apache/pulsar/io/debezium/postgres/DebeziumPostgresSource.java)

### Debezium MongoDB

* [Configuration](io-debezium-source.md#configuration)

* [Example](io-debezium-source.md#example-of-mongodb)

* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/mongodb/src/main/java/org/apache/pulsar/io/debezium/mongodb/DebeziumMongoDbSource.java)

### DynamoDB

* [Configuration](io-dynamodb-source.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/dynamodb/src/main/java/org/apache/pulsar/io/dynamodb/DynamoDBSource.java)

### File

* [Configuration](io-file-source.md#configuration)

* [Example](io-file-source.md#usage)

* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/file/src/main/java/org/apache/pulsar/io/file/FileSource.java)

### Flume

* [Configuration](io-flume-source.md#configuration)

* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/flume/src/main/java/org/apache/pulsar/io/flume/FlumeConnector.java)

### Twitter firehose

* [Configuration](io-twitter-source.md#configuration)

* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/twitter/src/main/java/org/apache/pulsar/io/twitter/TwitterFireHose.java)

### Kafka

* [Configuration](io-kafka-source.md#configuration)

* [Example](io-kafka-source.md#usage)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSource.java)

### Kinesis

* [Configuration](io-kinesis-source.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/kinesis/src/main/java/org/apache/pulsar/io/kinesis/KinesisSource.java)
  
### Netty

* [Configuration](io-netty-source.md#configuration)

* [Example of TCP](io-netty-source.md#tcp)

* [Example of HTTP](io-netty-source.md#http)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/netty/src/main/java/org/apache/pulsar/io/netty/NettySource.java)

### NSQ

* [Configuration](io-nsq-source.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/nsq/src/main/java/org/apache/pulsar/io/nsq/NSQSource.java)

### RabbitMQ

* [Configuration](io-rabbitmq-source.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/rabbitmq/src/main/java/org/apache/pulsar/io/rabbitmq/RabbitMQSource.java)
  
## Sink connector

Pulsar has various sink connectors, which are sorted alphabetically as below.

### Aerospike

* [Configuration](io-aerospike-sink.md#configuration)

* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/aerospike/src/main/java/org/apache/pulsar/io/aerospike/AerospikeStringSink.java)
  
### Cassandra

* [Configuration](io-cassandra-sink.md#configuration)
  
* [Example](io-cassandra-sink.md#usage)  
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/cassandra/src/main/java/org/apache/pulsar/io/cassandra/CassandraStringSink.java)

### ElasticSearch

* [Configuration](io-elasticsearch-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/elastic-search/src/main/java/org/apache/pulsar/io/elasticsearch/ElasticSearchSink.java)

### Flume

* [Configuration](io-flume-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/flume/src/main/java/org/apache/pulsar/io/flume/sink/StringSink.java)

### HBase

* [Configuration](io-hbase-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/hbase/src/main/java/org/apache/pulsar/io/hbase/HbaseAbstractConfig.java)
  
### HDFS2

* [Configuration](io-hdfs2-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/hdfs2/src/main/java/org/apache/pulsar/io/hdfs2/AbstractHdfsConnector.java)

### HDFS3

* [Configuration](io-hdfs3-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/hdfs3/src/main/java/org/apache/pulsar/io/hdfs3/AbstractHdfsConnector.java)

### InfluxDB

* [Configuration](io-influxdb-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/influxdb/src/main/java/org/apache/pulsar/io/influxdb/InfluxDBGenericRecordSink.java)

### JDBC ClickHouse

* [Configuration](io-jdbc-sink.md#configuration)
  
* [Example](io-jdbc-sink.md#example-for-clickhouse)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/jdbc/clickhouse/src/main/java/org/apache/pulsar/io/jdbc/ClickHouseJdbcAutoSchemaSink.java)

### JDBC MariaDB

* [Configuration](io-jdbc-sink.md#configuration)
  
* [Example](io-jdbc-sink.md#example-for-mariadb)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/jdbc/mariadb/src/main/java/org/apache/pulsar/io/jdbc/MariadbJdbcAutoSchemaSink.java)

### JDBC PostgreSQL

* [Configuration](io-jdbc-sink.md#configuration)
  
* [Example](io-jdbc-sink.md#example-for-postgresql)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/jdbc/postgres/src/main/java/org/apache/pulsar/io/jdbc/PostgresJdbcAutoSchemaSink.java)
  
### JDBC SQLite

* [Configuration](io-jdbc-sink.md#configuration)
  
* [Example](io-jdbc-sink.md#example-for-sqlite)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/jdbc/sqlite/src/main/java/org/apache/pulsar/io/jdbc/SqliteJdbcAutoSchemaSink.java)
  
### Kafka

* [Configuration](io-kafka-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSink.java)
  
### Kinesis

* [Configuration](io-kinesis-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/kinesis/src/main/java/org/apache/pulsar/io/kinesis/KinesisSink.java)
  
### MongoDB

* [Configuration](io-mongo-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/mongo/src/main/java/org/apache/pulsar/io/mongodb/MongoSink.java)
  
### RabbitMQ

* [Configuration](io-rabbitmq-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/rabbitmq/src/main/java/org/apache/pulsar/io/rabbitmq/RabbitMQSink.java)
  
### Redis

* [Configuration](io-redis-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/redis/src/main/java/org/apache/pulsar/io/redis/RedisAbstractConfig.java)
  
### Solr

* [Configuration](io-solr-sink.md#configuration)
  
* [Java class](https://github.com/apache/pulsar/blob/master/pulsar-io/solr/src/main/java/org/apache/pulsar/io/solr/SolrSinkConfig.java)
  

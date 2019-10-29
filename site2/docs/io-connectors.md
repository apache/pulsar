---
id: io-connectors
title: Built-in connector
sidebar_label: Built-in connector
---

Pulsar distribution includes a set of common connectors that have been packaged and tested with the rest of Apache Pulsar. These connectors import and export data from some of the most commonly used data systems.

Using any of these connectors is as easy as writing a simple connector and running the connector locally or submitting the connector to a Pulsar Functions cluster.

## Source connector

Pulsar has various source connectors, which are sorted alphabetically as below.

Name|Java class
|---|---
[Canal](io-canal-source.md) |[org.apache.pulsar.io.canal.CanalStringSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/canal/src/main/java/org/apache/pulsar/io/canal/CanalStringSource.java)
[Debezium MySQL](io-debezium-source.md)|[org.apache.pulsar.io.debezium.mysql.DebeziumMysqlSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/mysql/src/main/java/org/apache/pulsar/io/debezium/mysql/DebeziumMysqlSource.java)
[Debezium PostgreSQL](io-debezium-source.md#example-of-postgresql)|[org.apache.pulsar.io.debezium.postgres.DebeziumPostgresSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/postgres/src/main/java/org/apache/pulsar/io/debezium/postgres/DebeziumPostgresSource.java)
[File](io-file-source.md)|[org.apache.pulsar.io.file.FileSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/file/src/main/java/org/apache/pulsar/io/file/FileSource.java)
[Flume](io-flume-source.md)|[org.apache.pulsar.io.flume.FlumeConnector.java](https://github.com/apache/pulsar/blob/master/pulsar-io/flume/src/main/java/org/apache/pulsar/io/flume/FlumeConnector.java)
[Twitter firehose](io-twitter-source.md)|[org.apache.pulsar.io.twitter.TwitterFireHose.java](https://github.com/apache/pulsar/blob/master/pulsar-io/twitter/src/main/java/org/apache/pulsar/io/twitter/TwitterFireHose.java)
[Kafka](io-kafka-source.md)|[org.apache.pulsar.io.kafka.KafkaAbstractSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSource.java)
[Kinesis](io-kinesis-source.md)|[org.apache.pulsar.io.kinesis.KinesisSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/kinesis/src/main/java/org/apache/pulsar/io/kinesis/KinesisSource.java)
[Netty](io-netty-source.md)|[org.apache.pulsar.io.netty.NettySource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/netty/src/main/java/org/apache/pulsar/io/netty/NettySource.java)
[RabbitMQ](io-rabbitmq-source.md)|[org.apache.pulsar.io.rabbitmq.RabbitMQSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/rabbitmq/src/main/java/org/apache/pulsar/io/rabbitmq/RabbitMQSource.java)

## Sink connector

Pulsar has various sink connectors, which are sorted alphabetically as below.

Name|Java class
|---|---
[Aerospike](io-aerospike-sink.md)|[org.apache.pulsar.io.aerospike.AerospikeStringSink.java](https://github.com/apache/pulsar/blob/master/pulsar-io/aerospike/src/main/java/org/apache/pulsar/io/aerospike/AerospikeStringSink.java)
[Cassandra](io-cassandra-sink.md)|[org.apache.pulsar.io.cassandra.CassandraStringSink.java](https://github.com/apache/pulsar/blob/master/pulsar-io/cassandra/src/main/java/org/apache/pulsar/io/cassandra/CassandraStringSink.java)
[ElasticSearch](io-elasticsearch-sink.md)|[org.apache.pulsar.io.elasticsearch.ElasticSearchSink.java](https://github.com/apache/pulsar/blob/master/pulsar-io/elastic-search/src/main/java/org/apache/pulsar/io/elasticsearch/ElasticSearchSink.java)
[Flume](io-flume-sink.md)|[org.apache.pulsar.io.flume.sink.StringSink.java](https://github.com/apache/pulsar/blob/master/pulsar-io/flume/src/main/java/org/apache/pulsar/io/flume/sink/StringSink.java)
[HBase](io-hbase.md)|[org.apache.pulsar.io.hbase.HbaseAbstractConfig.java](https://github.com/apache/pulsar/blob/master/pulsar-io/hbase/src/main/java/org/apache/pulsar/io/hbase/HbaseAbstractConfig.java)
[HDFS2](io-hdfs2-sink.md)|[org.apache.pulsar.io.hdfs2.AbstractHdfsConnector.java](https://github.com/apache/pulsar/blob/master/pulsar-io/hdfs2/src/main/java/org/apache/pulsar/io/hdfs2/AbstractHdfsConnector.java)
[HDFS3](io-hdfs3-sink.md)|[org.apache.pulsar.io.hdfs3.AbstractHdfsConnector.java](https://github.com/apache/pulsar/blob/master/pulsar-io/hdfs3/src/main/java/org/apache/pulsar/io/hdfs3/AbstractHdfsConnector.java)
[InfluxDB](io-influxdb-sink.md)|[org.apache.pulsar.io.influxdb.InfluxDBAbstractSink.java](https://github.com/apache/pulsar/blob/master/pulsar-io/influxdb/src/main/java/org/apache/pulsar/io/influxdb/InfluxDBAbstractSink.java)
[JDBC](io-jdbc-sink.md)|[org.apache.pulsar.io.jdbc.JdbcAbstractSink.java](https://github.com/apache/pulsar/blob/master/pulsar-io/jdbc/src/main/java/org/apache/pulsar/io/jdbc/JdbcAbstractSink.java)
[Kafka](io-kafka-sink.md)|[org.apache.pulsar.io.kafka.KafkaAbstractSink.java](https://github.com/apache/pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSink.java)
[Kinesis](io-kinesis-sink.md)|[org.apache.pulsar.io.kinesis.KinesisSink.java](https://github.com/apache/pulsar/blob/master/pulsar-io/kinesis/src/main/java/org/apache/pulsar/io/kinesis/KinesisSink.java)
[MongoDB](io-mongo.md)|[org.apache.pulsar.io.mongodb.MongoSink.java](https://github.com/apache/pulsar/blob/master/pulsar-io/mongo/src/main/java/org/apache/pulsar/io/mongodb/MongoSink.java)
[RabbitMQ](io-rabbitmq-sink.md)|[org.apache.pulsar.io.rabbitmq.RabbitMQSink.java](https://github.com/apache/pulsar/blob/master/pulsar-io/rabbitmq/src/main/java/org/apache/pulsar/io/rabbitmq/RabbitMQSink.java)
[Redis](io-redis.md)|[org.apache.pulsar.io.redis.RedisAbstractConfig.java](https://github.com/apache/pulsar/blob/master/pulsar-io/redis/src/main/java/org/apache/pulsar/io/redis/RedisAbstractConfig.java)
[Solr](io-solr.md)|[org.apache.pulsar.io.solr.SolrSinkConfig.java](https://github.com/apache/pulsar/blob/master/pulsar-io/solr/src/main/java/org/apache/pulsar/io/solr/SolrSinkConfig.java)




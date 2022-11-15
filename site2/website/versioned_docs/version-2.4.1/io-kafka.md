---
id: io-kafka
title: Kafka Connector
sidebar_label: "Kafka Connector"
original_id: io-kafka
---

## Source

The Kafka Source Connector is used to pull messages from Kafka topics and persist the messages
to a Pulsar topic.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| bootstrapServers | `true` | `null` | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. |
| groupId | `true` | `null` | A unique string that identifies the consumer group this consumer belongs to. |
| fetchMinBytes | `false` | `null` | Minimum bytes expected for each fetch response. |
| autoCommitEnabled | `false` | `false` | If true, periodically commit to ZooKeeper the offset of messages already fetched by the consumer. This committed offset will be used when the process fails as the position from which the new consumer will begin. | 
| autoCommitIntervalMs | `false` | `null` | The frequency in ms that the consumer offsets are committed to zookeeper. |
| heartbeatIntervalMs | `false` | `3000` | The interval between heartbeats to the consumer when using Kafka's group management facilities. |
| sessionTimeoutMs | `false` | `null` | The timeout used to detect consumer failures when using Kafka's group management facility. |
| topic | `true` | `null` | Topic name to receive records from Kafka |
|consumerConfigProperties | `false` | `null` |The consumer config properties to be passed to Consumer. Note that other properties specified in the connector config file take precedence over this config.|
| keySerializerClass | false | org.apache.kafka.common.serialization.StringSerializer | Serializer class for key that implements the org.apache.kafka.common.serialization.Serializer interface. |
| valueSerializerClass | false | org.apache.kafka.common.serialization.StringSerializer | Serializer class for value that implements the org.apache.kafka.common.serialization.Serializer interface. |

## Sink

The Kafka Sink Connector is used to pull messages from Pulsar topics and persist the messages
to a Kafka topic.

### Sink Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| bootstrapServers | `true` | `null` | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. |
| acks | `true` | `null` | The kafka producer acks mode |
| batchSize | `true` | `null` | The kafka producer batch size. |
| maxRequestSize | `true` | `null` | The maximum size of a request in bytes. |
| topic | `true` | `null` | Topic name to receive records from Kafka |
| producerConfigProperties | `false` | `null` | The producer config properties to be passed to Producer. Note that other properties specified in the connector config file take precedence over this config. |
| keySerializerClass | false | org.apache.kafka.common.serialization.StringSerializer | Serializer class for value that implements the org.apache.kafka.common.serialization.Serializer interface. |
| valueSerializerClass | false | org.apache.kafka.common.serialization.StringSerializer | Serializer class for value that implements the org.apache.kafka.common.serialization.Serializer interface. |

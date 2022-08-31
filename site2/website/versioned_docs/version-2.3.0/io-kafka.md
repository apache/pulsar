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
| fetchMinBytes | `false` | `1` | Minimum bytes expected for each fetch response. |
| autoCommitEnabled | `false` | `true` | If true, the consumer's offset will be periodically committed in the background. This committed offset will be used when the process fails as the position from which the new consumer will begin. |
| autoCommitIntervalMs | `false` | `5000` | The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if `autoCommitEnabled` is set to true. |
| heartbeatIntervalMs | `false` | `3000` | The interval between heartbeats to the consumer when using Kafka's group management facilities. |
| sessionTimeoutMs | `false` | `30000` | The timeout used to detect consumer failures when using Kafka's group management facility. |
| topic | `true` | `null` | Topic name to receive records from Kafka. |
| consumerConfigProperties | `false` | `null` | The consumer config properties to be passed to Consumer. Note that other properties specified in the connector config file take precedence over this config. |
| keyDeserializationClass | `false` | `org.apache.kafka.common.serialization.StringDeserializer` | Deserializer class for key that implements the org.apache.kafka.common.serialization.Deserializer interface. |
| valueDeserializationClass | `false` | `org.apache.kafka.common.serialization.ByteArrayDeserializer` | Deserializer class for value that implements the org.apache.kafka.common.serialization.Deserializer interface. |

## Sink

The Kafka Sink Connector is used to pull messages from Pulsar topics and persist the messages
to a Kafka topic.

### Sink Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| bootstrapServers | `true` | `null` | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. |
| acks | `true` | `null` | The kafka producer acks mode. |
| batchSize | `false` | `16384` | The kafka producer batch size. |
| maxRequestSize | `false` | `1048576` | The maximum size of a request in bytes. |
| topic | `true` | `null` | Topic name to receive records from Kafka. |
| producerConfigProperties | `false` | `null` | The producer config properties to be passed to Producer. Note that other properties specified in the connector config file take precedence over this config. |
| keySerializerClass | `false` | `org.apache.kafka.common.serialization.StringSerializer` | Serializer class for value that implements the org.apache.kafka.common.serialization.Serializer interface. |
| valueSerializerClass | `false` | `org.apache.kafka.common.serialization.ByteArraySerializer` | Serializer class for value that implements the org.apache.kafka.common.serialization.Serializer interface. |

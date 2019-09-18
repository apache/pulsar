
---
id: io-kafka-sink
title: Kafka sink connector
sidebar_label: Kafka sink connector
---

The Kafka sink connector pulls messages from Pulsar topics and persists the messages
to Kafka topics.

This guide explains how to configure and use the Kafka sink connector.

## Configuration

The configuration of the Kafka sink connector has the following parameters.

| Name | Type| Required | Default | Description 
|------|----------|---------|-------------|-------------|
|  `bootstrapServers` |String| true | " " (empty string) | A comma-separated list of host and port pairs for establishing the initial connection to the Kafka cluster. |
|`ack`|String|true|" " (empty string) |The number of acknowledgments that the producer requires the leader to receive before a request completes. <br/>This controls the durability of the sent records.
|`batchsize`|
|`maxRequestSize`|
|`topic`|
| `keyDeserializationClass` | String|false | org.apache.kafka.common.serialization.StringDeserializer | The deserializer class for Kafka consumers to deserialize keys.
| `valueDeserializationClass` | String|false | org.apache.kafka.common.serialization.ByteArrayDeserializer | The deserializer class for Kafka consumers to deserialize values.
|`producerConfigProperties`






### Example

Before using the Kafka source connector, you need to create a configuration file through one of the following methods.

* JSON 
  
"bootstrapServers": "localhost:6667"
"topic": "test"
"acks": "1"
"batchSize": "16384"
"maxRequestSize": "1048576"
"producerConfigProperties":
    "client.id": "test-pulsar-producer"
    "security.protocol": "SASL_PLAINTEXT"
    "sasl.mechanism": "GSSAPI"
    "sasl.kerberos.service.name": "kafka"
    "acks": "all"
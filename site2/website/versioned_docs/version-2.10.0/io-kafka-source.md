---
id: io-kafka-source
title: Kafka source connector
sidebar_label: "Kafka source connector"
original_id: io-kafka-source
---

The Kafka source connector pulls messages from Kafka topics and persists the messages
to Pulsar topics.

This guide explains how to configure and use the Kafka source connector.

## Configuration

The configuration of the Kafka source connector has the following properties.

### Property

| Name | Type| Required | Default | Description 
|------|----------|---------|-------------|-------------|
|  `bootstrapServers` |String| true | " " (empty string) | A comma-separated list of host and port pairs for establishing the initial connection to the Kafka cluster. |
| `groupId` |String| true | " " (empty string) | A unique string that identifies the group of consumer processes to which this consumer belongs. |
| `fetchMinBytes` | long|false | 1 | The minimum byte expected for each fetch response. |
| `autoCommitEnabled` | boolean |false | true | If set to true, the consumer's offset is periodically committed in the background.<br /><br /> This committed offset is used when the process fails as the position from which a new consumer begins. |
| `autoCommitIntervalMs` | long|false | 5000 | The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if `autoCommitEnabled` is set to true. |
| `heartbeatIntervalMs` | long| false | 3000 | The interval between heartbeats to the consumer when using Kafka's group management facilities. <br /><br />**Note: `heartbeatIntervalMs` must be smaller than `sessionTimeoutMs`**.|
| `sessionTimeoutMs` | long|false | 30000 | The timeout used to detect consumer failures when using Kafka's group management facility. |
| `topic` | String|true | " " (empty string)| The Kafka topic that sends messages to Pulsar. |
|  `consumerConfigProperties` | Map| false | " " (empty string) | The consumer configuration properties to be passed to consumers. <br /><br />**Note: other properties specified in the connector configuration file take precedence over this configuration**. |
| `keyDeserializationClass` | String|false | org.apache.kafka.common.serialization.StringDeserializer | The deserializer class for Kafka consumers to deserialize keys.<br /> The deserializer is set by a specific implementation of [`KafkaAbstractSource`](https://github.com/apache/pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSource.java).
| `valueDeserializationClass` | String|false | org.apache.kafka.common.serialization.ByteArrayDeserializer | The deserializer class for Kafka consumers to deserialize values.
| `autoOffsetReset` | String | false | earliest | The default offset reset policy. |

### Schema Management

This Kafka source connector applies the schema to the topic depending on the data type that is present on the Kafka topic.
You can detect the data type from the `keyDeserializationClass` and `valueDeserializationClass` configuration parameters.

If the `valueDeserializationClass` is `org.apache.kafka.common.serialization.StringDeserializer`, you can set Schema.STRING() as schema type on the Pulsar topic.

If `valueDeserializationClass` is `io.confluent.kafka.serializers.KafkaAvroDeserializer`, Pulsar downloads the AVRO schema from the Confluent Schema Registry®
and sets it properly on the Pulsar topic.

In this case, you need to set `schema.registry.url` inside of the `consumerConfigProperties` configuration entry
of the source.

If `keyDeserializationClass` is not `org.apache.kafka.common.serialization.StringDeserializer`, it means 
that you do not have a String as key and the Kafka Source uses the KeyValue schema type with the SEPARATED encoding.

Pulsar supports AVRO format for keys.

In this case, you can have a Pulsar topic with the following properties:
- Schema: KeyValue schema with SEPARATED encoding
- Key: the content of key of the Kafka message (base64 encoded)
- Value: the content of value of the Kafka message
- KeySchema: the schema detected from `keyDeserializationClass`
- ValueSchema: the schema detected from `valueDeserializationClass`

Topic compaction and partition routing use the Pulsar key, that contains the Kafka key, and so they are driven by the same value that you have on Kafka.

When you consume data from Pulsar topics, you can use the `KeyValue` schema. In this way, you can decode the data properly.
If you want to access the raw key, you can use the `Message#getKeyBytes()` API.

### Example

Before using the Kafka source connector, you need to create a configuration file through one of the following methods.

- JSON

   ```json
   
     {
       "bootstrapServers": "pulsar-kafka:9092",
       "groupId": "test-pulsar-io",
       "topic": "my-topic",
       "sessionTimeoutMs": "10000",
       "autoCommitEnabled": false
     }
   
   ```

- YAML

   ```yaml
   
     configs:
       bootstrapServers: "pulsar-kafka:9092"
       groupId: "test-pulsar-io"
       topic: "my-topic"
       sessionTimeoutMs: "10000"
         autoCommitEnabled: false
   
   ```

## Usage

You can make the Kafka source connector as a Pulsar built-in connector and use it on a standalone cluster or an on-premises cluster.

### Standalone cluster

This example describes how to use the Kafka source connector to feed data from Kafka and write data to Pulsar topics in the standalone mode.

#### Prerequisites

- Install [Docker](https://docs.docker.com/get-docker/)(Community Edition). 

#### Steps

1. Download and start the Confluent Platform.

For details, see the [documentation](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) to install the Kafka service locally.

2. Pull a Pulsar image and start Pulsar in standalone mode.

   ```bash
   
   docker pull apachepulsar/pulsar:latest
     
   docker run -d -it -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --name pulsar-kafka-standalone apachepulsar/pulsar:latest bin/pulsar standalone
   
   ```

3. Create a producer file _kafka-producer.py_.

   ```python
   
   from kafka import KafkaProducer
   producer = KafkaProducer(bootstrap_servers='localhost:9092')
   future = producer.send('my-topic', b'hello world')
   future.get()
   
   ```

4. Create a consumer file _pulsar-client.py_.

   ```python
   
   import pulsar

   client = pulsar.Client('pulsar://localhost:6650')
   consumer = client.subscribe('my-topic', subscription_name='my-aa')

   while True:
       msg = consumer.receive()
       print msg
       print dir(msg)
       print("Received message: '%s'" % msg.data())
       consumer.acknowledge(msg)

   client.close()
   
   ```

5. Copy the following files to Pulsar.

   ```bash
   
   docker cp pulsar-io-kafka.nar pulsar-kafka-standalone:/pulsar
   docker cp kafkaSourceConfig.yaml pulsar-kafka-standalone:/pulsar/conf
   
   ```

6. Open a new terminal window and start the Kafka source connector in local run mode.

   ```bash
   
   docker exec -it pulsar-kafka-standalone /bin/bash

   ./bin/pulsar-admin source localrun \
   --archive ./pulsar-io-kafka.nar \
   --tenant public \
   --namespace default \
   --name kafka \
   --destination-topic-name my-topic \
   --source-config-file ./conf/kafkaSourceConfig.yaml \
   --parallelism 1
   
   ```

7. Open a new terminal window and run the Kafka producer locally.

   ```bash
   
   python3 kafka-producer.py
   
   ```

8. Open a new terminal window and run the Pulsar consumer locally.

   ```bash
   
   python3 pulsar-client.py
   
   ```

The following information appears on the consumer terminal window.

    ```bash
    
    Received message: 'hello world'
    
    ```

### On-premises cluster

This example explains how to create a Kafka source connector in an on-premises cluster.

1. Copy the NAR package of the Kafka connector to the Pulsar connectors directory.

   ```
   
   cp pulsar-io-kafka-{{connector:version}}.nar $PULSAR_HOME/connectors/pulsar-io-kafka-{{connector:version}}.nar
   
   ```

2. Reload all [built-in connectors](io-connectors.md).

   ```
   
   PULSAR_HOME/bin/pulsar-admin sources reload
   
   ```

3. Check whether the Kafka source connector is available on the list or not.

   ```
   
   PULSAR_HOME/bin/pulsar-admin sources available-sources
   
   ```

4. Create a Kafka source connector on a Pulsar cluster using the [`pulsar-admin sources create`](/tools/pulsar-admin/) command.

   ```
   
   PULSAR_HOME/bin/pulsar-admin sources create \
   --source-config-file <kafka-source-config.yaml>
   
   ```


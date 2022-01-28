---
id: version-2.6.3-adaptors-storm
title: Pulsar adaptor for Apache Storm
sidebar_label: Apache Storm
original_id: adaptors-storm
---

Pulsar Storm is an adaptor for integrating with [Apache Storm](http://storm.apache.org/) topologies. It provides core Storm implementations for sending and receiving data.

An application can inject data into a Storm topology via a generic Pulsar spout, as well as consume data from a Storm topology via a generic Pulsar bolt.

## Using the Pulsar Storm Adaptor

Include dependency for Pulsar Storm Adaptor:

```xml
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-storm</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

## Pulsar Spout

The Pulsar Spout allows for the data published on a topic to be consumed by a Storm topology. It emits a Storm tuple based on the message received and the `MessageToValuesMapper` provided by the client.

The tuples that fail to be processed by the downstream bolts will be re-injected by the spout with an exponential backoff, within a configurable timeout (the default is 60 seconds) or a configurable number of retries, whichever comes first, after which it is acknowledged by the consumer. Here's an example construction of a spout:

```java
MessageToValuesMapper messageToValuesMapper = new MessageToValuesMapper() {

    @Override
    public Values toValues(Message msg) {
        return new Values(new String(msg.getData()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // declare the output fields
        declarer.declare(new Fields("string"));
    }
};

// Configure a Pulsar Spout
PulsarSpoutConfiguration spoutConf = new PulsarSpoutConfiguration();
spoutConf.setServiceUrl("pulsar://broker.messaging.usw.example.com:6650");
spoutConf.setTopic("persistent://my-property/usw/my-ns/my-topic1");
spoutConf.setSubscriptionName("my-subscriber-name1");
spoutConf.setMessageToValuesMapper(messageToValuesMapper);

// Create a Pulsar Spout
PulsarSpout spout = new PulsarSpout(spoutConf);
```

For a complete example, click [here](https://github.com/apache/pulsar-adapters/blob/master/pulsar-storm/src/test/java/org/apache/pulsar/storm/PulsarSpoutTest.java).

## Pulsar Bolt

The Pulsar bolt allows data in a Storm topology to be published on a topic. It publishes messages based on the Storm tuple received and the `TupleToMessageMapper` provided by the client.

A partitioned topic can also be used to publish messages on different topics. In the implementation of the `TupleToMessageMapper`, a "key" will need to be provided in the message which will send the messages with the same key to the same topic. Here's an example bolt:

```java
TupleToMessageMapper tupleToMessageMapper = new TupleToMessageMapper() {

    @Override
    public TypedMessageBuilder<byte[]> toMessage(TypedMessageBuilder<byte[]> msgBuilder, Tuple tuple) {
        String receivedMessage = tuple.getString(0);
        // message processing
        String processedMsg = receivedMessage + "-processed";
        return msgBuilder.value(processedMsg.getBytes());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // declare the output fields
    }
};

// Configure a Pulsar Bolt
PulsarBoltConfiguration boltConf = new PulsarBoltConfiguration();
boltConf.setServiceUrl("pulsar://broker.messaging.usw.example.com:6650");
boltConf.setTopic("persistent://my-property/usw/my-ns/my-topic2");
boltConf.setTupleToMessageMapper(tupleToMessageMapper);

// Create a Pulsar Bolt
PulsarBolt bolt = new PulsarBolt(boltConf);
```

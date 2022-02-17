---
id: version-2.2.0-adaptors-storm
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
// Configure a Pulsar Client
ClientConfiguration clientConf = new ClientConfiguration();

// Configure a Pulsar Consumer
ConsumerConfiguration consumerConf = new ConsumerConfiguration();  

@SuppressWarnings("serial")
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
PulsarSpout spout = new PulsarSpout(spoutConf, clientConf, consumerConf);
```

## Pulsar Bolt

The Pulsar bolt allows data in a Storm topology to be published on a topic. It publishes messages based on the Storm tuple received and the `TupleToMessageMapper` provided by the client.

A partitioned topic can also be used to publish messages on different topics. In the implementation of the `TupleToMessageMapper`, a "key" will need to be provided in the message which will send the messages with the same key to the same topic. Here's an example bolt:

```java
// Configure a Pulsar Client
ClientConfiguration clientConf = new ClientConfiguration();

// Configure a Pulsar Producer  
ProducerConfiguration producerConf = new ProducerConfiguration();

@SuppressWarnings("serial")
TupleToMessageMapper tupleToMessageMapper = new TupleToMessageMapper() {

    @Override
    public Message toMessage(Tuple tuple) {
        String receivedMessage = tuple.getString(0);
        // message processing
        String processedMsg = receivedMessage + "-processed";
        return MessageBuilder.create().setContent(processedMsg.getBytes()).build();
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
PulsarBolt bolt = new PulsarBolt(boltConf, clientConf);
```

## Example

You can find a complete example [here](https://github.com/apache/pulsar/tree/master/pulsar-storm/src/test/java/org/apache/pulsar/storm/example/StormExample.java).

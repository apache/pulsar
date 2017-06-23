# Spark Streaming Pulsar Receiver

<!-- TOC depthFrom:2 depthTo:3 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Introduction](#introduction)
- [Using Spark Streaming Pulsar Receiver](#using-spark-streaming-pulsar-receiver)
- [Example](#example)

<!-- /TOC -->

## Introduction
Spark Streaming Pulsar Receiver is a custom receiver which enables Apache Spark Streaming to receive data from Pulsar.

An application can receive RDD format data via Spark Streaming Pulsar Receiver and can process it variously.

## Using Spark Streaming Pulsar Receiver
Include dependency for Spark Streaming Pulsar Receiver:

```xml
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-spark</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

Pass an instance of SparkStreamingPulsarReceiver to receiverStream method in JavaStreamingContext:
```java
SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pulsar-spark");
JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

ClientConfiguration clientConf = new ClientConfiguration();
ConsumerConfiguration consConf = new ConsumerConfiguration();
String url = "pulsar://localhost:6650/";
String topic = "persistent://sample/standalone/ns1/topic1";
String subs = "sub1";

JavaReceiverInputDStream<byte[]> msgs = jssc
        .receiverStream(new SparkStreamingPulsarReceiver(clientConf, consConf, url, topic, subs));
```


## Example
You can find a complete example [here](../pulsar-spark/src/test/java/org/apache/pulsar/spark/example/SparkStreamingPulsarReceiverExample.java).
In this example, the number of messages which contain "Pulsar" string in received messages is counted.

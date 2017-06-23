---
title: Spark Streaming Pulsar receiver
tags: [spark, streaming, java]
---

The Spark Streaming Pulsar Receiver is a custom receiver that enables Apache [Spark Streaming](https://spark.apache.org/streaming/) to receive data from Pulsar.

An application can receive data in [Resilient Distributed Dataset](https://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds) (RDD) format via the Spark Streaming Pulsar Receiver and can process it in a variety of ways.

## Prerequisites

To use the receiver, include a dependency for the `pulsar-spark` library in your Java configuration.

### Maven

If you're using Maven, add this to your `pom.xml`:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{ site.current_version }}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>com.yahoo.pulsar</groupId>
  <artifactId>pulsar-spark</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Gradle

If you're using Gradle, add this to your `build.gradle` file:

```groovy
def pulsarVersion = "{{ site.current_version }}"

dependencies {
    compile group: 'com.yahoo.pulsar', name: 'pulsar-spark', version: pulsarVersion
}
```

## Usage

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

You can find a complete example [here]({{ site.pulsar_repo }}/tree/master/pulsar-spark/src/test/java/com/yahoo/pulsar/spark/example/SparkStreamingPulsarReceiverExample.java).
In this example, the number of messages which contain "Pulsar" string in received messages is counted.

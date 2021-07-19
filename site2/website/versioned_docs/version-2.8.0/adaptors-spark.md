---
id: version-2.8.0-adaptors-spark
title: Pulsar adaptor for Apache Spark
sidebar_label: Apache Spark
original_id: adaptors-spark
---

## Spark structured streaming connector
Pulsar Spark Connector is an integration of Apache Pulsar and Apache Spark (data processing engine), which allows Spark reading data from Pulsar and writing data to Pulsar using Spark structured streaming and Spark SQL and provides exactly-once source semantics and at-least-once sink semantics. For details, refer to [Pulsar Spark Connector in StreamNative Hub](https://hub.streamnative.io/).

## Spark streaming connector
The Spark Streaming receiver for Pulsar is a custom receiver that enables Apache [Spark Streaming](https://spark.apache.org/streaming/) to receive data from Pulsar.

An application can receive data in [Resilient Distributed Dataset](https://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds) (RDD) format via the Spark Streaming Pulsar receiver and can process it in a variety of ways.

### Prerequisites

To use the receiver, include a dependency for the `pulsar-spark` library in your Java configuration.

#### Maven

If you're using Maven, add this to your `pom.xml`:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{pulsar:version}}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-spark</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

#### Gradle

If you're using Gradle, add this to your `build.gradle` file:

```groovy
def pulsarVersion = "{{pulsar:version}}"

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-spark', version: pulsarVersion
}
```

### Usage

Pass an instance of `SparkStreamingPulsarReceiver` to the `receiverStream` method in `JavaStreamingContext`:

```java
    String serviceUrl = "pulsar://localhost:6650/";
    String topic = "persistent://public/default/test_src";
    String subs = "test_sub";

    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Pulsar Spark Example");

    JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(60));

    ConsumerConfigurationData<byte[]> pulsarConf = new ConsumerConfigurationData();

    Set<String> set = new HashSet<>();
    set.add(topic);
    pulsarConf.setTopicNames(set);
    pulsarConf.setSubscriptionName(subs);

    SparkStreamingPulsarReceiver pulsarReceiver = new SparkStreamingPulsarReceiver(
        serviceUrl,
        pulsarConf,
        new AuthenticationDisabled());

    JavaReceiverInputDStream<byte[]> lineDStream = jsc.receiverStream(pulsarReceiver);
```

For a complete example, click [here](https://github.com/apache/pulsar-adapters/blob/master/examples/spark/src/main/java/org/apache/spark/streaming/receiver/example/SparkStreamingPulsarReceiverExample.java). In this example, the number of messages that contain the string "Pulsar" in received messages is counted.

Note that if needed, other Pulsar authentication classes can be used. For example, in order to use a token during authentication the following parameters for the `SparkStreamingPulsarReceiver` constructor can be set:
```java
SparkStreamingPulsarReceiver pulsarReceiver = new SparkStreamingPulsarReceiver(
        serviceUrl,
        pulsarConf,
        new AuthenticationToken("token:<secret-JWT-token>"));
```
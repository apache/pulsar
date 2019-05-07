---
title: Spark Streaming Pulsar receiver
tags: [spark, streaming, java]
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

The Spark Streaming receiver for Pulsar is a custom receiver that enables Apache [Spark Streaming](https://spark.apache.org/streaming/) to receive data from Pulsar.

An application can receive data in [Resilient Distributed Dataset](https://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds) (RDD) format via the Spark Streaming Pulsar receiver and can process it in a variety of ways.

## Prerequisites

To use the receiver, include a dependency for the `pulsar-spark` library in your Java configuration.

### Maven

If you're using Maven, add this to your `pom.xml`:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{ site.current_version }}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-spark</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Gradle

If you're using Gradle, add this to your `build.gradle` file:

```groovy
def pulsarVersion = "{{ site.current_version }}"

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-spark', version: pulsarVersion
}
```

## Usage

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


## Example

You can find a complete example [here]({{ site.pulsar_repo }}/examples/spark/src/main/java/org/apache/spark/streaming/receiver/example/SparkStreamingPulsarReceiverExample.java).
In this example, the number of messages which contain the string "Pulsar" in received messages is counted.

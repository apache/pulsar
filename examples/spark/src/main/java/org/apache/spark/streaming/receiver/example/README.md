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

## Apache Spark Streaming Receiver for Pulsar

This page describes how to use the receiver to read Pulsar topics with [Apache Spark](https://spark.apache.org/) stream processing applications.

## Example

### PulsarSparkReceiverWordCount

This spark streaming job is consuming from a Pulsar topic and counting the wordcount in a streaming fashion. The job can write the word count results
to stdout or another Pulsar topic.

If you choose local to run, modify PulsarSparkReceiverWordCount.java main code example :
```java
public static void main(String[] args) throws InterruptedException {
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
    JavaPairDStream<String, Integer> result = lineDStream.flatMap(x -> {
      String line = new String(x, Charset.forName("UTF-8"));
      List<String> list = Arrays.asList(line.split(" "));
      return list.iterator();
    })
        .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
        .reduceByKey((x, y) -> x + y);

    result.print();

    jsc.start();
    jsc.awaitTermination();
}
```

If you choose spark_submit to run, the steps to run the example:

1. Start Pulsar Standalone.

    You can follow the [instructions](https://pulsar.apache.org/docs/en/standalone/) to start a Pulsar standalone locally.

    ```shell
    $ bin/pulsar standalone
    ```
    
2. Build the examples.

    ```shell
    $ cd ${PULSAR_HOME}
    $ mvn clean install -DskipTests
    ```

3. Spark Run the word count example to print results to stdout.

    ```shell
    $ ${SPARK_HOME}/bin/spark-submit --class org.apache.spark.streaming.receiver.example.SparkStreamingPulsarReceiverExample \
                                     --master local[2] \ 
                                     --packages org.apache.pulsar:pulsar-client:${project.version},org.apache.pulsar:pulsar-spark:${project.version} \
                                     ${PULSAR_HOME}/examples/spark/target/pulsar-spark-examples.jar \ 
                                     pulsar://localhost:6650 test_src test_sub
    ```       

4. When you run pulsar Producer data like ProducerSparkReceiverData, You will see similar to print results to stdout, e.g.:

    ```shell
    (streaming,100)
    (producer,100)
    (spark,100)
    (msg,100)
    ```
    
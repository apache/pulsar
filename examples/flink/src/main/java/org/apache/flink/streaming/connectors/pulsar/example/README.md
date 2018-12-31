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

## Apache Flink Connectors for Pulsar

This page describes how to use the connectors to read and write Pulsar topics with [Apache Flink](https://flink.apache.org/) stream processing applications.

Build end-to-end stream processing pipelines that use Pulsar as the stream storage and message bus, and Apache Flink for computation over the streams.
See the [Pulsar Concepts](https://pulsar.apache.org/docs/en/concepts-overview/) page for more information.

## Example

### PulsarConsumerSourceWordCount

This Flink streaming job is consuming from a Pulsar topic and counting the wordcount in a streaming fashion. The job can write the word count results
to stdout or another Pulsar topic.

The steps to run the example:

1. Start Pulsar Standalone.

    You can follow the [instructions](https://pulsar.apache.org/docs/en/standalone/) to start a Pulsar standalone locally.

    ```shell
    $ bin/pulsar standalone
    ```

2. Start Flink locally.

    You can follow the [instructions](https://ci.apache.org/projects/flink/flink-docs-release-1.6/quickstart/setup_quickstart.html) to download and start Flink.

    ```shell
    $ ./bin/start-cluster.sh
    ```

3. Build the examples.

    ```shell
    $ cd ${PULSAR_HOME}
    $ mvn clean install -DskipTests
    ```

4. Run the word count example to print results to stdout.

    ```shell
    $ ./bin/flink run -c org.apache.flink.streaming.connectors.pulsar.example.PulsarConsumerSourceWordCount ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --input-topic test_src --subscription test_sub
    ```

5. Produce messages to topic `test_src`.

    ```shell
    $ bin/pulsar-client produce -m "hello world test again" -n 100 test_src
    ```

6. You can check the flink taskexecutor `.out` file. The `.out` file will print the counts at the end of each time window as long as words are floating in, e.g.:

    ```shell
    PulsarConsumerSourceWordCount.WordWithCount(word=hello, count=100)
    PulsarConsumerSourceWordCount.WordWithCount(word=again, count=100)
    PulsarConsumerSourceWordCount.WordWithCount(word=test, count=100)
    PulsarConsumerSourceWordCount.WordWithCount(word=world, count=100)  
    ```

Alternatively, when you run the flink word count example at step 4, you can choose dump the result to another pulsar topic.

```shell
$ ./bin/flink run -c org.apache.flink.streaming.connectors.pulsar.example.PulsarConsumerSourceWordCount ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --input-topic test_src --subscription test_sub --output-topic test_dest
```

Once the flink word count example is running, you can use `bin/pulsar-client` to tail the results produced into topic `test_dest`.

```shell
$ bin/pulsar-client consume -n 0 -s test test_dest
```

You will see similar results as what you see at step 6 when running the word count example to print results to stdout.


### PulsarConsumerSourceWordCountToAvroTableSink

This Flink streaming job is consuming from a Pulsar topic and counting the wordcount in a streaming fashion. The job can write the word count results
to csv file or another Pulsar topic for avro format.

The steps to run the example:

Step 1, 2 and 3 are same as above.

4. Run the word count example to print results to stdout.

    ```shell
    $ ./bin/flink run -c org.apache.flink.streaming.connectors.pulsar.example.PulsarConsumerSourceWordCountToAvroTableSink ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --input-topic test_src --subscription test_sub
    ```

5. Produce messages to topic `test_src`.

    ```shell
    $ bin/pulsar-client produce -m "hello world again" -n 100 test_src
    ```

6. You can check the ${FLINK_HOME}/examples/file. The file contains the counts at the end of each time window as long as words are floating in, e.g.:

    ```file
    hello|100
    again|100
    test|100
    world|100
    ```

Alternatively, when you run the flink word count example at step 4, you can choose dump the result to another pulsar topic.

```shell
$ ./bin/flink run -c org.apache.flink.streaming.connectors.pulsar.example.PulsarConsumerSourceWordCountToAvroTableSink ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --input-topic test_src --subscription test_sub --output-topic test_dest
```

Once the flink word count example is running, you can use `bin/pulsar-client` to tail the results produced into topic `test_dest`.

```shell
$ bin/pulsar-client consume -n 0 -s test test_dest
```

You will see sample output for above linked application as follows:.
```
----- got message -----

hello�
----- got message -----

again�
----- got message -----
test�
----- got message -----

world�

```

### PulsarConsumerSourceWordCountToJsonTableSink

This Flink streaming job is consuming from a Pulsar topic and counting the wordcount in a streaming fashion. The job can write the word count results
to csv file or another Pulsar topic for json format.

The steps to run the example:

Step 1, 2 and 3 are same as above.

4. Run the word count example to print results to stdout.

    ```shell
    $ ./bin/flink run -c org.apache.flink.streaming.connectors.pulsar.example.PulsarConsumerSourceWordCountToJsonTableSink ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --input-topic test_src --subscription test_sub
    ```

If java.lang.ClassNotFoundException: org.apache.flink.table.sinks.TableSink and java.lang.NoClassDefFoundError: org/apache/flink/formats/json/JsonRowSerializationSchema, you need build Apache Flink from source, then copy flink-table_{version}.jar, flink-json_{version}.jar to ${FLINK_HOME}/lib and restart flink cluster. 

5. Produce messages to topic `test_src`.

    ```shell
    $ bin/pulsar-client produce -m "hello world again" -n 100 test_src
    ```

6. You can check the ${FLINK_HOME}/examples/file. The file contains the counts at the end of each time window as long as words are floating in, e.g.:

    ```file
    hello|100
    again|100
    test|100
    world|100
    ```

Alternatively, when you run the flink word count example at step 4, you can choose dump the result to another pulsar topic.

```shell
$ ./bin/flink run -c org.apache.flink.streaming.connectors.pulsar.example.PulsarConsumerSourceWordCountToJsonTableSink ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --input-topic test_src --subscription test_sub --output-topic test_dest
```

Once the flink word count example is running, you can use `bin/pulsar-client` to tail the results produced into topic `test_dest`.

```shell
$ bin/pulsar-client consume -n 0 -s test test_dest
```

You will see sample output for above linked application as follows:.
```
----- got message -----
{"word":"hello","count":100}
----- got message -----
{"word":"again","count":100}
----- got message -----
{"word":"test","count":100}
----- got message -----
{"word":"world","count":100}
```

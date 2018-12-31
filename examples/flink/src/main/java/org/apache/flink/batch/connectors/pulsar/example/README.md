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

The Flink Batch Sink for Pulsar is a custom sink that enables Apache [Flink](https://flink.apache.org/) to write [DataSet](https://ci.apache.org/projects/flink/flink-docs-stable/dev/batch/index.html) to Pulsar.

# Prerequisites

To use this sink, include a dependency for the `pulsar-flink` library in your Java configuration.

# Maven

If you're using Maven, add this to your `pom.xml`:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{pulsar:version}}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-flink</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

# Gradle

If you're using Gradle, add this to your `build.gradle` file:

```groovy
def pulsarVersion = "{{pulsar:version}}"

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-flink', version: pulsarVersion
}
```

# Example

### PulsarOutputFormat

In this example, Flink DataSet is processed as word-count and being written to Pulsar. Please find a complete example for PulsarOutputFormat as follows:
[java](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchSinkExample.java)
[scala](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/scala/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchSinkScalaExample.scala)

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
    # java
    $ ./bin/flink run -c org.apache.flink.batch.connectors.pulsar.example.FlinkPulsarBatchSinkExample ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --topic test_flink_topic

    # scala
    $ ./bin/flink run -c org.apache.flink.batch.connectors.pulsar.example.FlinkPulsarBatchSinkScalaExample ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --topic test_flink_topic
    ```

5. Once the flink word count example is running, you can use `bin/pulsar-client` to tail the results produced into topic `test_flink_topic`.

```shell
$ bin/pulsar-client consume -n 0 -s test test_flink_topic
```

6. Please find sample output for above linked application as follows:
```
WordWithCount { word = important, count = 1 }
WordWithCount { word = encircles, count = 1 }
WordWithCount { word = imagination, count = 2 }
WordWithCount { word = knowledge, count = 2 }
WordWithCount { word = limited, count = 1 }
WordWithCount { word = world, count = 1 }
```


### PulsarCsvOutputFormat

In this example, Flink DataSet is processed and written to Pulsar in Csv format. Please find a complete example for PulsarCsvOutputFormat as follows:
[java](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchCsvSinkExample.java)
[scala](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/scala/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchCsvSinkScalaExample.scala)

The steps to run the example:

Step 1, 2 and 3 are same as above.

4. Run the word count example to print results to stdout.

    ```shell
    # java
    $ ./bin/flink run -c org.apache.flink.batch.connectors.pulsar.example.FlinkPulsarBatchCsvSinkExample ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --topic test_flink_topic

    # scala
    $ ./bin/flink run -c org.apache.flink.batch.connectors.pulsar.example.FlinkPulsarBatchCsvSinkScalaExample ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --topic test_flink_topic
    ```

5. Once the flink word count example is running, you can use `bin/pulsar-client` to tail the results produced into topic `test_flink_topic`.

```shell
$ bin/pulsar-client consume -n 0 -s test test_flink_topic
```

6. Please find sample output for above linked application as follows:
```
4,SKYLAB,1973,1974
5,APOLLO–SOYUZ TEST PROJECT,1975,1975
```


### PulsarJsonOutputFormat

In this example, Flink DataSet is processed and written to Pulsar in Json format. Please find a complete example for PulsarJsonOutputFormat as follows:
[java](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchJsonSinkExample.java)
[scala](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/scala/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchJsonSinkScalaExample.scala)

**Note:** Property definitions of the model should be public or have getter functions to be visible.

The steps to run the example:

Step 1, 2 and 3 are same as above.

4. Run the word count example to print results to stdout.

    ```shell
    # java
    $ ./bin/flink run -c org.apache.flink.batch.connectors.pulsar.example.FlinkPulsarBatchJsonSinkExample ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --topic test_flink_topic

    # scala
    $ ./bin/flink run -c org.apache.flink.batch.connectors.pulsar.example.FlinkPulsarBatchJsonSinkScalaExample ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --topic test_flink_topic
    ```

5. Once the flink word count example is running, you can use `bin/pulsar-client` to tail the results produced into topic `test_flink_topic`.

```shell
$ bin/pulsar-client consume -n 0 -s test test_flink_topic
```

6. Please find sample output for above linked application as follows:
```
{"id":4,"missionName":"SKYLAB","startYear":1973,"endYear":1974}
{"id":5,"missionName":"APOLLO–SOYUZ TEST PROJECT","startYear":1975,"endYear":1975}
```


### PulsarAvroOutputFormat

In this example, Flink DataSet is processed and written to Pulsar in Json format. Please find a complete example for PulsarAvroOutputFormat as follows:
[java](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchAvroSinkExample.java)
[scala](https://github.com/apache/pulsar/tree/master/examples/flink/src/main/scala/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchAvroSinkScalaExample.scala)

**Note:** NasaMission class are automatically generated by Avro.

The steps to run the example:

Step 1, 2 and 3 are same as above.

4. Run the word count example to print results to stdout.

    ```shell
    # java
    $ ./bin/flink run -c org.apache.flink.batch.connectors.pulsar.example.FlinkPulsarBatchAvroSinkExample ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --topic test_flink_topic

    # scala
    $ ./bin/flink run -c org.apache.flink.batch.connectors.pulsar.example.FlinkPulsarBatchAvroSinkScalaExample ${PULSAR_HOME}/examples/flink/target/pulsar-flink-examples.jar --service-url pulsar://localhost:6650 --topic test_flink_topic
    ```

5. Once the flink word count example is running, you can use `bin/pulsar-client` to tail the results produced into topic `test_flink_topic`.

```shell
$ bin/pulsar-client consume -n 0 -s test test_flink_topic
```

6. Please find sample output for above linked application as follows:
```
 ----- got message -----
 
 Skylab��
 ----- got message -----
 
 6Apollo–Soyuz Test Project��
```
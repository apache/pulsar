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
This document explains how to develop Scala Applications by using Flink Batch Sink.
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

# PulsarOutputFormat
### Usage

Please find Scala sample usage of `PulsarOutputFormat` as follows:

```scala
      /**
        * Data type for words with count.
        */
      case class WordWithCount(word: String, count: Long) {
        override def toString: String = "WordWithCount { word = " + word + ", count = " + count + " }"
      }

      /**
        * Implementation
        */
      private val EINSTEIN_QUOTE = "Imagination is more important than knowledge. " +
        "Knowledge is limited. Imagination encircles the world."
      private val SERVICE_URL = "pulsar://127.0.0.1:6650"
      private val TOPIC_NAME = "my-flink-topic"

      def main(args: Array[String]): Unit = {

        // set up the execution environment
        val env = ExecutionEnvironment.getExecutionEnvironment

        // create PulsarOutputFormat instance
        val pulsarOutputFormat =
          new PulsarOutputFormat[WordWithCount](SERVICE_URL, TOPIC_NAME, new SerializationSchema[WordWithCount] {
            override def serialize(wordWithCount: WordWithCount): Array[Byte] = wordWithCount.toString.getBytes
          })

        // create DataSet
        val textDS = env.fromElements[String](EINSTEIN_QUOTE)

        // convert sentence to words
        textDS.flatMap((value: String, out: Collector[WordWithCount]) => {
          val words = value.toLowerCase.split(" ")
          for (word <- words) {
            out.collect(new WordWithCount(word.replace(".", ""), 1))
          }
        })

        // filter words which length is bigger than 4
        .filter((wordWithCount: WordWithCount) => wordWithCount.word.length > 4)

        // group the words
        .groupBy((wordWithCount: WordWithCount) => wordWithCount.word)

        // sum the word counts
        .reduce((wordWithCount1: WordWithCount, wordWithCount2: WordWithCount) =>
          new WordWithCount(wordWithCount1.word, wordWithCount1.count + wordWithCount2.count))

        // write batch data to Pulsar
        .output(pulsarOutputFormat)

        // set parallelism to write Pulsar in parallel (optional)
        env.setParallelism(2)

        // execute program
        env.execute("Flink - Pulsar Batch WordCount")
      }
```

### Sample Output

Please find sample output for above application as follows:
```
WordWithCount { word = encircles, count = 1 }
WordWithCount { word = important, count = 1 }
WordWithCount { word = imagination, count = 2 }
WordWithCount { word = limited, count = 1 }
WordWithCount { word = knowledge, count = 2 }
WordWithCount { word = world, count = 1 }
```

### Complete Example

You can find a complete example [here](https://github.com/apache/pulsar/tree/master/examples/flink-consumer-source/src/main/scala/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchSinkScalaExample.scala).
In this example, Flink DataSet is processed as word-count and being written to Pulsar.


# PulsarCsvOutputFormat
### Usage

Please find Scala sample usage of `PulsarCsvOutputFormat` as follows:

```scala
      /**
        * NasaMission Model
        */
      private case class NasaMission(id: Int, missionName: String, startYear: Int, endYear: Int)
        extends Tuple4(id, missionName, startYear, endYear)

      /**
        * Implementation
        */
      private val SERVICE_URL = "pulsar://127.0.0.1:6650"
      private val TOPIC_NAME = "my-flink-topic"

      private val nasaMissions = List(
        NasaMission(1, "Mercury program", 1959, 1963),
        NasaMission(2, "Apollo program", 1961, 1972),
        NasaMission(3, "Gemini program", 1963, 1966),
        NasaMission(4, "Skylab", 1973, 1974),
        NasaMission(5, "Apollo–Soyuz Test Project", 1975, 1975))

      def main(args: Array[String]): Unit = {

        // set up the execution environment
        val env = ExecutionEnvironment.getExecutionEnvironment

        // create PulsarCsvOutputFormat instance
        val pulsarCsvOutputFormat =
          new PulsarCsvOutputFormat[NasaMission](SERVICE_URL, TOPIC_NAME)

        // create DataSet
        val textDS = env.fromCollection(nasaMissions)

        // map nasa mission names to upper-case
        textDS.map(nasaMission => NasaMission(
          nasaMission.id,
          nasaMission.missionName.toUpperCase,
          nasaMission.startYear,
          nasaMission.endYear))

        // filter missions which started after 1970
        .filter(_.startYear > 1970)

        // write batch data to Pulsar as Csv
        .output(pulsarCsvOutputFormat)

        // set parallelism to write Pulsar in parallel (optional)
        env.setParallelism(2)

        // execute program
        env.execute("Flink - Pulsar Batch Csv")
      }
```

### Sample Output

Please find sample output for above application as follows:
```
4,SKYLAB,1973,1974
5,APOLLO–SOYUZ TEST PROJECT,1975,1975
```

### Complete Example

You can find a complete example [here](https://github.com/apache/pulsar/tree/master/examples/flink-consumer-source/src/main/scala/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchCsvSinkScalaExample.scala).
In this example, Flink DataSet is processed and written to Pulsar in Csv format.


# PulsarJsonOutputFormat
### Usage

Please find Scala sample usage of `PulsarJsonOutputFormat` as follows:

```scala
      /**
        * NasaMission Model
        */
      private case class NasaMission(@BeanProperty id: Int,
                             @BeanProperty missionName: String,
                             @BeanProperty startYear: Int,
                             @BeanProperty endYear: Int)

      /**
        * Implementation
        */
      private val nasaMissions = List(
        NasaMission(1, "Mercury program", 1959, 1963),
        NasaMission(2, "Apollo program", 1961, 1972),
        NasaMission(3, "Gemini program", 1963, 1966),
        NasaMission(4, "Skylab", 1973, 1974),
        NasaMission(5, "Apollo–Soyuz Test Project", 1975, 1975))

      private val SERVICE_URL = "pulsar://127.0.0.1:6650"
      private val TOPIC_NAME = "my-flink-topic"

      def main(args: Array[String]): Unit = {

        // set up the execution environment
        val env = ExecutionEnvironment.getExecutionEnvironment

        // create PulsarJsonOutputFormat instance
        val pulsarJsonOutputFormat = new PulsarJsonOutputFormat[NasaMission](SERVICE_URL, TOPIC_NAME)

        // create DataSet
        val nasaMissionDS = env.fromCollection(nasaMissions)

        // map nasa mission names to upper-case
        nasaMissionDS.map(nasaMission =>
          NasaMission(
            nasaMission.id,
            nasaMission.missionName.toUpperCase,
            nasaMission.startYear,
            nasaMission.endYear))

        // filter missions which started after 1970
        .filter(_.startYear > 1970)

        // write batch data to Pulsar
        .output(pulsarJsonOutputFormat)

        // set parallelism to write Pulsar in parallel (optional)
        env.setParallelism(2)

        // execute program
        env.execute("Flink - Pulsar Batch Json")
      }
```

**Note:** Property definitions of the model should cover `@BeanProperty` to be visible.

### Sample Output

Please find sample output for above application as follows:
```
{"id":4,"missionName":"SKYLAB","startYear":1973,"endYear":1974}
{"id":5,"missionName":"APOLLO–SOYUZ TEST PROJECT","startYear":1975,"endYear":1975}
```

### Complete Example

You can find a complete example [here](https://github.com/apache/pulsar/tree/master/examples/flink-consumer-source/src/main/scala/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchJsonSinkScalaExample.scala).
In this example, Flink DataSet is processed and written to Pulsar in Json format.


# PulsarJsonOutputFormat
### Usage

Please find Scala sample usage of `PulsarJsonOutputFormat` as follows:

```scala
      val nasaMissions = List(
          NasaMission.newBuilder.setId(1).setName("Mercury program").setStartYear(1959).setEndYear(1963).build,
          NasaMission.newBuilder.setId(2).setName("Apollo program").setStartYear(1961).setEndYear(1972).build,
          NasaMission.newBuilder.setId(3).setName("Gemini program").setStartYear(1963).setEndYear(1966).build,
          NasaMission.newBuilder.setId(4).setName("Skylab").setStartYear(1973).setEndYear(1974).build,
          NasaMission.newBuilder.setId(5).setName("Apollo–Soyuz Test Project").setStartYear(1975).setEndYear(1975).build)
      
        def main(args: Array[String]): Unit = {
      
          // set up the execution environment
          val env = ExecutionEnvironment.getExecutionEnvironment
      
          // create PulsarCsvOutputFormat instance
          val pulsarAvroOutputFormat =
            new PulsarAvroOutputFormat[NasaMission](SERVICE_URL, TOPIC_NAME)
      
          // create DataSet
          val textDS = env.fromCollection(nasaMissions)
      
          // map nasa mission names to upper-case
          textDS.map(nasaMission => new NasaMission(
            nasaMission.getId,
            nasaMission.getName,
            nasaMission.getStartYear,
            nasaMission.getEndYear))
      
            // filter missions which started after 1970
            .filter(_.getStartYear > 1970)
      
            // write batch data to Pulsar as Avro
            .output(pulsarAvroOutputFormat)
      
          // set parallelism to write Pulsar in parallel (optional)
          env.setParallelism(2)
      
          // execute program
          env.execute("Flink - Pulsar Batch Avro")
        }
```

**Note:** Property definitions of the model should cover `@BeanProperty` to be visible.

### Sample Output

Please find sample output for above application as follows:
```
 "4,SKYLAB,1973,1974"
 "5,APOLLO–SOYUZ TEST PROJECT,1975,1975"
```

### Complete Example

You can find a complete example [here](https://github.com/apache/pulsar/tree/master/examples/flink-consumer-source/src/main/scala/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchAvroSinkScalaExample.scala).
In this example, Flink DataSet is processed and written to Pulsar in Avro format.

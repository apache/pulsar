The Flink Batch Sink for Pulsar is a custom sink that enables Apache [Flink](https://flink.apache.org/) to write [DataSet](https://ci.apache.org/projects/flink/flink-docs-stable/dev/batch/index.html) to Pulsar.

## Prerequisites

To use this sink, include a dependency for the `pulsar-flink` library in your Java configuration.

### Maven

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

### Gradle

If you're using Gradle, add this to your `build.gradle` file:

```groovy
def pulsarVersion = "{{pulsar:version}}"

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-flink', version: pulsarVersion
}
```

## Usage

Please find a sample usage as follows:

```java
        private static final String EINSTEIN_QUOTE = "Imagination is more important than knowledge. " +
                "Knowledge is limited. Imagination encircles the world.";

        private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
        private static final String TOPIC_NAME = "my-flink-topic";

        public static void main(String[] args) throws Exception {

            // set up the execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // create PulsarOutputFormat instance
            final OutputFormat<String> pulsarOutputFormat =
                    new PulsarOutputFormat(SERVICE_URL, TOPIC_NAME, wordWithCount -> wordWithCount.toString().getBytes());

            // create DataSet
            DataSet<String> textDS = env.fromElements(EINSTEIN_QUOTE);

            textDS.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out) throws Exception {
                    String[] words = value.toLowerCase().split(" ");
                    for(String word: words) {
                        out.collect(word.replace(".", ""));
                    }
                }
            })
            // filter words which length is bigger than 4
            .filter(word -> word.length() > 4)

            // write batch data to Pulsar
            .output(pulsarOutputFormat);

            // execute program
            env.execute("Flink - Pulsar Batch WordCount");
        }
```

## Sample Output

Please find sample output for above application as follows:
```
imagination
important
knowledge
knowledge
limited
imagination
encircles
world
```

## Complete Example

You can find a complete example [here](https://github.com/apache/incubator-pulsar/tree/master/pulsar-flink/src/test/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchSinkExample.java).
In this example, Flink DataSet is processed as word-count and being written to Pulsar.

## Complete Example Output
Please find sample output for above linked application as follows:
```
WordWithCount { word = important, count = 1 }
WordWithCount { word = encircles, count = 1 }
WordWithCount { word = imagination, count = 2 }
WordWithCount { word = knowledge, count = 2 }
WordWithCount { word = limited, count = 1 }
WordWithCount { word = world, count = 1 }
```
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

# PulsarOutputFormat
### Usage

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

### Sample Output

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

### Complete Example

You can find a complete example [here](https://github.com/apache/incubator-pulsar/tree/master/pulsar-flink/src/test/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchSinkExample.java).
In this example, Flink DataSet is processed as word-count and being written to Pulsar.

### Complete Example Output
Please find sample output for above linked application as follows:
```
WordWithCount { word = important, count = 1 }
WordWithCount { word = encircles, count = 1 }
WordWithCount { word = imagination, count = 2 }
WordWithCount { word = knowledge, count = 2 }
WordWithCount { word = limited, count = 1 }
WordWithCount { word = world, count = 1 }
```

# PulsarCsvOutputFormat
### Usage

Please find a sample usage as follows:

```java
        private static final List<Tuple4<Integer, String, Integer, Integer>> nasaMissions = Arrays.asList(
                new Tuple4(1, "Mercury program", 1959, 1963),
                new Tuple4(2, "Apollo program", 1961, 1972),
                new Tuple4(3, "Gemini program", 1963, 1966),
                new Tuple4(4, "Skylab", 1973, 1974),
                new Tuple4(5, "Apollo–Soyuz Test Project", 1975, 1975));

        private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
        private static final String TOPIC_NAME = "my-flink-topic";

        public static void main(String[] args) throws Exception {

            // set up the execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // create PulsarCsvOutputFormat instance
            final OutputFormat<Tuple4<Integer, String, Integer, Integer>> pulsarCsvOutputFormat =
                    new PulsarCsvOutputFormat<>(SERVICE_URL, TOPIC_NAME);

            // create DataSet
            DataSet<Tuple4<Integer, String, Integer, Integer>> nasaMissionDS = env.fromCollection(nasaMissions);
            // map nasa mission names to upper-case
            nasaMissionDS.map(
                new MapFunction<Tuple4<Integer, String, Integer, Integer>, Tuple4<Integer, String, Integer, Integer>>() {
                               @Override
                               public Tuple4<Integer, String, Integer, Integer> map(
                                       Tuple4<Integer, String, Integer, Integer> nasaMission) throws Exception {
                                   return new Tuple4(
                                           nasaMission.f0,
                                           nasaMission.f1.toUpperCase(),
                                           nasaMission.f2,
                                           nasaMission.f3);
                               }
                           }
            )
            // filter missions which started after 1970
            .filter(nasaMission -> nasaMission.f2 > 1970)
            // write batch data to Pulsar
            .output(pulsarCsvOutputFormat);

            // set parallelism to write Pulsar in parallel (optional)
            env.setParallelism(2);

            // execute program
            env.execute("Flink - Pulsar Batch Csv");

        }
```

### Sample Output

Please find sample output for above application as follows:
```
4,SKYLAB,1973,1974
5,APOLLO–SOYUZ TEST PROJECT,1975,1975
```

### Complete Example

You can find a complete example [here](https://github.com/apache/incubator-pulsar/tree/master/pulsar-flink/src/test/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchCsvSinkExample.java).
In this example, Flink DataSet is processed and written to Pulsar in Csv format.


# PulsarJsonOutputFormat
### Usage

Please find a sample usage as follows:

```java
        private static final List<NasaMission> nasaMissions = Arrays.asList(
                new NasaMission(1, "Mercury program", 1959, 1963),
                new NasaMission(2, "Apollo program", 1961, 1972),
                new NasaMission(3, "Gemini program", 1963, 1966),
                new NasaMission(4, "Skylab", 1973, 1974),
                new NasaMission(5, "Apollo–Soyuz Test Project", 1975, 1975));

        private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
        private static final String TOPIC_NAME = "my-flink-topic";

        public static void main(String[] args) throws Exception {

            // set up the execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // create PulsarJsonOutputFormat instance
            final OutputFormat<NasaMission> pulsarJsonOutputFormat = new PulsarJsonOutputFormat<>(SERVICE_URL, TOPIC_NAME);

            // create DataSet
            DataSet<NasaMission> nasaMissionDS = env.fromCollection(nasaMissions);
            // map nasa mission names to upper-case
            nasaMissionDS.map(nasaMission -> new NasaMission(
                    nasaMission.id,
                    nasaMission.missionName.toUpperCase(),
                    nasaMission.startYear,
                    nasaMission.endYear))
            // filter missions which started after 1970
            .filter(nasaMission -> nasaMission.startYear > 1970)
            // write batch data to Pulsar
            .output(pulsarJsonOutputFormat);

            // set parallelism to write Pulsar in parallel (optional)
            env.setParallelism(2);

            // execute program
            env.execute("Flink - Pulsar Batch Json");
        }

        /**
         * NasaMission data model
         *
         * Note: Property definitions of the model should be public or have getter functions to be visible
         */
        private static class NasaMission {

            private int id;
            private String missionName;
            private int startYear;
            private int endYear;

            public NasaMission(int id, String missionName, int startYear, int endYear) {
                this.id = id;
                this.missionName = missionName;
                this.startYear = startYear;
                this.endYear = endYear;
            }

            public int getId() {
                return id;
            }

            public String getMissionName() {
                return missionName;
            }

            public int getStartYear() {
                return startYear;
            }

            public int getEndYear() {
                return endYear;
            }
        }

```

**Note:** Property definitions of the model should be public or have getter functions to be visible

### Sample Output

Please find sample output for above application as follows:
```
{"id":4,"missionName":"SKYLAB","startYear":1973,"endYear":1974}
{"id":5,"missionName":"APOLLO–SOYUZ TEST PROJECT","startYear":1975,"endYear":1975}
```

### Complete Example

You can find a complete example [here](https://github.com/apache/incubator-pulsar/tree/master/pulsar-flink/src/test/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchJsonSinkExample.java).
In this example, Flink DataSet is processed and written to Pulsar in Json format.

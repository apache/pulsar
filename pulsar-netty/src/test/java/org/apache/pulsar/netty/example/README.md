The Pulsar Netty Connector enables Tcp Clients [Netty](https://netty.io/) to write Tcp messages to Pulsar.

# Prerequisites

To use this connector, include a dependency for the `pulsar-netty` library in your Java configuration.

# Maven

If you're using Maven, add this to your `pom.xml`:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{pulsar:version}}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-netty</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

# Gradle

If you're using Gradle, add this to your `build.gradle` file:

```groovy
def pulsarVersion = "{{pulsar:version}}"

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-netty', version: pulsarVersion
}
```

# PulsarTcpServer
### Usage

Please find a sample usage as follows. Custom Serializer is used in this example. Also, `PulsarStringSerializer` can be used:

```java
        private static String HOST = "localhost";
        private static int PORT = 8999;
        private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
        private static final String TOPIC_NAME = "my-netty-topic";

        public static void main(String[] args) throws Exception {

            PulsarTcpServer<String> pulsarTcpServer = new PulsarTcpServer.Builder<String>()
                    .setHost(HOST)
                    .setPort(PORT)
                    .setServiceUrl(SERVICE_URL)
                    .setTopicName(TOPIC_NAME)
                    .setNumberOfThreads(2)
                    .setDecoder(new StringDecoder())
                    .setPulsarSerializer(new MyUpperCaseStringSerializer())
                    .build();

            pulsarTcpServer.run();
        }

        private static class MyUpperCaseStringSerializer implements PulsarSerializer<String> {

            @Override
            public byte[] serialize(String s) {
                return s.toUpperCase().getBytes();
            }

        }
```

###  Tcp Client

Please use telnet as Tcp client to send messages as follows:
```
telnet localhost 8999
```

### Sample Output

Please find sample input and output for above application as follows:

**Input:**
```
Houston, we do not have a problem :)
```

**Output:**
```
HOUSTON, WE DO NOT HAVE A PROBLEM :)
```

### Complete Example

You can find a complete examples [here](https://github.com/apache/incubator-pulsar/tree/master/pulsar-netty/src/test/java/org/apache/pulsar/netty/example/).
In this example, Incoming Tcp messages are being written to Pulsar.

### References
https://netty.io/wiki/user-guide-for-4.x.html
https://netty.io/wiki/

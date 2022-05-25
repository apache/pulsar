---
id: io-develop
title: Develop Connectors
sidebar_label: "Developing Connectors"
original_id: io-develop
---

This guide describes how developers can write new connectors for Pulsar IO to move data
between Pulsar and other systems. It describes how to create a Pulsar IO connector.

Pulsar IO connectors are specialized [Pulsar Functions](functions-overview). So writing
a Pulsar IO connector is as simple as writing a Pulsar function. Pulsar IO connectors come
in two flavors: {@inject: github:Source:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java},
which import data from another system, and {@inject: github:Sink:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java},
which export data to another system. For example, [KinesisSink](io-kinesis) would export
the messages of a Pulsar topic to a Kinesis stream, and [RabbitmqSource](io-rabbitmq) would import
the messages of a RabbitMQ queue to a Pulsar topic.

### Developing

#### Develop a source connector

What you need to develop a source connector is to implement {@inject: github:Source:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java}
interface.

First, you need to implement the {@inject: github:open:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java#L33} method. This method will be called once when the source connector
is initialized. In this method, you can retrieve all the connector specific settings through
the passed `config` parameter, and initialize all the necessary resourcess. For example, a Kafka
connector can create the Kafka client in this `open` method.

Beside the passed-in `config` object, the Pulsar runtime also provides a `SourceContext` for the
connector to access runtime resources for tasks like collecting metrics. The implementation can
save the `SourceContext` for further usage.

```java

    /**
     * Open connector with configuration
     *
     * @param config initialization config
     * @param sourceContext
     * @throws Exception IO type exceptions when opening a connector
     */
    void open(final Map<String, Object> config, SourceContext sourceContext) throws Exception;

```

The main task for a Source implementor is to implement {@inject: github:read:/master/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java#L41}
method.

```java

    /**
     * Reads the next message from source.
     * If source does not have any new messages, this call should block.
     * @return next message from source.  The return result should never be null
     * @throws Exception
     */
    Record<T> read() throws Exception;

```

The implementation should be blocking on this method if nothing to return. It should never return
`null`. The returned {@inject: github:Record:/master/pulsar-functions/api-java/src/main/java/org/apache/pulsar/functions/api/Record.java#L28} should encapsulates the information that is needed by
Pulsar IO runtime.

These information includes:

- *Topic Name*: _Optional_. If the record is originated from a Pulsar topic, it should be the Pulsar topic name.
- *Key*: _Optional_. If the record has a key associated with it.
- *Value*: _Required_. The actual data of this record.
- *Partition Id*: _Optional_. If the record is originated from a partitioned source,
  return its partition id. The partition id will be used as part of the unique identifier
  by Pulsar IO runtime to do message deduplication and achieve exactly-once processing guarantee.
- *Record Sequence*: _Optional_. If the record is originated from a sequential source,
  return its record sequence. The record sequence will be used as part of the unique identifier
  by Pulsar IO runtime to do message deduplication and achieve exactly-once processing guarantee.
- *Properties*: _Optional_. If the record carries user-defined properties, return those properties.

Additionally, the implementation of the record should provide two methods: `ack` and `fail`. These
two methods will be used by Pulsar IO connector to acknowledge the records that it has done
processing and fail the records that it has failed to process.

{@inject: github:KafkaSource:/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSource.java} is a good example to follow.

#### Develop a sink connector

Developing a sink connector is as easy as developing a source connector. You just need to
implement {@inject: github:Sink:/master/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java} interface.

Similarly, you first need to implement the {@inject: github:open:/master/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java#L36} method to initialize all the necessary resources
before implementing the {@inject: github:write:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java#L44} method.

```java

    /**
     * Open connector with configuration
     *
     * @param config initialization config
     * @param sinkContext
     * @throws Exception IO type exceptions when opening a connector
     */
    void open(final Map<String, Object> config, SinkContext sinkContext) throws Exception;

```

The main task for a Sink implementor is to implement {@inject: github:write:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java#L44} method.

```java

    /**
     * Write a message to Sink
     * @param record record to write to sink
     * @throws Exception
     */
    void write(Record<T> record) throws Exception;

```

In the implementation of `write` method, the implementor can decide how to write the value and
the optional key to the actual source, and leverage all the provided information such as
`Partition Id`, `Record Sequence` for achieving different processing guarantees. The implementor
is also responsible for acknowledging records if it has successfully written them or failing
records if has failed to write them.

### Testing

Testing connectors can be challenging because Pulsar IO connectors interact with two systems
that may be difficult to mock - Pulsar and the system the connector is connecting to. It is
recommended to write very specifically test the functionalities of the connector classes
while mocking the external services.

Once you have written sufficient unit tests for your connector, we also recommend adding
separate integration tests to verify end-to-end functionality. In Pulsar, we are using [testcontainers](https://www.testcontainers.org/) for all Pulsar integration tests. Pulsar IO
{@inject: github:IntegrationTests:/tests/integration/src/test/java/org/apache/pulsar/tests/integration/io} are good examples to follow on integration testing your connectors.

### Packaging

Once you've developed and tested your connector, you must package it so that it can be submitted
to a [Pulsar Functions](functions-overview) cluster. There are two approaches described
here work with Pulsar Functions' runtime.

If you plan to package and distribute your connector for others to use, you are obligated to
properly license and copyright your own code and to adhere to the licensing and copyrights of
all libraries your code uses and that you include in your distribution. If you are using the
approach described in ["Creating a NAR package"](#creating-a-nar-package), the NAR plugin will
automatically create a `DEPENDENCIES` file in the generated NAR package, including the proper
licensing and copyrights of all libraries of your connector.

#### Creating a NAR package

The easiest approach to packaging a Pulsar IO connector is to create a NAR package using [nifi-nar-maven-plugin](https://mvnrepository.com/artifact/org.apache.nifi/nifi-nar-maven-plugin).

NAR stands for NiFi Archive. It is a custom packaging mechanism used by Apache NiFi, to provide
a bit of Java ClassLoader isolation. For more details, you can read this [blog post](https://medium.com/hashmapinc/nifi-nar-files-explained-14113f7796fd) to understand
how NAR works. Pulsar uses the same mechanism for packaging all the [builtin connectors](io-connectors).

All what you need is to include this [nifi-nar-maven-plugin](https://mvnrepository.com/artifact/org.apache.nifi/nifi-nar-maven-plugin) in your maven project for your connector. For example:

```xml

<plugins>
  <plugin>
    <groupId>org.apache.nifi</groupId>
    <artifactId>nifi-nar-maven-plugin</artifactId>
    <version>1.2.0</version>
  </plugin>
</plugins>

```

The {@inject: github:TwitterFirehose:/pulsar-io/twitter} connector is a good example to follow.

#### Creating an Uber JAR

An alternative approach is to create an _uber JAR_ that contains all of the connector's JAR files
and other resource files. No directory internal structure is necessary.

You can use [maven-shade-plugin](https://maven.apache.org/plugins/maven-shade-plugin/examples/includes-excludes.html) to create a Uber JAR. For example:

```xml

<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <version>3.1.1</version>
  <executions>
    <execution>
      <phase>package</phase>
      <goals>
        <goal>shade</goal>
      </goals>
      <configuration>
        <filters>
          <filter>
            <artifact>*:*</artifact>
          </filter>
        </filters>
      </configuration>
    </execution>
  </executions>
</plugin>

```


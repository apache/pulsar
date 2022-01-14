---
id: develop-plugin
title: Pulsar plugin development
sidebar_label: Plugin
---

You can develop various plugins for Pulsar, such as entry filters, protocol handlers, interceptors, and so on.

## Entry filter

This chapter describes what the entry filter is and shows how to use the entry filter.

### What is an entry filter?

The entry filter is an extension point for implementing a custom message entry strategy. With an entry filter, you can decide **whether to send messages to consumers** (brokers can use the return values of entry filters to determine whether the messages need to be sent or discarded) or **send messages to specific consumers.** 

To implement features such as tagged messages or custom delayed messages, use [`subscriptionProperties`](https://github.com/apache/pulsar/blob/ec0a44058d249a7510bb3d05685b2ee5e0874eb6/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/ConsumerBuilder.java?_pjax=%23js-repo-pjax-container%2C%20div%5Bitemtype%3D%22http%3A%2F%2Fschema.org%2FSoftwareSourceCode%22%5D%20main%2C%20%5Bdata-pjax-container%5D#L174), [`​​properties`](https://github.com/apache/pulsar/blob/ec0a44058d249a7510bb3d05685b2ee5e0874eb6/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/ConsumerBuilder.java?_pjax=%23js-repo-pjax-container%2C%20div%5Bitemtype%3D%22http%3A%2F%2Fschema.org%2FSoftwareSourceCode%22%5D%20main%2C%20%5Bdata-pjax-container%5D#L533), and entry filters.

### How to use an entry filter?

Follow the steps below:

1. Create a Maven project.
   
2. Implement the `EntryFilter` interface.
   
3. Package the implementation class into a NAR file.

4. Configure the `broker.conf` file (or the `standalone.conf` file) and restart your broker.

#### Step 1: Create a Maven project

For how to create a Maven project, see [here](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html).

#### Step 2: Implement the `EntryFilter` interface

1. Add a dependency for Pulsar broker in the `pom.xml` file to display. Otherwise, you can not find the [`EntryFilter` interface](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/plugin/EntryFilter.java).

    ```xml
    <dependency>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>pulsar-broker</artifactId>
    <version>${pulsar.version}</version>
    <scope>provided</scope>
    </dependency>
    ```

2. Implement the [`FilterResult filterEntry(Entry entry, FilterContext context);` method](https://github.com/apache/pulsar/blob/2adb6661d5b82c5705ee00ce3ebc9941c99635d5/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/plugin/EntryFilter.java#L34).

   - If the method returns `ACCEPT` or NULL, this message is sent to consumers.

   - If the method returns `REJECT`, this message is filtered out and it does not consume message permits. 

   - If there are multiple entry filters, this message passes through all filters in the pipeline in a round-robin manner. If any entry filter returns `REJECT`, this message is discarded.

    You can get entry metadata, subscriptions, and other information through `FilterContext`.

3. Describe a NAR file.

    Create an `entry_filter.yaml` file in the `resources/META-INF/services` directory to describe a NAR file.

    ```conf
    # Entry filter name, which should be configured in the broker.conf file later
    name: entryFilter
    # Entry filter description
    description: entry filter
    # Implementation class name of entry filter 
    entryFilterClass: com.xxxx.xxxx.xxxx.DefaultEntryFilterImpl
    ```

#### Step 3: package implementation class of entry filter into a NAR file

1. Add the compiled plugin of the NAR file to your `pom.xml` file.

    ```xml
    <build>
            <finalName>${project.artifactId}</finalName>
            <plugins>
                <plugin>
                    <groupId>org.apache.nifi</groupId>
                    <artifactId>nifi-nar-maven-plugin</artifactId>
                    <version>1.2.0</version>
                    <extensions>true</extensions>
                    <configuration>
                        <finalName>${project.artifactId}-${project.version}</finalName>
                    </configuration>
                    <executions>
                        <execution>
                            <id>default-nar</id>
                            <phase>package</phase>
                            <goals>
                                <goal>nar</goal>
                            </goals>
                        </execution>
                    </executions>
                </plugin>
            </plugins>
        </build>
    ```

2. Generate a NAR file in the `target` directory.

    ```script
    mvn clean install
    ```

#### Step 4: configure and restart broker

1. Configure the following parameters in the `broker.conf` file (or the `standalone.conf` file).

    ```conf
    # Class name of pluggable entry filters
    # Multiple classes need to be separated by commas.
    entryFilterNames=entryFilter1,entryFilter2,entryFilter3
    # The directory for all entry filter implementations
    entryFiltersDirectory=tempDir
    ```

2. Restart your broker. 
   
    You can see the following broker log if the plug-in is successfully loaded.

    ```text
    Successfully loaded entry filter for name `{name of your entry filter}`
    ```

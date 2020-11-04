---
id: version-2.6.1-window-functions-context
title: Window Functions Context
sidebar_label: "Window Functions: Context"
original_id: window-functions-context
---

Java SDK provides access to a **window context object** that can be used by a window function. This context object provides a wide variety of information and functionality for Pulsar window functions as below.

- [Spec](#spec)

  * Names of all input topics and the output topic associated with the function.
  * Tenant and namespace associated with the function.
  * Pulsar window function name, ID, and version.
  * ID of the Pulsar function instance running the window function.
  * Number of instances that invoke the window function.
  * Built-in type or custom class name of the output schema.
  
- [Logger](#logger)
  
  * Logger object used by the window function, which can be used to create window function log messages.

- [User config](#user-config)
  
  * Access to arbitrary user configuration values.

- [Routing](#routing)
  
  * Routing is supported in Pulsar window functions. Pulsar window functions send messages to arbitrary topics as per the `publish` interface.

- [Metrics](#metrics)
  
  * Interface for recording metrics.

- [State storage](#state-storage)
  
  * Interface for storing and retrieving state in [state storage](#state-storage).

## Spec

Spec contains the basic information of a function.

### Get input topics

The `getInputTopics` method gets the **name list** of all input topics.

This example demonstrates how to get the name list of all input topics in a Java window function.

```java
public class GetInputTopicsWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        Collection<String> inputTopics = context.getInputTopics();
        System.out.println(inputTopics);

        return null;
    }

}
```

### Get output topic

The `getOutputTopic` method gets the **name of a topic** to which the message is sent.

This example demonstrates how to get the name of an output topic in a Java window function.

```java
public class GetOutputTopicWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        String outputTopic = context.getOutputTopic();
        System.out.println(outputTopic);

        return null;
    }
}
```

### Get tenant

The `getTenant` method gets the tenant name associated with the window function.

This example demonstrates how to get the tenant name in a Java window function.

```java
public class GetTenantWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        String tenant = context.getTenant();
        System.out.println(tenant);

        return null;
    }

}
```

### Get namespace

The `getNamespace` method gets the namespace associated with the window function.

This example demonstrates how to get the namespace in a Java window function.

```java
public class GetNamespaceWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        String ns = context.getNamespace();
        System.out.println(ns);

        return null;
    }

}
```

### Get function name

The `getFunctionName` method gets the window function name.

This example demonstrates how to get the function name in a Java window function.

```java
public class GetNameOfWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        String functionName = context.getFunctionName();
        System.out.println(functionName);

        return null;
    }

}
```

### Get function ID

The `getFunctionId` method gets the window function ID.

This example demonstrates how to get the function ID in a Java window function.

```java
public class GetFunctionIDWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        String functionID = context.getFunctionId();
        System.out.println(functionID);

        return null;
    }

}
```

### Get function version

The `getFunctionVersion` method gets the window function version.

This example demonstrates how to get the function version of a Java window function.

```java
public class GetVersionOfWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        String functionVersion = context.getFunctionVersion();
        System.out.println(functionVersion);

        return null;
    }

}
```

### Get instance ID

The `getInstanceId` method gets the instance ID of a window function.

This example demonstrates how to get the instance ID in a Java window function.

```java
public class GetInstanceIDWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        int instanceId = context.getInstanceId();
        System.out.println(instanceId);

        return null;
    }

}
```

### Get num instances

The `getNumInstances` method gets the number of instances that invoke the window function.

This example demonstrates how to get the number of instances in a Java window function.

```java
public class GetNumInstancesWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        int numInstances = context.getNumInstances();
        System.out.println(numInstances);

        return null;
    }

}
```

### Get output schema type

The `getOutputSchemaType` method gets the built-in type or custom class name of the output schema.

This example demonstrates how to get the output schema type of a Java window function.

```java
public class GetOutputSchemaTypeWindowFunction implements WindowFunction<String, Void> {

    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        String schemaType = context.getOutputSchemaType();
        System.out.println(schemaType);

        return null;
    }
}
```

## Logger

Pulsar window functions using Java SDK has access to an [SLF4j](https://www.slf4j.org/) [`Logger`](https://www.slf4j.org/api/org/apache/log4j/Logger.html) object that can be used to produce logs at the chosen log level.

This example logs either a `WARNING`-level or `INFO`-level log based on whether the incoming string contains the word `danger` or not in a Java function.

```java
import java.util.Collection;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.WindowContext;
import org.apache.pulsar.functions.api.WindowFunction;
import org.slf4j.Logger;

public class LoggingWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        Logger log = context.getLogger();
        for (Record<String> record : inputs) {
            log.info(record + "-window-log");
        }
        return null;
    }

}
```

If you need your function to produce logs, specify a log topic when creating or running the function. 

```bash
bin/pulsar-admin functions create \
  --jar my-functions.jar \
  --classname my.package.LoggingFunction \
  --log-topic persistent://public/default/logging-function-logs \
  # Other function configs
```

You can access all logs produced by `LoggingFunction` via the `persistent://public/default/logging-function-logs` topic.

## Metrics

Pulsar window functions can publish arbitrary metrics to the metrics interface which can be queried. 

> **Note**
>
> If a Pulsar window function uses the language-native interface for Java, that function is not able to publish metrics and stats to Pulsar.

You can record metrics using the context object on a per-key basis. 

This example sets a metric for the `process-count` key and a different metric for the `elevens-count` key every time the function processes a message in a Java function. 

```java
import java.util.Collection;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.WindowContext;
import org.apache.pulsar.functions.api.WindowFunction;


/**
 * Example function that wants to keep track of
 * the event time of each message sent.
 */
public class UserMetricWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {

        for (Record<String> record : inputs) {
            if (record.getEventTime().isPresent()) {
                context.recordMetric("MessageEventTime", record.getEventTime().get().doubleValue());
            }
        }

        return null;
    }
}
```

## User config

When you run or update Pulsar Functions that are created using SDK, you can pass arbitrary key/value pairs to them with the `--user-config` flag. Key/value pairs **must** be specified as JSON. 

This example passes a user configured key/value to a function.

```bash
bin/pulsar-admin functions create \
  --name word-filter \
 --user-config '{"forbidden-word":"rosebud"}' \
  # Other function configs
```

### API
You can use the following APIs to get user-defined information for window functions.
#### getUserConfigMap

`getUserConfigMap` API gets a map of all user-defined key/value configurations for the window function.


```java
/**
     * Get a map of all user-defined key/value configs for the function.
     *
     * @return The full map of user-defined config values
     */
    Map<String, Object> getUserConfigMap();
```


#### getUserConfigValue

The `getUserConfigValue` API gets a user-defined key/value.

```java
/**
     * Get any user-defined key/value.
     *
     * @param key The key
     * @return The Optional value specified by the user for that key.
     */
    Optional<Object> getUserConfigValue(String key);
```

#### getUserConfigValueOrDefault

The `getUserConfigValueOrDefault` API gets a user-defined key/value or a default value if none is present.

```java
/**
     * Get any user-defined key/value or a default value if none is present.
     *
     * @param key
     * @param defaultValue
     * @return Either the user config value associated with a given key or a supplied default value
     */
    Object getUserConfigValueOrDefault(String key, Object defaultValue);
```

This example demonstrates how to access key/value pairs provided to Pulsar window functions.

Java SDK context object enables you to access key/value pairs provided to Pulsar window functions via the command line (as JSON). 

>**Tip**
>
> For all key/value pairs passed to Java window functions, both the `key` and the `value` are `String`. To set the value to be a different type, you need to deserialize it from the `String` type.

This example passes a key/value pair in a Java window function.

```bash
bin/pulsar-admin functions create \
   --user-config '{"word-of-the-day":"verdure"}' \
  # Other function configs
 ```

This example accesses values in a Java window function.

The `UserConfigFunction` function logs the string `"The word of the day is verdure"` every time the function is invoked (which means every time a message arrives). The user config of `word-of-the-day` is changed **only** when the function is updated with a new config value via 
multiple ways, such as the command line tool or REST API.

```java
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.util.Optional;

public class UserConfigWindowFunction implements WindowFunction<String, String> {
    @Override
    public String process(Collection<Record<String>> input, WindowContext context) throws Exception {
        Optional<Object> whatToWrite = context.getUserConfigValue("WhatToWrite");
        if (whatToWrite.get() != null) {
            return (String)whatToWrite.get();
        } else {
            return "Not a nice way";
        }
    }

}
```

If no value is provided, you can access the entire user config map or set a default value.

```java
// Get the whole config map
Map<String, String> allConfigs = context.getUserConfigMap();

// Get value or resort to default
String wotd = context.getUserConfigValueOrDefault("word-of-the-day", "perspicacious");
```

## Routing

You can use the `context.publish()` interface to publish as many results as you want.

This example shows that the `PublishFunction` class uses the built-in function in the context to publish messages to the `publishTopic` in a Java function.

```java
public class PublishWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> input, WindowContext context) throws Exception {
        String publishTopic = (String) context.getUserConfigValueOrDefault("publish-topic", "publishtopic");
        String output = String.format("%s!", input);
        context.publish(publishTopic, output);

        return null;
    }

}
```

## State storage

Pulsar window functions use [Apache BookKeeper](https://bookkeeper.apache.org) as a state storage interface. Apache Pulsar installation (including the standalone installation) includes the deployment of BookKeeper bookies.

Apache Pulsar integrates with Apache BookKeeper `table service` to store the `state` for functions. For example, the `WordCount` function can store its `counters` state into BookKeeper table service via Pulsar Functions state APIs.

States are key-value pairs, where the key is a string and the value is arbitrary binary data—counters are stored as 64-bit big-endian binary values. Keys are scoped to an individual Pulsar Function and shared between instances of that function.

Currently, Pulsar window functions expose Java API to access, update, and manage states. These APIs are available in the context object when you use Java SDK functions.

| Java API| Description
|---|---
|`incrCounter`|Increases a built-in distributed counter referred by key.
|`getCounter`|Gets the counter value for the key.
|`putState`|Updates the state value for the key.

You can use the following APIs to access, update, and manage states in Java window functions. 

#### incrCounter

The `incrCounter` API increases a built-in distributed counter referred by key.

Applications use the `incrCounter` API to change the counter of a given `key` by the given `amount`. If the `key` does not exist, a new key is created.

```java
    /**
     * Increment the builtin distributed counter referred by key
     * @param key The name of the key
     * @param amount The amount to be incremented
     */
    void incrCounter(String key, long amount);
```

#### getCounter

The `getCounter` API gets the counter value for the key.

Applications uses the `getCounter` API to retrieve the counter of a given `key` changed by the `incrCounter` API.

```java
    /**
     * Retrieve the counter value for the key.
     *
     * @param key name of the key
     * @return the amount of the counter value for this key
     */
    long getCounter(String key);
```

Except the `getCounter` API, Pulsar also exposes a general key/value API (`putState`) for functions to store general key/value state.

#### putState

The `putState` API updates the state value for the key.

```java
    /**
     * Update the state value for the key.
     *
     * @param key name of the key
     * @param value state value of the key
     */
    void putState(String key, ByteBuffer value);
```

This example demonstrates how applications store states in Pulsar window functions.

The logic of the `WordCountWindowFunction` is simple and straightforward.

1. The function first splits the received string into multiple words using regex `\\.`.

2. For each `word`, the function increments the corresponding `counter` by 1 via `incrCounter(key, amount)`.

```java
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.Arrays;

public class WordCountWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        for (Record<String> input : inputs) {
            Arrays.asList(input.getValue().split("\\.")).forEach(word -> context.incrCounter(word, 1));
        }
        return null;

    }
}
```


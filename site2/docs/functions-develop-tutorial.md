---
id: functions-develop-tutorial
title: Tutorials
sidebar_label: "Tutorials"
---

## Write a function for word count

:::note

The following example is a stateful function. By default, the state of a function is disabled. See [Enable stateful functions](functions-worker-stateful.md) for more instructions.

:::

1. Write the function in Java using the [SDK for Java](functions-develop-api.md).

   ```java

    package org.example.functions;

    import org.apache.pulsar.functions.api.Context;
    import org.apache.pulsar.functions.api.Function;

    import java.util.Arrays;

    public class WordCountFunction implements Function<String, Void> {
        // This function is invoked every time a message is published to the input topic
        @Override
        public Void process(String input, Context context) throws Exception {
            Arrays.asList(input.split(" ")).forEach(word -> {
                String counterKey = word.toLowerCase();
                context.incrCounter(counterKey, 1);
            });
            return null;
        }
    }

   ```

2. Bundle and build the JAR file, and then deploy it in your Pulsar cluster using the `pulsar-admin` command.

   ```bash

    bin/pulsar-admin functions create \
      --jar target/my-jar-with-dependencies.jar \
      --classname org.example.functions.WordCountFunction \
      --tenant public \
      --namespace default \
      --name word-count \
      --inputs persistent://public/default/sentences \
      --output persistent://public/default/count
   ```

## Write a function for content-based routing

1. Write the function in Python using the [SDK for Python](functions-develop-api.md).

   ```python

    from pulsar import Function

    class RoutingFunction(Function):
        def __init__(self):
            self.fruits_topic = "persistent://public/default/fruits"
            self.vegetables_topic = "persistent://public/default/vegetables"

        def is_fruit(item):
            return item in [b"apple", b"orange", b"pear", b"other fruits..."]

        def is_vegetable(item):
            return item in [b"carrot", b"lettuce", b"radish", b"other vegetables..."]

        def process(self, item, context):
            if self.is_fruit(item):
                context.publish(self.fruits_topic, item)
            elif self.is_vegetable(item):
                context.publish(self.vegetables_topic, item)
            else:
                warning = "The item {0} is neither a fruit nor a vegetable".format(item)
                context.get_logger().warn(warning)

   ```

2. Suppose this code is stored in `~/router.py`, then you can deploy it in your Pulsar cluster using the `pulsar-admin` command.

   ```bash

    bin/pulsar-admin functions create \
      --py ~/router.py \
      --classname router.RoutingFunction \
      --tenant public \
      --namespace default \
      --name route-fruit-veg \
      --inputs persistent://public/default/basket-items

   ```

## Write a window function for word count

:::note

Currently, window function is only available in Java. 

:::

This example demonstrates how to use the [language-native interface](functions-develop-api.md) to write a window function in Java. 

Each input message is a sentence that is split into words and each word counted. The built-in counter state is used to keep track of the word count in a persistent and consistent manner.

```java

public class WordCountFunction implements Function<String, Void> {
    @Override
    public Void process(String input, Context context) {
        Arrays.asList(input.split("\\s+")).forEach(word -> context.incrCounter(word, 1));
        return null;
    }
}

```

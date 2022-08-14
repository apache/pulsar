---
id: functions-develop-serde
title: Use SerDe
sidebar_label: "Use SerDe"
---

Pulsar Functions use SerDe (**Ser**ialization and **De**serialization) when publishing data to or consuming data from Pulsar topics. How SerDe works by default depends on the language you use (Java or Python) for a particular function. In both languages, however, you can write custom SerDe logic for more complex, application-specific types.

## Use SerDe for Java functions

The following basic Java types are built-in and supported by default for Java functions: `string`, `double`, `integer`, `float`, `long`, `short`, and `byte`.

To customize Java types, you need to implement the following interface.

```java

public interface SerDe<T> {
    T deserialize(byte[] input);
    byte[] serialize(T input);
}

```

SerDe works in the following ways for Java functions.
- If the input and output topics have a schema, Pulsar Functions use the schema for SerDe.
- If the input or output topics do not exist, Pulsar Functions adopt the following rules to determine SerDe:
  - If the schema type is specified, Pulsar Functions use the specified schema type.
  - If SerDe is specified, Pulsar Functions use the specified SerDe, and the schema type for input and output topics is `byte`.
  - If neither the schema type nor SerDe is specified, Pulsar Functions use the built-in SerDe. For non-primitive schema types, the built-in SerDe serializes and deserializes objects in the `JSON` format. 

For example, imagine that you're writing a function that processes tweet objects. You can refer to the following example of the `Tweet` class in Java.

```java

public class Tweet {
    private String username;
    private String tweetContent;

    public Tweet(String username, String tweetContent) {
        this.username = username;
        this.tweetContent = tweetContent;
    }

    // Standard setters and getters
}

```

To pass `Tweet` objects directly between functions, you need to provide a custom SerDe class. In the example below, `Tweet` objects are basically strings, and username and tweet content are separated by `|`.

```java

package com.example.serde;

import org.apache.pulsar.functions.api.SerDe;

import java.util.regex.Pattern;

public class TweetSerde implements SerDe<Tweet> {
    public Tweet deserialize(byte[] input) {
        String s = new String(input);
        String[] fields = s.split(Pattern.quote("|"));
        return new Tweet(fields[0], fields[1]);
    }

    public byte[] serialize(Tweet input) {
        return "%s|%s".format(input.getUsername(), input.getTweetContent()).getBytes();
    }
}

```

To apply a customized SerDe to a particular function, you need to:
* Package the `Tweet` and `TweetSerde` classes into a JAR.
* Specify a path to the JAR and SerDe class name when deploying the function.

The following is an example of using the `create` command to deploy a function by applying a customized SerDe.

```bash

 bin/pulsar-admin functions create \
  --jar /path/to/your.jar \
  --output-serde-classname com.example.serde.TweetSerde \
  # Other function attributes

```

:::note

Custom SerDe classes must be packaged with your function JARs.

:::

## Use SerDe for Python functions

In Python, the default SerDe is an identity, meaning that the type is serialized as whatever type the function returns.

For example, you can specify the SerDe as follows when deploying a function in [cluster mode](functions-deploy-cluster.md). 

```bash

bin/pulsar-admin functions create \
  --tenant public \
  --namespace default \
  --name my_function \
  --py my_function.py \
  --classname my_function.MyFunction \
  --custom-serde-inputs '{"input-topic-1":"Serde1","input-topic-2":"Serde2"}' \
  --output-serde-classname Serde3 \
  --output output-topic-1

```

This case contains two input topics: `input-topic-1` and `input-topic-2`, each of which is mapped to a different SerDe class (the mapping must be specified as a JSON string). The output topic `output-topic-1` uses the `Serde3` class for SerDe. 

:::note

All function related logic, including processing and SerDe classes, must be contained within a single Python file.

:::

The table outlines three SerDe options for Python functions.

| SerDe option | Description | Use case| 
| ------------|-----------|-----------| 
| `IdentitySerde` (default) | Use the [`IdentitySerde`](https://github.com/apache/pulsar/blob/master/pulsar-client-cpp/python/pulsar/functions/serde.py#L70), which leaves the data unchanged. Creating or running a function without explicitly specifying SerDe means that this option is used. | When you work with simple types like strings, booleans, integers.| 
| `PickleSerDe` | Use the [`PickleSerDe`](https://github.com/apache/pulsar/blob/master/pulsar-client-cpp/python/pulsar/functions/serde.py#L62), which uses Python [`pickle`](https://docs.python.org/3/library/pickle.html) for SerDe. | When you work with complex, application-specific types and are comfortable with the "best-effort" approach of `pickle`.| 
| `Custom SerDe` | Create a custom SerDe class by implementing the baseline [`SerDe`](https://github.com/apache/pulsar/blob/master/pulsar-client-cpp/python/pulsar/functions/serde.py#L50) class, which has just two methods:<br />* [`serialize`](https://github.com/apache/pulsar/blob/master/pulsar-client-cpp/python/pulsar/functions/serde.py#L53) for converting the object into bytes.<br />* [`deserialize`](https://github.com/apache/pulsar/blob/master/pulsar-client-cpp/python/pulsar/functions/serde.py#L58) for converting bytes into an object of the required application-specific type. | When you require explicit control over SerDe, potentially for performance or data compatibility purposes.| 

For example, imagine that you are writing a function that processes tweet objects. You can refer to the following example of the `Tweet` class in Python.

```python

class Tweet(object):
    def __init__(self, username, tweet_content):
        self.username = username
        self.tweet_content = tweet_content

```

To use this class in Pulsar Functions, you have two options:
* Specify `PickleSerDe`, which applies the `pickle` library for SerDe.
* Create your own SerDe class. The following is an example.

```python

from pulsar import SerDe

class TweetSerDe(SerDe):

    def serialize(self, input):
        return bytes("{0}|{1}".format(input.username, input.tweet_content))

    def deserialize(self, input_bytes):
        tweet_components = str(input_bytes).split('|')
        return Tweet(tweet_components[0], tweet_componentsp[1])

```

For more details, see [code example](https://github.com/apache/pulsar/blob/master/pulsar-functions/python-examples/custom_object_function.py).

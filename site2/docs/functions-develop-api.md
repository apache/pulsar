---
id: functions-develop-api
title: Use APIs
sidebar_label: "Use APIs"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

The following table outlines the APIs that you can use to develop Pulsar Functions in Java, Python, and Go.

| Interface | Description | Use case| 
|---------|------------|---------| 
| [Language-native interface for Java/Python](#use-language-native-interface-for-javapython) | No Pulsar-specific libraries or special dependencies required (only core libraries). | Functions that do not require access to the [context](functions-concepts.md#context).| 
| [Pulsar Functions SDK for Java/Python/Go](#use-sdk-for-javapythongo) | Pulsar-specific libraries that provide a range of functionality not available in the language-native interfaces,  such as state management or user configuration. | Functions that require access to the [context](functions-concepts.md#context).| 
| [Extended Pulsar Functions SDK for Java](#use-extended-sdk-for-java) | An extension to Pulsar-specific libraries, providing the initialization and close interfaces in Java. | Functions that require initializing and releasing external resources.| 


## Use language-native interface for Java/Python

The language-native interface provides a simple and clean approach to write Java/Python functions, by adding an exclamation point to all incoming strings and publishing the output string to a topic. It has no external dependencies. 

The following examples are language-native functions.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"}]}>
<TabItem value="Java">

To use a piece of Java code as a “language-native” function, you need to implement the `java.util.Function` interface. You can include any sort of complex logic inside the `apply` method to provide more processing capabilities.

```java

import java.util.function.Function;

public class JavaNativeExclamationFunction implements Function<String, String> {
    @Override
    public String apply(String input) {
        return String.format("%s!", input);
    }
}

```

For more details, see [code example](https://github.com/apache/pulsar/blob/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/JavaNativeExclamationFunction.java).

</TabItem>
<TabItem value="Python">

To use a piece of Python code as a “language-native” function, you must have a method named `process` as follows. It appends an exclamation point to any string value it receives.

```python

def process(input):
    return "{}!".format(input)

```

For more details, see [code example](https://github.com/apache/pulsar/blob/master/pulsar-functions/python-examples/native_exclamation_function.py).

:::note

Write Pulsar Functions in Python 3. To make sure your functions can run, you need to have Python 3 installed for functions workers and set Python 3 as the default interpreter.
 
:::

</TabItem>
</Tabs>
````


## Use SDK for Java/Python/Go

The implementation of Pulsar Functions SDK specifies a functional interface that includes the [context](functions-concepts.md#context) object as a parameter. 

The following examples use Pulsar Functions SDK for different languages.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"Go","value":"Go"}]}>
<TabItem value="Java">

When developing a function using the Java SDK, you need to implement the `org.apache.pulsar.functions.api.Function` interface. It specifies only one method that you need to implement called `process`.

```java

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class ExclamationFunction implements Function<String, String> {
    @Override
    public String process(String input, Context context) {
        return String.format("%s!", input);
    }
}

```

For more details, see [code example](https://github.com/apache/pulsar/blob/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/ExclamationFunction.java).

</TabItem>
<TabItem value="Python">

To develop a function using the Python SDK, you need to add the pulsar client dependency to your Python installation. 

```python

from pulsar import Function

class ExclamationFunction(Function):
  def __init__(self):
    pass

  def process(self, input, context):
    return input + '!'

```

For more details, see [code example](https://github.com/apache/pulsar/blob/master/pulsar-functions/python-examples/exclamation_function.py).

</TabItem>
<TabItem value="Go">

To develop a function using the Go SDK, you need to add the pulsar client dependency to your Go installation and provide the name of the function to the `pf.Start()` method inside the `main()` method. This registers the function with the Pulsar Functions framework and ensures that the specified function can be invoked when a new message arrives. 

```go

package main

import (
	"context"
	"fmt"

	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func HandleRequest(ctx context.Context, in []byte) error{
	fmt.Println(string(in) + "!")
	return nil
}

func main() {
	pf.Start(HandleRequest)
}

```

For more details, see [code example](https://github.com/apache/pulsar/blob/77cf09eafa4f1626a53a1fe2e65dd25f377c1127/pulsar-function-go/examples/inputFunc/inputFunc.go#L20-L36).

</TabItem>
</Tabs>
````


## Use extended SDK for Java

This extended Pulsar Functions SDK provides two additional interfaces to initialize and release external resources.
- By using the `initialize` interface, you can initialize external resources which only need one-time initialization when the function instance starts.
- By using the `close` interface, you can close the referenced external resources when the function instance closes. 

:::note

The extended Pulsar Functions SDK for Java is only available in Pulsar 2.10.0 or later versions. Before using it, you need to [set up function workers](functions-worker.md) in Pulsar 2.10.0 or later versions.

:::

The following example uses the extended interface of Pulsar Functions SDK for Java to initialize RedisClient when the function instance starts and release it when the function instance closes.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"}]}>
<TabItem value="Java">

```java

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import io.lettuce.core.RedisClient;

public class InitializableFunction implements Function<String, String> {
    private RedisClient redisClient;
    
    private void initRedisClient(Map<String, Object> connectInfo) {
        redisClient = RedisClient.create(connectInfo.get("redisURI"));
    }

    @Override
    public void initialize(Context context) {
        Map<String, Object> connectInfo = context.getUserConfigMap();
        redisClient = initRedisClient(connectInfo);
    }
    
    @Override
    public String process(String input, Context context) {
        String value = client.get(key);
        return String.format("%s-%s", input, value);
    }

    @Override
    public void close() {
        redisClient.close();
    }
}

```

</TabItem>
</Tabs>
````

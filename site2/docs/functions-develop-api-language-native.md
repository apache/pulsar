---
id: functions-develop-api-language-native
title: Use language-native interface for Java/Python
sidebar_label: "Use language-native interface"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

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

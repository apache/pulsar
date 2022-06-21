---
id: functions-debug-unit-test
title: Debug with unit test
sidebar_label: "Debug with unit test"
---


Like any function with inputs and outputs, you can test Pulsar Functions in a similar way as you test any other function.

:::note

Pulsar uses TestNG for testing.

:::

For example, if you have the following function written through the language-native interface for Java:

```java

import java.util.function.Function;

public class JavaNativeExclamationFunction implements Function<String, String> {
   @Override
   public String apply(String input) {
       return String.format("%s!", input);
   }
}

```

You can write a simple unit test to test the function.

```java

@Test
public void testJavaNativeExclamationFunction() {
   JavaNativeExclamationFunction exclamation = new JavaNativeExclamationFunction();
   String output = exclamation.apply("foo");
   Assert.assertEquals(output, "foo!");
}

```

The following example is written through the Java SDK.

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

You can write a unit test to test this function and mock the `Context` parameter as follows. 

```java

@Test
public void testExclamationFunction() {
   ExclamationFunction exclamation = new ExclamationFunction();
   String output = exclamation.process("foo", mock(Context.class));
   Assert.assertEquals(output, "foo!");
}

```

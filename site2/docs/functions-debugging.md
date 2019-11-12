---
id: functions-debugging
title: How to debug Pulsar Functions
sidebar_label: Debugging
---


## Use unit test

A Pulsar Function at its core is just a function with inputs and outputs, thus testing a Pulsar Function can be done in a similar way as testing any function.

For example, if a user has the following Pulsar Function:

```java
import java.util.function.Function;

public class JavaNativeExclamationFunction implements Function<String, String> {
   @Override
   public String apply(String input) {
       return String.format("%s!", input);
   }
}
```

The user can write a simple unit test to test this Pulsar function:

```java
@Test
public void testJavaNativeExclamationFunction() {
   JavaNativeExclamationFunction exclamation = new JavaNativeExclamationFunction();
   String output = exclamation.apply("foo");
   Assert.assertEquals(output, "foo!");
}
```

Consequently, if a user has a Pulsar Function that implements the ```org.apache.pulsar.functions.api.Function``` interface:

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

The user can write a unit test for this function as well. Remember to mock out the ```Context``` parameter.

For example:

```java
@Test
public void testExclamationFunction() {
   ExclamationFunction exclamation = new ExclamationFunction();
   String output = exclamation.process("foo", mock(Context.class));
   Assert.assertEquals(output, "foo!");
}
```

## Debug with localrun mode

> Note
>
> Currently, debugging with localrun mode only supports Pulsar Functions written in Java. Users need Pulsar version 2.4.0 or later to do the following. Even though localrun is available in versions earlier than Pulsar 2.4.0, it does not have the functionality to be executed programmatically and run Functions as threads.

To test in a more realistic fashion, a Pulsar Function can be run via localrun mode which will launch an instance of the Function on your local machine as a thread.

In this mode, the Pulsar Function can consume and produce actual data to a Pulsar cluster and mirrors how the function will actually run in a Pulsar cluster.

Users can launch his or her function in the following manner:

```java
FunctionConfig functionConfig = new FunctionConfig();
functionConfig.setName(functionName);
functionConfig.setInputs(Collections.singleton(sourceTopic));
functionConfig.setClassName(ExclamationFunction.class.getName());
functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
functionConfig.setOutput(sinkTopic);

LocalRunner localRunner = LocalRunner.builder().functionConfig(functionConfig).build();
localRunner.start(true);
```

This allows users to easily debug functions using an IDE. Users can set breakpoints and manually step through a function to debug with real data.

The following code snippet is a more complete example on how to programmatically launch a function in localrun mode.

```java
public class ExclamationFunction implements Function<String, String> {

   @Override
   public String process(String s, Context context) throws Exception {
       return s + "!";
   }


public static void main(String[] args) throws Exception {
    FunctionConfig functionConfig = new FunctionConfig();
    functionConfig.setName("exclamation");
    functionConfig.setInputs(Collections.singleton("input"));
    functionConfig.setClassName(ExclamationFunction.class.getName());
    functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
    functionConfig.setOutput("output");

    LocalRunner localRunner = LocalRunner.builder().functionConfig(functionConfig).build();
    localRunner.start(false);
}
```

To use localrun like above programmatically please addd the following dependency:

```xml
<dependency>
   <groupId>org.apache.pulsar</groupId>
   <artifactId>pulsar-functions-local-runner</artifactId>
   <version>${pulsar.version}</version>
</dependency>

```

For complete code samples, see [here](https://github.com/jerrypeng/pulsar-functions-demos/tree/master/debugging)

In the future, debugging with localrun mode for Pulsar Functions written in other languages will be supported.
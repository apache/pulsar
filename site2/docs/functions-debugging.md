---
id: functions-debugging
title: Debugging Pulsar Functions
sidebar_label: Debugging functions
---

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

The user can write a unit test for this function as well. Just be remember to mock out the ```Context``` parameter.

For example:

```java
@Test
public void testExclamationFunction() {
   ExclamationFunction exclamation = new ExclamationFunction();
   String output = exclamation.process("foo", mock(Context.class));
   Assert.assertEquals(output, "foo!");
}
```

## Debugging with localrun mode

> Please note that there is currenlty only support for this for Pulsar Functions written in Java.  Users will need a Pulsar version > 2.4.9 to do the following. Even though localrun is available in older version of Pulsar, it does not have the functionally to be executed programmatically and run functions as threads.

To test in a more realistic fashion, a Pulsar Function can be run via localrun mode which will launch a instance of the function on your local machine as a thread.

In this mode, the Pulsar Function can consume and produce actual data to a Pulsar Cluster and mirrors how the function will actually run in a Pulsar Cluster.

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

This allows users to easily debug functions using an IDE.  User's can set break points and manually set through a function to debug will real data.

The code snippet below illustrates a more complete example of how to use launch a function via localrun programmatically.

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

For complete code samples please visit [here](https://github.com/jerrypeng/pulsar-functions-demos/tree/master/debugging)

In the future, we will add native local run support for Pulsar Functions written in other languages.

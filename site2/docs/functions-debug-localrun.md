---
id: functions-debug-localrun
title: Debug with localrun mode
sidebar_label: "Debug with localrun mode"
---

In localrun mode, a function consumes and produces actual data to a Pulsar cluster, and mirrors how the function actually runs in a Pulsar cluster. This provides a way to test your function and allow you to launch a function instance on your local machine as a thread for easy debugging.

:::note

Debugging with localrun mode is only available for Java functions in Pulsar 2.4.0 or later versions.

:::

Before using localrun mode, you need to add the following dependency.

```xml

<dependency>
   <groupId>org.apache.pulsar</groupId>
   <artifactId>pulsar-functions-local-runner</artifactId>
   <version>${pulsar.version}</version>
</dependency>

```

For example, you can run your function in the following manner.

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

You can debug functions using an IDE. Set breakpoints and manually step through a function to debug with real data.

The following code example shows how to run a function in localrun mode.

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

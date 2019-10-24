---
id: version-2.4.0-functions-debugging
title: How to debug Pulsar Functions
sidebar_label: Debugging
original_id: functions-debugging
---

You can use the following methods to debug Pulsar Functions:

* [Use unit test](functions-debugging.md#use-unit-test)
* [Debug with localrun mode](functions-debugging.md#debug-with-localrun-mode)
* [Use log topic](functions-debugging.md#use-log-topic)
* [Use Functions CLI](functions-debugging.md#use-functions-cli)

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

For complete code samples, see [here](https://github.com/jerrypeng/pulsar-functions-demos/tree/master/debugging).

In the future, debugging with localrun mode for Pulsar Functions written in other languages will be supported.

## Use log topic

Pulsar Functions allow you to output the log information defined in functions to a specified log topic. Consumers can be configured to consume messages from a specified log topic to check the log information.

![Pulsar Functions core programming model](assets/pulsar-functions-overview.png)

**Example** 

```text
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class LoggingFunction implements Function<String, Void> {
    @Override
    public void apply(String input, Context context) {
        Logger LOG = context.getLogger();
        String messageId = new String(context.getMessageId());

        if (input.contains("danger")) {
            LOG.warn("A warning was received in message {}", messageId);
        } else {
            LOG.info("Message {} received\nContent: {}", messageId, input);
        }

        return null;
    }
}
```

As shown in the example above, you can get the logger via `context.getLogger()` and assign the logger to the `LOG` variable of `slf4j`, so you can define your desired log information in a function using the `LOG` variable. You also need to specify the topic to which the log information is produced.

**Example** 

```
$ bin/pulsar-admin functions create \
  --log-topic persistent://public/default/logging-function-logs \
  # Other function configs
```

## Use Functions CLI

The [Pulsar Functions CLI](reference-pulsar-admin.md#functions) helps you in debugging Pulsar Functions with the following subcommands:

* `get`
* `status`
* `stats`
* `list`
* `trigger`

> **Tip**
> 
> For complete commands of **Pulsar Functions CLI**, see [here](reference-pulsar-admin.md#functions)。

### `get`

Get information about a Pulsar Function.

**Usage**

```text
$ pulsar-admin functions get options
```

**Options**

|Flag|Description
|---|---
|`--fqfn`|The Fully Qualified Function Name (FQFN) of a Pulsar Function
|`--name`|The name of a Pulsar Function
|`--namespace`|The namespace of a Pulsar Function
|`--tenant`|The tenant of a Pulsar Function

> **Tip**
> 
> `--fqfn` consists of `--name`, `--namespace` and `--tenant`, that is, you can specify only `--fqfn` or specify `--name`, `--namespace` and `--tenant` instead.

**Example** 

You can specify `--fqfn` to get information about a Pulsar Function.

```text
$ ./bin/pulsar-admin functions get public/default/ExclamationFunctio6
```
Optionally, you can specify `--name`, `--namespace` and `--tenant` to get information about a Pulsar Function.

```text
$ ./bin/pulsar-admin functions get \
    --tenant public \
    --namespace default \
    --name ExclamationFunctio6
```

As shown below, the `get` command shows input, output, runtime, and other information about the _ExclamationFunctio6_ function.

```text
{
  "tenant": "public",
  "namespace": "default",
  "name": "ExclamationFunctio6",
  "className": "org.example.test.ExclamationFunction",
  "inputSpecs": {
    "persistent://public/default/my-topic-1": {
      "isRegexPattern": false
    }
  },
  "output": "persistent://public/default/test-1",
  "processingGuarantees": "ATLEAST_ONCE",
  "retainOrdering": false,
  "userConfig": {},
  "runtime": "JAVA",
  "autoAck": true,
  "parallelism": 1
}
```

### `status`

Check the current status of a Pulsar Function.

**Usage**

```text
$ pulsar-admin functions status options
```

**Options**

|Flag|Description
|---|---
|`--fqfn`|The Fully Qualified Function Name (FQFN) of a Pulsar Function
|`--instance-id`|The instance ID of a Pulsar Function <br>If the `--instance-id` is not specified, it will get the IDs of all instances<br>
|`--name`|The name of a Pulsar Function 
|`--namespace`|The namespace of a Pulsar Function
|`--tenant`|The tenant of a Pulsar Function

**Example** 

```text
$ ./bin/pulsar-admin functions status \
    --tenant public \
    --namespace default \
    --name ExclamationFunctio6 \
```

As shown below, the `status` command shows the number of instances, the number of running instances, the instance running under the _ExclamationFunctio6_ function, the number of received messages, the number of successfully processed messages, the number of system exceptions, the average latency and so on.

```text
{
  "numInstances" : 1,
  "numRunning" : 1,
  "instances" : [ {
    "instanceId" : 0,
    "status" : {
      "running" : true,
      "error" : "",
      "numRestarts" : 0,
      "numReceived" : 1,
      "numSuccessfullyProcessed" : 1,
      "numUserExceptions" : 0,
      "latestUserExceptions" : [ ],
      "numSystemExceptions" : 0,
      "latestSystemExceptions" : [ ],
      "averageLatency" : 0.8385,
      "lastInvocationTime" : 1557734137987,
      "workerId" : "c-standalone-fw-23ccc88ef29b-8080"
    }
  } ]
}
```

### `stats`

Get the current stats of a Pulsar Function.

**Usage**

```text
$ pulsar-admin functions stats options
```

**Options**

|Flag|Description
|---|---
|`--fqfn`|The Fully Qualified Function Name (FQFN) of a Pulsar Function
|`--instance-id`|The instance ID of a Pulsar Function <br>If the `--instance-id` is not specified, it will get the IDs of all instances<br>
|`--name`|The name of a Pulsar Function 
|`--namespace`|The namespace of a Pulsar Function
|`--tenant`|The tenant of a Pulsar Function

**Example**

```text
$ ./bin/pulsar-admin functions stats \
    --tenant public \
    --namespace default \
    --name ExclamationFunctio6 \
```

The output is as below:

```text
{
  "receivedTotal" : 1,
  "processedSuccessfullyTotal" : 1,
  "systemExceptionsTotal" : 0,
  "userExceptionsTotal" : 0,
  "avgProcessLatency" : 0.8385,
  "1min" : {
    "receivedTotal" : 0,
    "processedSuccessfullyTotal" : 0,
    "systemExceptionsTotal" : 0,
    "userExceptionsTotal" : 0,
    "avgProcessLatency" : null
  },
  "lastInvocation" : 1557734137987,
  "instances" : [ {
    "instanceId" : 0,
    "metrics" : {
      "receivedTotal" : 1,
      "processedSuccessfullyTotal" : 1,
      "systemExceptionsTotal" : 0,
      "userExceptionsTotal" : 0,
      "avgProcessLatency" : 0.8385,
      "1min" : {
        "receivedTotal" : 0,
        "processedSuccessfullyTotal" : 0,
        "systemExceptionsTotal" : 0,
        "userExceptionsTotal" : 0,
        "avgProcessLatency" : null
      },
      "lastInvocation" : 1557734137987,
      "userMetrics" : { }
    }
  } ]
}
```

### `list`

List all Pulsar Functions running under a specific tenant and namespace.

**Usage**

```text
$ pulsar-admin functions list options
```

**Options**

|Flag|Description
|---|---
|`--namespace`|The namespace of a Pulsar Function
|`--tenant`|The tenant of a Pulsar Function

**Example** 

```text
$ ./bin/pulsar-admin functions list \
    --tenant public \
    --namespace default
```
As shown below, the `list` command returns three functions running under the _public_ tenant and the _default_ namespace.

```text
ExclamationFunctio1
ExclamationFunctio2
ExclamationFunctio3
```

### `trigger`

Trigger a specified Pulsar Function with a supplied value. This command simulates the execution process of a Plusar Function and verifies it.

**Usage**

```text
$ pulsar-admin functions trigger options
```

**Options**

|Flag|Description
|---|---
|`--fqfn`|The Fully Qualified Function Name (FQFN) of a Pulsar Function
|`--name`|The name of a Pulsar Function
|`--namespace`|The namespace of a Pulsar Function
|`--tenant`|The tenant of a Pulsar Function
|`--topic`|The topic name that a Pulsar Function consumes from
|`--trigger-file`|The path to a file that contains the data to trigger a Pulsar Function
|`--trigger-value`|The value to trigger a Pulsar Function

**Example** 

```text
$ ./bin/pulsar-admin functions trigger \
    --tenant public \
    --namespace default \
    --name ExclamationFunctio6 \
    --topic persistent://public/default/my-topic-1 \
    --trigger-value "hello pulsar functions"
```

As shown below, the `trigger` command returns the following result:

```text
This is my function!
```

> #### **Note**
> You must specify the [entire topic name](getting-started-pulsar.md#topic-names) when using the `--topic` option. Otherwise, the following error is raised.
>
>```text
>Function in trigger function has unidentified topic
>
>Reason: Function in trigger function has unidentified topic
>```

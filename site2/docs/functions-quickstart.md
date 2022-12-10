---
id: functions-quickstart
title: Getting started with Pulsar Functions
sidebar_label: "Get started"
---

This hands-on tutorial provides step-by-step instructions and examples on how to create and validate functions in a [standalone Pulsar](getting-started-standalone.md), including stateful functions and window functions.

## Prerequisites

- JDK 8+. For more details, refer to [Pulsar runtime Java version recommendation](https://github.com/apache/pulsar#pulsar-runtime-java-version-recommendation).
- Windows OS is not supported.

## Start standalone Pulsar 

1. Enable pulsar function in `conf/standalone.conf` (add this field if not exists):

   ```properties
   functionsWorkerEnabled=true
   ```

2. Start Pulsar locally.

   ```bash
   bin/pulsar standalone
   ```

   All the components (including ZooKeeper, BookKeeper, broker, and so on) of a Pulsar service start in order. You can use the `bin/pulsar-admin brokers healthcheck` command to make sure the Pulsar service is up and running.

3. Check the Pulsar binary protocol port.

   ```bash
   telnet localhost 6650
   ```

4. Check the Pulsar Function cluster.

   ```bash
   bin/pulsar-admin functions-worker get-cluster
   ```

   **Output**

   ```json
   [{"workerId":"c-standalone-fw-localhost-6750","workerHostname":"localhost","port":6750}]
   ```

5. Make sure a public tenant exists.

   ```bash
   bin/pulsar-admin tenants list
   ```

   **Output**

   ```text
   public
   ```

6. Make sure a default namespace exists.

   ```bash
   bin/pulsar-admin namespaces list public
   ```

   **Output**

   ```text
   public/default
   ```

7. Make sure the table service is enabled successfully.

   ```bash
   telnet localhost 4181
   ```

   **Output**

   ```text
   Trying ::1...
   telnet: connect to address ::1: Connection refused
   Trying 127.0.0.1...
   Connected to localhost.
   Escape character is '^]'.
   ```

## Create a namespace for test

1. Create a tenant and a namespace.

   ```bash
   bin/pulsar-admin tenants create test
   bin/pulsar-admin namespaces create test/test-namespace
   ```

2. In the same terminal window as step 1, verify the tenant and the namespace.

   ```bash
   bin/pulsar-admin namespaces list test
   ```

   **Output**

   This output shows that both tenant and namespace are created successfully.

   ```text
   "test/test-namespace"
   ```

## Start functions

:::note

Before starting functions, you need to [start Pulsar](#start-standalone-pulsar) and [create a test namespace](#create-a-namespace-for-test).

:::

1. Create a function named `examples`.

   :::tip

   You can see both the `example-function-config.yaml` and `api-examples.jar` files under the `examples` folder of the Pulsar’s directory on your local machine.

   This example function will add a `!` at the end of every message.

   :::

   ```bash
   bin/pulsar-admin functions create \
      --function-config-file examples/example-function-config.yaml \
      --jar examples/api-examples.jar
   ```

   **Output**

   ```text
   Created Successfully
   ```
   
   You can check the config of this function in `examples/example-function-config.yaml`:

   ```yaml
   tenant: "test"
   namespace: "test-namespace"
   name: "example" # function name
   className: "org.apache.pulsar.functions.api.examples.ExclamationFunction"
   inputs: ["test_src"] # this function will read messages from these topics
   output: "test_result" # the return value of this function will be sent to this topic  
   autoAck: true # function will acknowledge input messages if set true 
   parallelism: 1
   ```

   You can see the [source code](https://github.com/apache/pulsar/blob/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/ExclamationFunction.java) of `ExclamationFunction`.
   For more information about the yaml config, see the [reference](functions-cli.md#yaml-configurations-for-pulsar-functions). 

2. In the same terminal window as step 1, verify the function's configurations.

   ```bash
   bin/pulsar-admin functions get \
      --tenant test \
      --namespace test-namespace \
      --name example
   ```

   **Output**

   ```json
   {
       "tenant": "test",
       "namespace": "test-namespace",
       "name": "example",
       "className": "org.apache.pulsar.functions.api.examples.ExclamationFunction",
       "inputSpecs": {
           "test_src": {
           "isRegexPattern": false,
           "schemaProperties": {},
           "consumerProperties": {},
           "poolMessages": false
           }
       },
       "output": "test_result",
       "producerConfig": {
           "useThreadLocalProducers": false,
           "batchBuilder": ""
       },
       "processingGuarantees": "ATLEAST_ONCE",
       "retainOrdering": false,
       "retainKeyOrdering": false,
       "forwardSourceMessageProperty": true,
       "userConfig": {},
       "runtime": "JAVA",
       "autoAck": true,
       "parallelism": 1,
       "resources": {
           "cpu": 1.0,
           "ram": 1073741824,
           "disk": 10737418240
       },
       "cleanupSubscription": true,
       "subscriptionPosition": "Latest"
   }
   ```

3. In the same terminal window as step 1, verify the function's status.

   ```bash
   bin/pulsar-admin functions status \
      --tenant test \
      --namespace test-namespace \
      --name example
   ```

   **Output**

   `"running": true` shows that the function is running.
    
   ```json
   {
     "numInstances" : 1,
     "numRunning" : 1,
     "instances" : [ {
       "instanceId" : 0,
       "status" : {
         "running" : true,
         "error" : "",
         "numRestarts" : 0,
         "numReceived" : 0,
         "numSuccessfullyProcessed" : 0,
         "numUserExceptions" : 0,
         "latestUserExceptions" : [ ],
         "numSystemExceptions" : 0,
         "latestSystemExceptions" : [ ],
         "averageLatency" : 0.0,
         "lastInvocationTime" : 0,
         "workerId" : "c-standalone-fw-localhost-8080"
       }
     } ]
   }
   ```

4. In the same terminal window as step 1, subscribe to the **output topic** `test_result`.

   ```bash
   bin/pulsar-client consume -s test-sub -n 0 test_result
   ```

5. In a new terminal window, produce messages to the **input topic** `test_src`.

   ```bash
   bin/pulsar-client produce -m "test-messages-`date`" -n 10 test_src
   ```

6. In the same terminal window as step 1, the messages produced by the `example` function are returned. You can see there is a `!` added at the end of every message.

   **Output**

   ```text
   ----- got message -----
   test-messages-Thu Jul 19 11:59:15 PDT 2021!
   ----- got message -----
   test-messages-Thu Jul 19 11:59:15 PDT 2021!
   ----- got message -----
   test-messages-Thu Jul 19 11:59:15 PDT 2021!
   ----- got message -----
   test-messages-Thu Jul 19 11:59:15 PDT 2021!
   ----- got message -----
   test-messages-Thu Jul 19 11:59:15 PDT 2021!
   ----- got message -----
   test-messages-Thu Jul 19 11:59:15 PDT 2021!
   ----- got message -----
   test-messages-Thu Jul 19 11:59:15 PDT 2021!
   ----- got message -----
   test-messages-Thu Jul 19 11:59:15 PDT 2021!
   ----- got message -----
   test-messages-Thu Jul 19 11:59:15 PDT 2021!
   ----- got message -----
   test-messages-Thu Jul 19 11:59:15 PDT 2021!
   ```

## Start stateful functions

The standalone mode of Pulsar enables BookKeeper table service for stateful functions. For more information, see [Configure state storage](functions-develop-state.md).

The following example provides instructions to validate counter functions.

:::note

Before starting stateful functions, you need to [start Pulsar](#start-standalone-pulsar) and [create a test namespace](#create-a-namespace-for-test).

:::

1. Create a function using `examples/example-stateful-function-config.yaml`.

   ```bash
   bin/pulsar-admin functions create \
      --function-config-file examples/example-stateful-function-config.yaml \
      --jar examples/api-examples.jar
   ```

   **Output**

   ```text
   Created Successfully
   ```

   You can check the config of this function in `examples/example-stateful-function-config.yaml`:

   ```yaml
   tenant: "test"
   namespace: "test-namespace"
   name: "word_count"
   className: "org.apache.pulsar.functions.api.examples.WordCountFunction"
   inputs: ["test_wordcount_src"] # this function will read messages from these topics
   autoAck: true
   parallelism: 1
   ```
   
   You can see the [source code](https://github.com/apache/pulsar/blob/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/WordCountFunction.java) of `WordCountFunction`. This function won't return any value but store the occurrence of words in function context. So you don't need to specify an output topic.
   For more information about the yaml config, see the [reference](functions-cli.md#yaml-configurations-for-pulsar-functions).

2. In the same terminal window as step 1, get the information of the `word_count` function.

   ```bash
   bin/pulsar-admin functions get \
      --tenant test \
      --namespace test-namespace \
      --name word_count
   ```

   **Output**

   ```json
   {
     "tenant": "test",
     "namespace": "test-namespace",
     "name": "word_count",
     "className": "org.apache.pulsar.functions.api.examples.WordCountFunction",
     "inputSpecs": {
       "test_wordcount_src": {
         "isRegexPattern": false,
         "schemaProperties": {},
         "consumerProperties": {},
         "poolMessages": false
       }
     },
     "producerConfig": {
       "useThreadLocalProducers": false,
       "batchBuilder": ""
     },
     "processingGuarantees": "ATLEAST_ONCE",
     "retainOrdering": false,
     "retainKeyOrdering": false,
     "forwardSourceMessageProperty": true,
     "userConfig": {},
     "runtime": "JAVA",
     "autoAck": true,
     "parallelism": 1,
     "resources": {
       "cpu": 1.0,
       "ram": 1073741824,
       "disk": 10737418240
     },
     "cleanupSubscription": true,
     "subscriptionPosition": "Latest"
   }
   ```

3. In the same terminal window as step 1, get the status of the `word_count` function.

   ```bash
   bin/pulsar-admin functions status \
      --tenant test \
      --namespace test-namespace \
      --name word_count
   ```

   **Output**

   ```json
   {
     "numInstances" : 1,
     "numRunning" : 1,
     "instances" : [ {
       "instanceId" : 0,
       "status" : {
         "running" : true,
          "error" : "",
          "numRestarts" : 0,
          "numReceived" : 0,
          "numSuccessfullyProcessed" : 0,
          "numUserExceptions" : 0,
          "latestUserExceptions" : [ ],
          "numSystemExceptions" : 0,
          "latestSystemExceptions" : [ ],
          "averageLatency" : 0.0,
          "lastInvocationTime" : 0,
          "workerId" : "c-standalone-fw-localhost-8080"
       }
     } ]
   }
   ```

4. In the same terminal window as step 1, query the state table for the function with the key `hello`. This operation watches the changes associated with `hello`.

   ```bash
   bin/pulsar-admin functions querystate \
      --tenant test \
      --namespace test-namespace \
      --name word_count -k hello -w
   ```

   :::tip
    
   For more information about the `pulsar-admin functions querystate options` command, including flags, descriptions, default values, and shorthands, see [Admin API](/tools/pulsar-admin/).

   :::

   **Output**

   ```text
   key 'hello' doesn't exist.
   key 'hello' doesn't exist.
   key 'hello' doesn't exist.
   ...
   ```

5. In a new terminal window, produce 10 messages with `hello` to the **input topic** `test_wordcount_src` using one of the following methods. The value of `hello` is updated to 10.

  ```bash
  bin/pulsar-client produce -m "hello" -n 10 test_wordcount_src
  ```

6. In the same terminal window as step 1, check the result. 

   The result shows that the **output topic** `test_wordcount_dest` receives the messages.

   **Output**

   ```json
   {
     "key": "hello",
     "numberValue": 10,
     "version": 9
   }
   ```

7. In the terminal window as step 5, produce another 10 messages with `hello`. The value of `hello` is updated to 20.

   ```bash
   bin/pulsar-client produce -m "hello" -n 10 test_wordcount_src
   ```

8. In the same terminal window as step 1, check the result. 

   The result shows that the **output topic** `test_wordcount_dest` receives the value of 20.

   ```json
   {
      "key": "hello",
      "numberValue": 20,
      "version": 19
   }
   ```

## Start window functions

Window functions are a special form of Pulsar Functions. For more information, see [concepts](functions-concepts.md#window-function).

The following example provides instructions to start a window function to calculate the sum in a window.

:::note

Before starting window functions, you need to [start Pulsar](#start-standalone-pulsar)  and [create a test namespace](#create-a-namespace-for-test).

:::

1. Create a function using `example-window-function-config.yaml`.

   ```bash
   bin/pulsar-admin functions create \
      --function-config-file examples/example-window-function-config.yaml \
      --jar examples/api-examples.jar
   ```

   **Output**

   ```text
   Created Successfully
   ```

   You can check the config of this function in `examples/example-window-function-config.yaml`:

   ```yaml
   tenant: "test"
   namespace: "test-namespace"
   name: "window-example"
   className: "org.apache.pulsar.functions.api.examples.AddWindowFunction"
   inputs: ["test_window_src"]
   output: "test_window_result"
   autoAck: true
   parallelism: 1
   
   # every 5 messages, calculate sum of the latest 10 messages
   windowConfig:
     windowLengthCount: 10
     slidingIntervalCount: 5
   ```

   You can see the source code of `AddWindowFunction` [here](https://github.com/apache/pulsar/blob/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/AddWindowFunction.java).
   For more information about the yaml config, see the [reference](functions-cli.md#yaml-configurations-for-pulsar-functions).


2. In the same terminal window as step 1, verify the function's configurations.

   ```bash
   bin/pulsar-admin functions get \
      --tenant test \
      --namespace test-namespace \
      --name window-example
   ```

   **Output**

   ```json
   {
     "tenant": "test",
     "namespace": "test-namespace",
     "name": "window-example",
     "className": "org.apache.pulsar.functions.api.examples.AddWindowFunction",
     "inputSpecs": {
       "test_window_src": {
         "isRegexPattern": false,
         "schemaProperties": {},
         "consumerProperties": {},
         "poolMessages": false
       }
     },
     "output": "test_window_result",
     "producerConfig": {
       "useThreadLocalProducers": false,
       "batchBuilder": ""
     },
     "processingGuarantees": "ATLEAST_ONCE",
     "retainOrdering": false,
     "retainKeyOrdering": false,
     "forwardSourceMessageProperty": true,
     "userConfig": {},
     "runtime": "JAVA",
     "autoAck": false,
     "parallelism": 1,
     "resources": {
       "cpu": 1.0,
       "ram": 1073741824,
       "disk": 10737418240
     },
     "windowConfig": {
       "windowLengthCount": 10,
       "slidingIntervalCount": 5,
       "actualWindowFunctionClassName": "org.apache.pulsar.functions.api.examples.AddWindowFunction",
       "processingGuarantees": "ATLEAST_ONCE"
     },
     "cleanupSubscription": true,
     "subscriptionPosition": "Latest"
   }
   ```

3. In the same terminal window as step 1, verify the function’s status.

   ```bash
   bin/pulsar-admin functions status \
      --tenant test \
      --namespace test-namespace \
      --name window-example
   ```

   **Output**

   `"running": true` shows that the function is running.
    
   ```json
   {
     "numInstances" : 1,
     "numRunning" : 1,
     "instances" : [ {
       "instanceId" : 0,
       "status" : {
         "running" : true,
         "error" : "",
         "numRestarts" : 0,
         "numReceived" : 0,
         "numSuccessfullyProcessed" : 0,
         "numUserExceptions" : 0,
         "latestUserExceptions" : [ ],
         "numSystemExceptions" : 0,
         "latestSystemExceptions" : [ ],
         "averageLatency" : 0.0,
         "lastInvocationTime" : 0,
         "workerId" : "c-standalone-fw-localhost-8080"
       }
     } ]
   }
   ```

4. In the same terminal window as step 1, subscribe to the **output topic** `test_window_result`.

   ```bash
   bin/pulsar-client consume -s test-sub -n 0 test_window_result
   ```

5. In a new terminal window, produce messages to the **input topic** `test_window_src`.

   ```bash
   bin/pulsar-client produce -m "3" -n 10 test_window_src
   ```

6. In the same terminal window as step 1, the messages produced by the window function `window-example` are returned. 

   **Output**

   ```text
   ----- got message -----
   key:[null], properties:[], content:15
   ----- got message -----
   key:[null], properties:[], content:30
   ```
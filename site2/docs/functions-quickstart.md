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

1. Start Pulsar locally.

   ```bash

   bin/pulsar standalone

   ```

   All the components (including ZooKeeper, BookKeeper, broker, and so on) of a Pulsar service start in order. You can use the `bin/pulsar-admin brokers healthcheck` command to make sure the Pulsar service is up and running.

2. Check the Pulsar binary protocol port.

   ```bash

   telnet localhost 6650

   ```

3. Check the Pulsar Function cluster.

   ```bash

   bin/pulsar-admin functions-worker get-cluster

   ```

   **Output**

   ```json

   [{"workerId":"c-standalone-fw-localhost-6750","workerHostname":"localhost","port":6750}]

   ```

4. Make sure a public tenant exists.

   ```bash
   
   bin/pulsar-admin tenants list

   ```

   **Output**

   ```json
   
   "public"

   ```

5. Make sure a default namespace exists.

   ```bash

   bin/pulsar-admin namespaces list public

   ```

   **Output**

   ```json

   "public/default"

   ```

6. Make sure the table service is enabled successfully.

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

## Start functions

:::note

Before starting functions, you need to [start Pulsar](#start-standalone-pulsar).

:::

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

3. In the same terminal window as step 1, create a function named `examples`.

   :::tip

   You can see both the `example-function-config.yaml` and `api-examples.jar` files under the `examples` folder of the Pulsar’s directory on your local machine.

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

4. In the same terminal window as step 1, verify the function's configurations.

   ```bash

   bin/pulsar-admin functions get \
   --tenant test \
   --namespace test-namespace \
   --name example

   ```

   **Output**

   ```text

   {
     "tenant": "test",
     "namespace": "test-namespace",
     "name": "example",
     "className": "org.apache.pulsar.functions.api.examples.ExclamationFunction",
     "userConfig": "{\"PublishTopic\":\"test_result\"}",
     "autoAck": true,
     "parallelism": 1,
     "source": {
       "topicsToSerDeClassName": {
         "test_src": ""
       },
       "typeClassName": "java.lang.String"
     },
     "sink": {
       "topic": "test_result",
       "typeClassName": "java.lang.String"
     },
     "resources": {}
   }

   ```

5. In the same terminal window as step 1, verify the function's status.

   ```bash

   bin/pulsar-admin functions status \
   --tenant test \
   --namespace test-namespace \
   --name example

   ```

   **Output**

   `"running": true` shows that the function is running.
    
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

6. In the same terminal window as step 1, subscribe to the **output topic** `test_result`.

   ```bash

   bin/pulsar-client consume -s test-sub -n 0 test_result

   ```

7. In a new terminal window, produce messages to the **input topic** `test_src`.

   ```bash

   bin/pulsar-client produce -m "test-messages-`date`" -n 10 test_src

   ```

8.  In the same terminal window as step 1, the messages produced by the `example` function are returned. 

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

Before starting stateful functions, you need to [start Pulsar](#start-standalone-pulsar).

:::

1. Create a function named `word_count`.

   ```bash

   bin/pulsar-admin functions create \
   --function-config-file examples/example-function-config.yaml \
   --jar examples/api-examples.jar \
   --name word_count \
   --className org.apache.pulsar.functions.api.examples.WordCountFunction \
   --inputs test_wordcount_src \
   --output test_wordcount_dest

   ```

   **Output**

   ```text

   Created Successfully

   ```

2. In the same terminal window as step 1, get the information of the `word_count` function.

   ```bash

   bin/pulsar-admin functions get \
   --tenant test \
   --namespace test-namespace \
   --name word_count

   ```

   **Output**

   ```text

   {
     "tenant": "test",
     "namespace": "test-namespace",
     "name": "word_count",
     "className": "org.apache.pulsar.functions.api.examples.WordCountFunction",
     "inputSpecs": {
       "test_wordcount_src": {
         "isRegexPattern": false
       }
     },
     "output": "test_wordcount_dest",
     "processingGuarantees": "ATLEAST_ONCE",
     "retainOrdering": false,
     "userConfig": {
       "PublishTopic": "test_result"
     },
     "runtime": "JAVA",
     "autoAck": true,
     "parallelism": 1,
     "resources": {
       "cpu": 1.0,
       "ram": 1073741824,
       "disk": 10737418240
     },
     "cleanupSubscription": true
   }

   ```

3. In the same terminal window as step 1, get the status of the `word_count` function.

   ```bash

   bin/pulsar-admin functions status \
   --tenant test \
   --namespace test-namespace\
   --name word_count

   ```

   **Output**

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

   * **Method 1**

     ```bash

     bin/pulsar-client produce -m "hello" -n 10 test_wordcount_src

     ```

   * **Method 2**

     ```bash

     bin/pulsar-admin functions putstate \
     --tenant test \
     --namespace test-namespace \
     --name word_count hello-word \

     ```

     :::tip
      
     For more information about the `pulsar-admin functions putstate options` command, including flags, descriptions, default values, and shorthands, see [Admin API](/tools/pulsar-admin/).
    
     :::

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

   ```text

   value = 10
   value = 20

   ```

## Start window functions

Window functions are a special form of Pulsar Functions. For more information, see [concepts](functions-concepts.md#window-function).

:::note

Before starting window functions, you need to [start Pulsar](#start-standalone-pulsar).

:::

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

3. In the same terminal window as step 1, create a function named `example`.

   :::tip

   You can see both `example-window-function-config.yaml` and `api-examples.jar` files under the `examples` folder of the Pulsar’s directory on your local machine.

   :::

   ```bash

   bin/pulsar-admin functions create --function-config-file \
   examples/example-window-function-config.yaml \
   --jar examples/api-examples.jar

   ```

   **Output**

   ```text

   Created Successfully

   ```

4. In the same terminal window as step 1, verify the function's configurations.

   ```bash

   bin/pulsar-admin functions get \
   --tenant test \
   --namespace test-namespace \
   --name example

   ```

   **Output**

   ```text

   {
     "tenant": "test",
     "namespace": "test-namespace",
     "name": "example",
     "className": "org.apache.pulsar.functions.api.examples.ExclamationFunction",
     "userConfig": "{\"PublishTopic\":\"test_result\"}",
     "autoAck": true,
     "parallelism": 1,
     "source": {
       "topicsToSerDeClassName": {
         "test_src": ""
       },
       "typeClassName": "java.lang.String"
     },
     "sink": {
       "topic": "test_result",
       "typeClassName": "java.lang.String"
     },
     "resources": {}
   }

   ```

5. In the same terminal window as step 1, verify the function’s status.

   ```bash

   bin/pulsar-admin functions status \
   --tenant test \
   --namespace test-namespace \
   --name example

   ```

   **Output**

   `"running": true` shows that the function is running.
    
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

6. In the same terminal window as step 1, subscribe to the **output topic** `test_result`.

   ```bash

   bin/pulsar-client consume -s test-sub -n 0 test_result

   ```

7. In a new terminal window, produce messages to the **input topic** `test_src`.

   ```bash

   bin/pulsar-client produce -m "test-messages-`date`" -n 10 test_src

   ```

8. In the same terminal window as step 1, the messages produced by the window function `example` are returned. 

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
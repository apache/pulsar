---
id: functions-debug-cli
title: Debug with Functions CLI
sidebar_label: "Debug with Functions CLI"
---

With [Pulsar Functions CLI](/tools/pulsar-admin/), you can debug Pulsar Functions with the following subcommands:
* `get`
* `status`
* `stats`
* `list`
* `trigger`

## `get`

To get information about a function, you can specify `--fqfn` as follows.

```bash

 ./bin/pulsar-admin functions get public/default/ExclamationFunctio6

```

Alternatively, you can specify `--name`, `--namespace` and `--tenant` as follows.

```bash

 ./bin/pulsar-admin functions get \
    --tenant public \
    --namespace default \
    --name ExclamationFunctio6

```

As shown below, the `get` command shows input, output, runtime, and other information about the `_ExclamationFunctio6_` function.

```json

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

## `list`

To list all Pulsar Functions running under a specific tenant and namespace:

```bash

bin/pulsar-admin functions list \
    --tenant public \
    --namespace default

```

As shown below, the `list` command returns three functions running under the `public` tenant and the `default` namespace.

```text

ExclamationFunctio1
ExclamationFunctio2
ExclamationFunctio3

```

## `status`

To check the current status of a function:

```bash

 ./bin/pulsar-admin functions status \
    --tenant public \
    --namespace default \
    --name ExclamationFunctio6 \

```

As shown below, the `status` command shows the number of instances, running instances, the instance running under the `_ExclamationFunctio6_` function, received messages, successfully processed messages, system exceptions, the average latency and so on.

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

## `stats`

To get the current stats of a function:

```bash

bin/pulsar-admin functions stats \
    --tenant public \
    --namespace default \
    --name ExclamationFunctio6 \

```

The output is shown as follows:

```json

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

## `trigger`

To trigger a specified function with a supplied value:

```bash

 ./bin/pulsar-admin functions trigger \
    --tenant public \
    --namespace default \
    --name ExclamationFunctio6 \
    --topic persistent://public/default/my-topic-1 \
    --trigger-value "hello pulsar functions"

```

This command simulates the execution process of a function and verifies it. As shown below, the `trigger` command returns the following result:

```text

This is my function!

```

:::note

When using the `--topic` option, you must specify the [entire topic name](getting-started-pulsar.md#topic-names). Otherwise, the following error occurs.

  ```text

  Function in trigger function has unidentified topic

  Reason: Function in trigger function has unidentified topic

  ```

:::

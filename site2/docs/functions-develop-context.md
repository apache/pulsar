---
id: functions-develop-context
title: Use context object
sidebar_label: "Use context object"
---

Java, Python, and Go SDKs provide access to a **context object** that can be used by a function. This context object provides a wide variety of information and functionality to the function including:
* The name and ID of a function.
* The message ID of a message. Each message is automatically assigned with an ID.
* The key, event time, properties, and partition key of a message.
* The name of the topic that a message is sent to.
* The names of all input topics as well as the output topic associated with the function.
* The name of the class used for [SerDe](#functions-develop-serde).
* The tenant and namespace associated with the function.
* The ID of the function instance running the function.
* The version of the function.
* The [logger object](functions-develop-context-logs) used by the function, which is used to create log messages.
* Access to arbitrary [user configuration](functions-develop-context-user-defined-configs) values supplied via the CLI.
* An interface for recording [metrics](functions-develop-metrics).
* An interface for storing and retrieving state in [state storage](#functions-develop-state).
* A function to publish new messages onto arbitrary topics.
* A function to acknowledge the message being processed (if auto-ack is disabled).
* (Java) get Pulsar admin client.

:::tip

For more information about code examples, refer to [Java](https://github.com/apache/pulsar/blob/master/pulsar-functions/api-java/src/main/java/org/apache/pulsar/functions/api/BaseContext.java), [Python](https://github.com/apache/pulsar/blob/master/pulsar-functions/instance/src/main/python/contextimpl.py) and [Go](https://github.com/apache/pulsar/blob/master/pulsar-function-go/pf/context.go).

:::


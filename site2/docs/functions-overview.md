---
id: functions-overview
title: Pulsar Functions overview
sidebar_label: "Overview"
---

This section introduces the following content:
* [What is Pulsar Functions](#what-is-pulsar-functions)
* [Why use Puslar Functions](#why-use-pulsar-functions)
* [Use cases](#use-cases)
* [User flow](#user-flow)


## What are Pulsar Functions

Pulsar Functions are a serverless computing framework that runs on top of Pulsar and processes messages in the following way:
* consumes messages from one or more topics,
* applies a user-defined processing logic to the messages,
* publishes the outputs of the messages to other topics.

The following figure illustrates the computing process of a function. 

![Pulsar Functions execute user-defined code on data published to Pulsar topics](/assets/function-overview.svg)

A function receives messages from one or more **input topics**. Each time messages are received, the function completes the following steps:
1. Consumes the messages in the input topics.
2. Applies a customized processing logic to the messages and:
    a) writes output messages to an **output topic** in Pulsar
    b) writes logs to a **log topic** if it is configured (for debugging purposes)
    c) writes [state](functions-develop-state.md) to BookKeeper (if it is configured) 


You can write functions in Java, Python, and Go. For example, you can use Pulsar Functions to set up the following processing chain:
* A Python function listens for the `raw-sentences` topic and "sanitizes" incoming strings (removing extraneous white space and converting all characters to lowercase) and then publishes the results to a `sanitized-sentences` topic.
* A Java function listens for the `sanitized-sentences` topic, counts the number of times each word appears within a specified time [window](functions-concepts.md#window-function), and publishes the results to a `results` topic.
* A Python function listens for the `results` topic and writes the results to a MySQL table.

See [Develop Pulsar Functions](functions-develop.md) for more details.


## Why use Pulsar Functions

Pulsar Functions provide the capabilities to perform simple computations on the messages before they are routed to consumers. 

Pulsar Functions can be characterized as Lambda-style functions that are specifically designed and integrated with Pulsar as the underlying message bus. The framework of Pulsar Functions provides a simple computing framework on your Pulsar cluster and takes care of the underlying details of sending/receiving messages. You only need to focus on the business logic and run it as Pulsar Functions to maximize the value of your data and enjoy the benefits of:
* Simplified deployment and operations - you can create a data pipeline without deploying a separate Stream Processing Engine (SPE), such as [Apache Storm](http://storm.apache.org/), [Apache Heron](https://heron.incubator.apache.org/), or [Apache Flink](https://flink.apache.org/).
* Serverless computing (when Kubernetes runtime is used)
* Maximized developer productivity (both language-native interfaces and SDKs for Java/Python/Go).
* Easy troubleshooting


## Use cases

Here are two real-world use cases to help you understand the capabilities of Pulsar Functions and what they can be used for.

### Word count example

This figure illustrates the process of implementing the classic word count example using Pulsar Functions. It calculates a sum of the occurrences of every individual word published to a given topic.

![Word count example using Pulsar Functions](/assets/pulsar-functions-word-count.png)

### Content-based routing example

For example, a function takes items (strings) as input and publishes them to either a `fruits` or `vegetables` topic, depending on the item. If an item is neither fruit nor vegetable, a warning is logged to a [log topic](functions-develop-log.md).

This figure demonstrates the process of implementing a content-based routing using Pulsar Functions. 

![Count-based routing example using Pulsar Functions](/assets/pulsar-functions-routing-example.png)

## User flow

**Admins/operators**
1. [Set up function workers](functions-worker.md).
2. [Configure function runtime](functions-runtime.md). 
3. [Deploy a function](functions-deploy.md).

**Developers**
1. [Develop a function](functions-develop.md).
2. [Debug a function](functions-debug.md).
3. [Package a function](functions-package.md).
4. [Deploy a function](functions-deploy.md).

**More reference**
* [Function concepts](functions-concepts.md)
* [Function CLIs and configs](functions-cli.md)


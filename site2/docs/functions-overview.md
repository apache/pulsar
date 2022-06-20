---
id: functions-overview
title: Pulsar Functions overview
sidebar_label: "Overview"
---

This section introduces the following content:
* [What are Pulsar Functions](#what-is-pulsar-functions)
* [Why use Puslar Functions](#why-use-pulsar-functions)
* [Use cases](#use-cases)
* [User flow](#user-flow)


## What are Pulsar Functions
Pulsar functions are a serverless computing framework that runs on top of Pulsar. You can use this simple API and execution framework to implement common use cases, such as filtering and routing. Once you write a Pulsar function and submit it to the Pulsar cluster, it is up and running immediately.

The diagram below illustrates the three steps in the functions computing process. 

![Pulsar Functions execute user-defined code on data published to Pulsar topics](/assets/function-overview.svg)

Each time a function receives a message, it completes the following consume-apply-publish steps.
1. Consumes the message from one or more **input topics**. 
2. Applies the customized (user-supplied) processing logic to the message.
3. Publishes the output of the message to other topics and writes:
    a) output messages to an **output topic** in Pulsar
    b) logs to a **log topic** (if it is configured)for debugging
    c) [state](functions-develop-state) updates to BookKeeper (if it is configured) 

You can write functions in Java, Python, and Go. For example, you can use Pulsar Functions to set up the following processing chain:
* A Python function listens for the `raw-sentences` topic and "sanitizes" incoming strings (removing extraneous white space and converting all characters to lowercase) and then publishes the results to a `sanitized-sentences` topic.
* A Java function listens for the `sanitized-sentences` topic, counts the number of times each word appears within a specified time [window](functions-concepts.md#window-function), and publishes the results to a `results` topic.
* A Python function listens for the `results` topic and writes the results to a MySQL table.

See [Develop Pulsar Functions](functions-develop.md) for more details.


## Why use Pulsar Functions

Pulsar Functions perform simple computations on messages before routing the messages to consumers. These Lambda-style functions are specifically designed and integrated with Pulsar. The framework provides a simple computing framework on your Pulsar cluster and takes care of the underlying details of sending and receiving messages. You only need to focus on the business logic.

Pulsar Functions enable your organization to maximize the value of your data and enjoy the benefits of:
* Simplified deployment and operations - you can create a data pipeline without deploying a separate Stream Processing Engine (SPE), such as [Apache Storm](http://storm.apache.org/), [Apache Heron](https://heron.incubator.apache.org/), or [Apache Flink](https://flink.apache.org/).
* Serverless computing (when you use Kubernetes runtime)
* Maximized developer productivity (both language-native interfaces and SDKs for Java/Python/Go).
* Easy troubleshooting

## Use cases

Below are two simple examples of use cases for Pulsar Functions.

### Word count example

This figure shows the process of implementing the classic word count use case. 

![Word count example using Pulsar Functions](/assets/pulsar-functions-word-count.png)

In this example, the function calculates a sum of the occurrences of every individual word published to a given topic.

### Content-based routing example

This figure demonstrates the process of implementing a content-based routing use case. 

![Count-based routing example using Pulsar Functions](/assets/pulsar-functions-routing-example.png)

In this example, the function takes items (strings) as input and publishes them to either a `fruits` or `vegetables` topic, depending on the item. If an item is neither fruit nor vegetable, a warning is logged to a [log topic](functions-develop-log.md).

## Where do I go next?

* [Function concepts](functions-concepts.md)
* [Function CLIs and configs](functions-cli.md)

**For Developers**
1. [Develop a function](functions-develop.md).
2. [Debug a function](functions-debug.md).
3. [Package a function](functions-package.md).
4. [Deploy a function](functions-deploy.md).

**For Adminstrators and Operators**
1. [Set up function workers](functions-worker.md).
2. [Configure function runtime](functions-runtime.md). 
3. [Deploy a function](functions-deploy.md).



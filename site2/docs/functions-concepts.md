---
id: functions-concepts
title: Pulsar Functions concepts
sidebar_label: "Concepts"
---


## Fully Qualified Function Name

Each function has a Fully Qualified Function Name (FQFN) with a specified tenant, namespace, and function name. With FQFN, you can create multiple functions in different namespaces with the same function name. 

An FQFN looks like this:

```text

tenant/namespace/name

```

## Function instance

Function instance is the core element of the function execution framework, consisting of the following elements:
* A collection of consumers consuming messages from different input topics. 
* An executor that invokes the function.
* A producer that sends the result of a function to an output topic.

The following figure illustrates the internal workflow of a function instance.

![Function instance](/assets/function-instance.svg)

A function can have multiple instances, and each instance executes one copy of a function. You can specify the number of instances in the configuration file. 

The consumers inside a function instance use FQFN as subscriber names to enable load balancing between multiple instances based on subscription types. The subscription type can be specified at the function level.

Each function has a separate state store with FQFN. You can specify a state interface to persist intermediate results in the BookKeeper. Other users can query the state of the function and extract these results. 


## Function worker

Function worker is a logic component to monitor, orchestrate, and execute individual function in [cluster-mode](functions-deploy.md#depoy-a-function-in-cluster-mode) deployment of Pulsar Functions. 

Within function workers, each [function instance](#function-instance) can be executed as a thread or process, depending on the selected configurations. Alternatively, if a Kubernetes cluster is available, functions can be spawned as StatefulSets within Kubernetes. See [Set up function workers](functions-worker.md) for more details.

The following figure illustrates the internal architecture and workflow of function workers.

![Function worker workflow](/assets/function-worker-workflow.svg)

Function workers form a cluster of worker nodes and the workflow is described as follows.
1. User sends a request to the REST server to execute a function instance.
2. The REST server responds to the request and passes the request to the function metadata manager.
3. The function metadata manager writes the request updates to the function metadata topic. It also keeps track of all the metadata-related messages and uses the function metadata topic to persist the state updates of functions. 
4. The function metadata manager reads updates from the function metadata topic and triggers the schedule manager to compute an assignment.
5. The schedule manager writes the assignment updates to the assignment topic.
6. The function runtime manager listens to the assignment topic, reads the assignment updates, and updates its internal state that contains a global view of all assignments for all workers. If the update changes the assignment on a worker, the function runtime manager materializes the new assignment by starting or stopping the execution of function instances.
7. The membership manager requests the coordination topic to elect a lead worker. All workers subscribe to the coordination topic in a failover subscription, but the active worker becomes the leader and performs the assignment, guaranteeing only one active consumer for this topic. 
8. The membership manager reads updates from the coordination topic.


## Function runtime

A [function instance](#function-instance) is invoked inside a runtime, and a number of instances can run in parallel. Pulsar supports three types of function runtime with different costs and isolation guarantees to maximize deployment flexibility. You can use one of them to run functions based on your needs. See [Configure function runtime](functions-runtime.md) for more details.
 
The following table outlines the three types of function runtime.

| Type   | Description     |
|--------|-----------------|
| Thread runtime     | Each instance runs as a thread.<br /><br />Since the code for thread mode is written in Java, it is **only** applicable to Java instances. When a function runs in thread mode, it runs on the same Java virtual machine (JVM) with a function worker. |
| Process runtime    | Each instance runs as a process.<br /><br />When a function runs in process mode, it runs on the same machine that the function worker runs.|
| Kubernetes runtime | Function is submitted as Kubernetes StatefulSet by workers and each function instance runs as a pod. Pulsar supports adding labels to the Kubernetes StatefulSets and services while launching functions, which facilitates selecting the target Kubernetes objects. |


## Processing guarantees and subscription types

Pulsar provides three different messaging delivery semantics that you can apply to a function. 

| Delivery semantics | Description | Adopted subscription type |
|--------------------|-------------|---------------------------|
| **At-most-once** delivery | Each message sent to a function is processed at its best effort. There’s no guarantee that the message will be processed or not. | Shared |
| **At-least-once** delivery (default) | Each message sent to the function can be processed more than once (in case of a processing failure or redelivery).<br /><br />If you create a function without specifying the `--processing-guarantees` flag, the function provides `at-least-once` delivery guarantee. | Shared |
| **Effectively-once** delivery | Each message sent to the function can be processed more than once but it has only one output. Duplicated messages are ignored.<br /><br />`Effectively once` is achieved on top of `at-least-once` processing and guaranteed server-side deduplication. This means a state update can happen twice, but the same state update is only applied once, the other duplicated state update is discarded on the server-side. | Failover |

:::tip

* By default, Pulsar Functions provide `at-least-once` delivery guarantees. If you create a function without supplying a value for the `--processingGuarantees` flag, the function provides `at-least-once` guarantees.
* The `Exclusive` subscription type is **not** available in Pulsar Functions because:
  * If there is only one instance, `exclusive` equals `failover`.
  * If there are multiple instances, `exclusive` may crash and restart when functions restart. In this case, `exclusive` does not equal `failover`. Because when the master consumer disconnects, all non-acknowledged and subsequent messages are delivered to the next consumer in line.
* To change the subscription type from `shared` to `key_shared`, you can use the `—retain-key-ordering` option in [`pulsar-admin`](/tools/pulsar-admin/).

:::

You can set the processing guarantees for a function when you create the function. The following command creates a function with effectively-once guarantees applied.

```bash

bin/pulsar-admin functions create \
  --name my-effectively-once-function \
  --processing-guarantees EFFECTIVELY_ONCE \
  # Other function configs

```

You can change the processing guarantees applied to a function using the `update` command. 

```bash

bin/pulsar-admin functions update \
  --processing-guarantees ATMOST_ONCE \
  # Other function configs

```

## Context

Java, Python, and Go SDKs provide access to a **context object** that can be used by a function. This context object provides a wide variety of information and functionality to the function including:
* The name and ID of a function.
* The message ID of a message. Each message is automatically assigned with an ID.
* The key, event time, properties, and partition key of a message.
* The name of the topic that a message is sent to.
* The names of all input topics as well as the output topic associated with the function.
* The name of the class used for [SerDe](functions-develop-serde).
* The tenant and namespace associated with the function.
* The ID of the function instance running the function.
* The version of the function.
* The [logger object](functions-develop-log) used by the function, which is used to create log messages.
* Access to arbitrary [user configuration](functions-develop-user-defined-configs) values supplied via the CLI.
* An interface for recording [metrics](functions-develop-metrics).
* An interface for storing and retrieving state in [state storage](functions-develop-state).
* A function to publish new messages onto arbitrary topics.
* A function to acknowledge the message being processed (if auto-ack is disabled).
* (Java) get Pulsar admin client.

:::tip

For more information about code examples, refer to [Java](https://github.com/apache/pulsar/blob/master/pulsar-functions/api-java/src/main/java/org/apache/pulsar/functions/api/BaseContext.java), [Python](https://github.com/apache/pulsar/blob/master/pulsar-functions/instance/src/main/python/contextimpl.py) and [Go](https://github.com/apache/pulsar/blob/master/pulsar-function-go/pf/context.go).

:::

## Function message types

Pulsar Functions take byte arrays as inputs and spit out byte arrays as output. You can write typed functions and bind messages to types by using either of the following ways: 
* [Schema Registry](functions-develop-schema-registry.md)
* [SerDe](functions-develop-serde.md)


## Window function

:::note    

Currently, window function is only available in Java.

:::

Window function is a function that performs computation across a data window, that is, a finite subset of the event stream. As illustrated below, the stream is split into “buckets” where functions can be applied.

![A window of data within an event stream](/assets/function-data-window.png)

The definition of a data window for a function involves two policies:
* Eviction policy: Controls the amount of data collected in a window. 
* Trigger policy: Controls when a function is triggered and executed to process all of the data collected in a window based on eviction policy. 

Both trigger policy and eviction policy are driven by either time or count.

:::tip

Both processing time and event time are supported.
 * Processing time is defined based on the wall time when the function instance builds and processes a window. The judging of window completeness is straightforward and you don’t have to worry about data arrival disorder. 
 * Event time is defined based on the timestamps that come with the event record. It guarantees event time correctness but also offers more data buffering and a limited completeness guarantee.

:::

### Types of window

Based on whether two adjacent windows can share common events or not, windows can be divided into the following two types:
* [Tumbling window](#tumbling-window) 
* [Sliding window](#sliding-window)

#### Tumbling window

Tumbling window assigns elements to a window of a specified time length or count. The eviction policy for tumbling windows is always based on the window being full. So you only need to specify the trigger policy, either count-based or time-based. 

In a tumbling window with a count-based trigger policy, as illustrated in the following example, the trigger policy is set to 2. Each function is triggered and executed when two items are in the window, regardless of the time. 

![A tumbling window with a count-based trigger policy](/assets/function-count-based-tumbling-window.png)

In contrast, as illustrated in the following example, the window length of the tumbling window is 10 seconds, which means the function is triggered when the 10-second time interval has elapsed, regardless of how many events are in the window. 

![A tumbling window with a time-based trigger policy](/assets/function-time-based-tumbling-window.png)

#### Sliding window

The sliding window method defines a fixed window length by setting the eviction policy to limit the amount of data retained for processing and setting the trigger policy with a sliding interval. If the sliding interval is smaller than the window length, there is data overlapping, which means the data simultaneously falling into adjacent windows is used for computation more than once.

As illustrated in the following example, the window length is 2 seconds, which means that any data older than 2 seconds will be evicted and not used in the computation. The sliding interval is configured to be 1 second, which means that function is executed every second to process the data within the entire window length. 

![Sliding window with an overlap](/assets/function-sliding-window.png)
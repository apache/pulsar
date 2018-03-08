---
title: Getting started with Pulsar Functions
lead: Write and run your first Pulsar Function in just a few steps
---

This tutorial will walk you through running a {% popover standalone %} Pulsar {% popover cluster %} on your machine and then running your first Pulsar Functions using that cluster. The first function will run in local run mode (outside your Pulsar {% popover cluster %}), while the second will run in cluster mode (inside your cluster).

{% include admonition.html content="In local run mode, your Pulsar Function will communicate with your Pulsar cluster but will run outside of the cluster." %}

## Prerequisites

In order to follow along with this tutorial, you'll need to have [Maven](https://maven.apache.org/download.cgi) installed on your machine.

## Run a standalone Pulsar cluster

In order to run our Pulsar Functions, we'll need to run a Pulsar cluster locally first. The easiest way to do that is to run Pulsar in {% popover standalone %} mode. Follow these steps to start up a standalone cluster:

```bash
$ wget https://github.com/streamlio/incubator-pulsar/releases/download/2.0.0-incubating-functions-preview/apache-pulsar-2.0.0-incubating-functions-preview-bin.tar.gz
$ tar xvf apache-pulsar-2.0.0-incubating-functions-preview-bin.tar.gz
$ cd apache-pulsar-2.0.0-incubating-functions-preview
$ bin/pulsar standalone \
  --advertised-address 127.0.0.1
```

When running Pulsar in standalone mode, the `sample` {% popover tenant %} and `ns1` {% popover namespace %} will be created automatically for you. That tenant and namespace will be used throughout this tutorial.

## Run a Pulsar Function in local run mode

Let's start with a simple function that takes a string as input from a Pulsar topic, adds an exclamation point to the end of the string, and then publishes that new string to another Pulsar topic. Here's the code for the function:

```java
package org.apache.pulsar.functions.api.examples;

import java.util.function.Function;

public class ExclamationFunction implements Function<String, String> {
    @Override
    public String apply(String input) {
        return String.format("%s!", input);
    }
}
```

A JAR file containing this and several other functions (written in Java) is included with the binary distribution you downloaded above (in the `examples` folder). To run the function in local mode, i.e. on our laptop but outside our Pulsar cluster:

```bash
$ bin/pulsar-admin functions localrun \
  --jar examples/api-examples.jar \
  --className org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://sample/standalone/ns1/exclamation-input \
  --output persistent://sample/standalone/ns1/exclamation-output \
  --name exclamation
```

We can use the [`pulsar-client`](../../reference/CliTools#pulsar-client) CLI tool to publish a message to the input topic:

```bash
$ bin/pulsar-client produce persistent://sample/standalone/ns1/exclamation-input \
  --num-produce 1 \
  --messages "Hello world"
```

Here's what happens next:

* The `Hello world` message that we published to the input {% popover topic %} (`persistent://sample/standalone/ns1/exclamation-input`) will be passed to the exclamation function that we're now running on our laptop
* The exclamation function will process the message (providing a result of `Hello world!`) and publish the result to the output topic (`persistent://sample/standalone/ns1/exclamation-output`).
* Pulsar will durably store the message data published to the output topic in [Apache BookKeeper](https://bookkeeper.apache.org) until a {% popover consumer %} consumes and {% popover acknowledges %} the message

To consume the message, we can use the same [`pulsar-client`](../../reference/CliTools#pulsar-client) tool that we used to publish the original message:

```bash
$ bin/pulsar-client consume persistent://sample/standalone/ns1/exclamation-output \
  --subscription-name my-subscription \
  --num-messages 1
```

In the output, you should see the following:

```
----- got message -----
Hello world!
```

Success! As you can see, the message has been successfully processed by the exclamation function. To shut down the function, simply hit **Ctrl+C**.

## Run a Pulsar Function in cluster mode

[Local run mode](#run-a-pulsar-function-in-local-run-mode) is useful for development and experimentation, but if you wanted to use Pulsar Functions in a real Pulsar deployment, you'd want to run them in **cluster mode**. In cluster mode, Pulsar Functions run *inside* your Pulsar cluster and are managed using the same `pulsar-admin functions` interface that we've been using thus far.

This command would deploy the same exclamation function we ran locally above in our Pulsar cluster:

```bash
$ bin/pulsar-admin functions create \
  --jar examples/api-examples.jar \
  --className org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://sample/standalone/ns1/exclamation-input \
  --output persistent://sample/standalone/ns1/exclamation-output \
  --tenant sample \
  --namespace ns1 \
  --name exclamation
```

You should see `Created successfully` in the output. Now, let's see a list of functions running in our cluster:

```bash
$ bin/pulsar-admin functions list \
  --tenant sample \
  --namespace ns1
```

We should see just the `exclamation` function listed there. We can also check the status of our deployed function using the `getstatus` command:

```bash
$ bin/pulsar-admin functions getstatus \
  --tenant sample \
  --namespace ns1 \
  --name exclamation
```

You should see this JSON output:

```json
{
  "functionStatusList": [
    {
      "running": true,
      "instanceId": "0"
    }
  ]
}
```

As we can see, (a) the instance is currently running and (b) there is one instance, with an ID of 0, running. We can get other information about the function (topics, tenant, namespace, etc.) using the `get` command instead of `getstatus`:

```bash
$ bin/pulsar-admin functions get \
  --tenant sample \
  --namespace ns1 \
  --name exclamation
```

You should see this JSON output:

```json
{
  "tenant": "sample",
  "namespace": "ns1",
  "name": "exclamation",
  "className": "org.apache.pulsar.functions.api.examples.ExclamationFunction",
  "output": "persistent://sample/standalone/ns1/exclamation-output",
  "autoAck": true,
  "inputs": [
    "persistent://sample/standalone/ns1/exclamation-input"
  ],
  "parallelism": 1
}
```

As we can see, the parallelism of the function is 1, meaning that only one instance of the function is running in our cluster. Let's update our function to a parallelism of 3 using the `update` command:

```bash
$ bin/pulsar-admin functions update \
  --jar examples/api-examples.jar \
  --className org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://sample/standalone/ns1/exclamation-input \
  --output persistent://sample/standalone/ns1/exclamation-output \
  --tenant sample \
  --namespace ns1 \
  --name exclamation \
  --parallelism 3
```

You should see `Updated successfully` in the output. If you run the `get` command from above for the function, you can see that the parallelism has increased to 3, meaning that there are now three instances of the function running in our cluster:

```json
{
  "tenant": "sample",
  "namespace": "ns1",
  "name": "exclamation",
  "className": "org.apache.pulsar.functions.api.examples.ExclamationFunction",
  "output": "persistent://sample/standalone/ns1/exclamation-output",
  "autoAck": true,
  "inputs": [
    "persistent://sample/standalone/ns1/exclamation-input"
  ],
  "parallelism": 3
}
```

Finally, we can shut down our running function using the `delete` command:

```bash
$ bin/pulsar-admin functions delete \
  --tenant sample \
  --namespace ns1 \
  --name exclamation
```

If you see `Deleted successfully` in the output, then you've succesfully run, updated, and shut down a Pulsar Function running in cluster mode. Congrats! Now, let's go even further and run a brand new function in the next section.

## Writing and running a new function

In the above examples, we ran and managed a pre-written Pulsar Function and saw how it worked. To really get our hands dirty, let's write and our own function from scratch, using the Python API. This simple function will also take a string as input but it will reverse the string and publish the resulting, reversed string to the specified topic.

First, create a new Python file:

```bash
$ touch reverse.py
```

In that file, add the following:

```python
from pulsarfunction import pulsar_function

class Reverse(pulsar_function.PulsarFunction):
  def __init__(self):
    pass

  def process(self, input):
    return input[::-1]
```

Here, the `process` method defines the processing logic of the Pulsar Function. It simply uses some Python slice magic to reverse each incoming string. Now, we can deploy the function using `create`:

```bash
$ bin/pulsar-admin functions create \
  --py reverse.py \
  --className reverse.Reverse \
  --inputs persistent://sample/standalone/ns1/forwards \
  --output persistent://sample/standalone/ns1/backwards \
  --tenant sample \
  --namespace ns1 \
  --name reverse 
```

If you see `Created successfully`, the function is ready to accept incoming messages. Let's publish a string to the input topic:

```bash
$ bin/pulsar-client produce persistent://sample/standalone/ns1/forwards \
  --num-produce 1 \
  --messages "sdrawrof won si tub sdrawkcab saw gnirts sihT"
```

Now, let's pull in a message from the output topic:

```bash
$ bin/pulsar-client consume persistent://sample/standalone/ns1/backwards \
  --subscription-name my-subscription \
  --num-messages 1
```

You should see the reversed string in the log output:

```
----- got message -----
This string was backwards but is now forwards
```

Once again, success! We created a brand new Pulsar Function, deployed it in our Pulsar standalone cluster, and successfully published to the function's input topic and consumed from its output topic.
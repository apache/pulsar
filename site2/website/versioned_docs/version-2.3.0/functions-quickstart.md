---
id: version-2.3.0-functions-quickstart
title: Getting started with Pulsar Functions
sidebar_label: Getting started
original_id: functions-quickstart
---

This tutorial will walk you through running a [standalone](reference-terminology.md#standalone) Pulsar [cluster](reference-terminology.md#cluster) on your machine and then running your first Pulsar Functions using that cluster. The first function will run in local run mode (outside your Pulsar [cluster](reference-terminology.md#cluster)), while the second will run in cluster mode (inside your cluster).

> In local run mode, your Pulsar Function will communicate with your Pulsar cluster but will run outside of the cluster.

## Prerequisites

In order to follow along with this tutorial, you'll need to have [Maven](https://maven.apache.org/download.cgi) installed on your machine.

## Run a standalone Pulsar cluster

In order to run our Pulsar Functions, we'll need to run a Pulsar cluster locally first. The easiest way to do that is to run Pulsar in [standalone](reference-terminology.md#standalone) mode. Follow these steps to start up a standalone cluster:

```bash
$ wget pulsar:binary_release_url
$ tar xvfz apache-pulsar-{{pulsar:version}}-bin.tar.gz
$ cd apache-pulsar-{{pulsar:version}}
$ bin/pulsar standalone \
  --advertised-address 127.0.0.1
```

When running Pulsar in standalone mode, the `public` tenant and `default` namespace will be created automatically for you. That tenant and namespace will be used throughout this tutorial.

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
  --classname org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://public/default/exclamation-input \
  --output persistent://public/default/exclamation-output \
  --name exclamation
```

> #### Multiple input topics allowed
>
> In the example above, a single topic was specified using the `--inputs` flag. You can also specify multiple input topics as a comma-separated list using the same flag. Here's an example:
>
> ```bash
> --inputs topic1,topic2
> ```

We can open up another shell and use the [`pulsar-client`](reference-cli-tools.md#pulsar-client) tool to listen for messages on the output topic:

```bash
$ bin/pulsar-client consume persistent://public/default/exclamation-output \
  --subscription-name my-subscription \
  --num-messages 0
```

> Setting the `--num-messages` flag to 0 means that the consumer will listen on the topic indefinitely (rather than only accepting a certain number of messages).

With a listener up and running, we can open up another shell and produce a message on the input topic that we specified:

```bash
$ bin/pulsar-client produce persistent://public/default/exclamation-input \
  --num-produce 1 \
  --messages "Hello world"
```

In the output, you should see the following:

```
----- got message -----
Hello world!
```

Success! As you can see, the message has been successfully processed by the exclamation function. To shut down the function, simply hit **Ctrl+C**.

Here's what happened:

* The `Hello world` message that we published to the input topic (`persistent://public/default/exclamation-input`) was passed to the exclamation function that we ran on our machine
* The exclamation function processed the message (providing a result of `Hello world!`) and published the result to the output topic (`persistent://public/default/exclamation-output`).
* If our exclamation function *hadn't* been running, Pulsar would have durably stored the message data published to the input topic in [Apache BookKeeper](https://bookkeeper.apache.org) until a consumer consumed and acknowledged the message

## Run a Pulsar Function in cluster mode

[Local run mode](#run-a-pulsar-function-in-local-run-mode) is useful for development and experimentation, but if you want to use Pulsar Functions in a real Pulsar deployment, you'll want to run them in **cluster mode**. In this mode, Pulsar Functions run *inside* your Pulsar cluster and are managed using the same [`pulsar-admin functions`](reference-pulsar-admin.md#functions) interface that we've been using thus far.

This command, for example, would deploy the same exclamation function we ran locally above *in our Pulsar cluster* (rather than outside it):

```bash
$ bin/pulsar-admin functions create \
  --jar examples/api-examples.jar \
  --classname org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://public/default/exclamation-input \
  --output persistent://public/default/exclamation-output \
  --name exclamation
```

You should see `Created successfully` in the output. Now, let's see a list of functions running in our cluster:

```bash
$ bin/pulsar-admin functions list \
  --tenant public \
  --namespace default
```

We should see just the `exclamation` function listed there. We can also check the status of our deployed function using the `getstatus` command:

```bash
$ bin/pulsar-admin functions getstatus \
  --tenant public \
  --namespace default \
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
  --tenant public \
  --namespace default \
  --name exclamation
```

You should see this JSON output:

```json
{
  "tenant": "public",
  "namespace": "default",
  "name": "exclamation",
  "className": "org.apache.pulsar.functions.api.examples.ExclamationFunction",
  "output": "persistent://public/default/exclamation-output",
  "autoAck": true,
  "inputs": [
    "persistent://public/default/exclamation-input"
  ],
  "parallelism": 1
}
```

As we can see, the parallelism of the function is 1, meaning that only one instance of the function is running in our cluster. Let's update our function to a parallelism of 3 using the `update` command:

```bash
$ bin/pulsar-admin functions update \
  --jar examples/api-examples.jar \
  --classname org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://public/default/exclamation-input \
  --output persistent://public/default/exclamation-output \
  --tenant public \
  --namespace default \
  --name exclamation \
  --parallelism 3
```

You should see `Updated successfully` in the output. If you run the `get` command from above for the function, you can see that the parallelism has increased to 3, meaning that there are now three instances of the function running in our cluster:

```json
{
  "tenant": "public",
  "namespace": "default",
  "name": "exclamation",
  "className": "org.apache.pulsar.functions.api.examples.ExclamationFunction",
  "output": "persistent://public/default/exclamation-output",
  "autoAck": true,
  "inputs": [
    "persistent://public/default/exclamation-input"
  ],
  "parallelism": 3
}
```

Finally, we can shut down our running function using the `delete` command:

```bash
$ bin/pulsar-admin functions delete \
  --tenant public \
  --namespace default \
  --name exclamation
```

If you see `Deleted successfully` in the output, then you've successfully run, updated, and shut down a Pulsar Function running in cluster mode. Congrats! Now, let's go even further and run a brand new function in the next section.

## Writing and running a new function

> In order to write and run the [Python](functions-api.md#functions-for-python) function below, you'll need to install a few dependencies:
> ```bash
> $ pip install pulsar-client
> ```

In the above examples, we ran and managed a pre-written Pulsar Function and saw how it worked. To really get our hands dirty, let's write and our own function from scratch, using the Python API. This simple function will also take a string as input but it will reverse the string and publish the resulting, reversed string to the specified topic.

First, create a new Python file:

```bash
$ touch reverse.py
```

In that file, add the following:

```python
def process(input):
    return input[::-1]
```

Here, the `process` method defines the processing logic of the Pulsar Function. It simply uses some Python slice magic to reverse each incoming string. Now, we can deploy the function using `create`:

```bash
$ bin/pulsar-admin functions create \
  --py reverse.py \
  --classname reverse \
  --inputs persistent://public/default/backwards \
  --output persistent://public/default/forwards \
  --tenant public \
  --namespace default \
  --name reverse
```

If you see `Created successfully`, the function is ready to accept incoming messages. Because the function is running in cluster mode, we can **trigger** the function using the [`trigger`](reference-pulsar-admin.md#trigger) command. This command will send a message that we specify to the function and also give us the function's output. Here's an example:

```bash
$ bin/pulsar-admin functions trigger \
  --name reverse \
  --tenant public \
  --namespace default \
  --trigger-value "sdrawrof won si tub sdrawkcab saw gnirts sihT"
```

You should get this output:

```
This string was backwards but is now forwards
```

Once again, success! We created a brand new Pulsar Function, deployed it in our Pulsar standalone cluster in [cluster mode](#run-a-pulsar-function-in-cluster-mode) and successfully triggered the function. If you're ready for more, check out one of these docs:

## Packaging python dependencies

For python functions requiring dependencies to be deployable in pulsar worker instances in an offline manner, we need to package the artifacts as below.


#### Client Requirements

Following programs are required to be installed on the client machine

```
pip \\ required for getting python dependencies
zip \\ for building zip archives
```

#### Python Dependencies

A file named **requirements.txt** is needed with required dependencies for the python function


```
sh==1.12.14
```

Prepare the pulsar function in folder called **src**.

Run the following command to gather the python dependencies in the folder caller **deps**

```
pip download \
--only-binary :all: \
--platform manylinux1_x86_64 \
--python-version 27 \
--implementation cp \
--abi cp27m -r requirements.txt -d deps

```

Sample output

```
Collecting sh==1.12.14 (from -r requirements.txt (line 1))
  Using cached https://files.pythonhosted.org/packages/4a/22/17b22ef5b049f12080f5815c41bf94de3c229217609e469001a8f80c1b3d/sh-1.12.14-py2.py3-none-any.whl
  Saved ./deps/sh-1.12.14-py2.py3-none-any.whl
Successfully downloaded sh
```

**Note** pulsar-client is not needed as a dependency as it already installed in the worker node.


#### Packaging
Create a destination folder with the desired pacaking name eg : **exclamation**, copy **src** and **deps** folder into it and finally compress the folder into a zip archive.

Sample sequence

```
cp -R deps exclamation/
cp -R src exclamation/

ls -la exclamation/
total 7
drwxr-xr-x   5 a.ahmed  staff  160 Nov  6 17:51 .
drwxr-xr-x  12 a.ahmed  staff  384 Nov  6 17:52 ..
drwxr-xr-x   3 a.ahmed  staff   96 Nov  6 17:51 deps
drwxr-xr-x   3 a.ahmed  staff   96 Nov  6 17:51 src

zip -r exclamation.zip exclamation
```

Archive **exclamation.zip** can we deployed as function into a pulsar worker, the worker does not need internet connectivity to download packages as they are all included in the zip file.


* [The Pulsar Functions API](functions-api.md)
* [Deploying Pulsar Functions](functions-deploying.md)

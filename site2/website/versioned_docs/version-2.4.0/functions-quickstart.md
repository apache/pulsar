---
id: functions-quickstart
title: Get started with Pulsar Functions
sidebar_label: "Get started"
original_id: functions-quickstart
---

This tutorial walks you through running a [standalone](reference-terminology.md#standalone) Pulsar [cluster](reference-terminology.md#cluster) on your machine, and then running your first Pulsar Function using that cluster. The first Pulsar Function runs in local run mode (outside your Pulsar [cluster](reference-terminology.md#cluster)), while the second runs in cluster mode (inside your cluster).

> In local run mode, Pulsar Functions communicate with Pulsar cluster, but run outside of the cluster.

## Prerequisites

Install [Maven](https://maven.apache.org/download.cgi) on your machine.

## Run a standalone Pulsar cluster

In order to run Pulsar Functions, you need to run a Pulsar cluster locally first. The easiest way is to run Pulsar in [standalone](reference-terminology.md#standalone) mode. Follow these steps to start up a standalone cluster.

```bash

$ wget pulsar:binary_release_url
$ tar xvfz apache-pulsar-@pulsar:version@-bin.tar.gz
$ cd apache-pulsar-@pulsar:version@
$ bin/pulsar standalone \
  --advertised-address 127.0.0.1

```

When running Pulsar in standalone mode, the `public` tenant and the `default` namespace are created automatically. The tenant and namespace are used throughout this tutorial.

## Run a Pulsar Function in local run mode

You can start with a simple function that takes a string as input from a Pulsar topic, adds an exclamation point to the end of the string, and then publishes the new string to another Pulsar topic. The following is the code for the function.

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

A JAR file containing this function and several other functions (written in Java) is included with the binary distribution you have downloaded (in the `examples` folder). Run the function in local mode on your laptop but outside your Pulsar cluster with the following commands.

```bash

$ bin/pulsar-admin functions localrun \
  --jar examples/api-examples.jar \
  --classname org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://public/default/exclamation-input \
  --output persistent://public/default/exclamation-output \
  --name exclamation

```

> #### Multiple input topics
>
> In the example above, a single topic is specified using the `--inputs` flag. You can also specify multiple input topics with a comma-separated list using the same flag. 
>

> ```bash
> 
> --inputs topic1,topic2
>
> 
> ```


You can open up another shell and use the [`pulsar-client`](reference-cli-tools.md#pulsar-client) tool to listen for messages on the output topic.

```bash

$ bin/pulsar-client consume persistent://public/default/exclamation-output \
  --subscription-name my-subscription \
  --num-messages 0

```

> Setting the `--num-messages` flag to `0` means that consumers listen on the topic indefinitely, rather than only accepting a certain number of messages.

With a listener up and running, you can open up another shell and produce a message on the input topic that you specify.

```bash

$ bin/pulsar-client produce persistent://public/default/exclamation-input \
  --num-produce 1 \
  --messages "Hello world"

```

When the message has been successfully processed by the exclamation function, you will see the following output. To shut down the function, press **Ctrl+C**.

```

----- got message -----
Hello world!

```

### Process explanation

* The `Hello world` message you publish to the input topic (`persistent://public/default/exclamation-input`) is passed to the exclamation function.
* The exclamation function processes the message (providing a result of `Hello world!`) and publishes the result to the output topic (`persistent://public/default/exclamation-output`).
* If the exclamation function *does not* run, Pulsar will durably store the message data published to the input topic in [Apache BookKeeper](https://bookkeeper.apache.org) until a consumer consumes and acknowledges the message.

## Run a Pulsar Function in cluster mode

[Local run mode](#run-a-pulsar-function-in-local-run-mode) is useful for development and test. However, when you use Pulsar for real deployment, you run it in **cluster mode**. In cluster mode, Pulsar Functions run *inside* of your Pulsar cluster and are managed using the same [`pulsar-admin functions`](reference-pulsar-admin.md#functions) interface.

The following command deploys the same exclamation function you run locally in your Pulsar cluster, rather than outside of it.

```bash

$ bin/pulsar-admin functions create \
  --jar examples/api-examples.jar \
  --classname org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://public/default/exclamation-input \
  --output persistent://public/default/exclamation-output \
  --name exclamation

```

You will see `Created successfully` in the output. Check the list of functions running in your cluster.

```bash

$ bin/pulsar-admin functions list \
  --tenant public \
  --namespace default

```

You will see the `exclamation` function. Check the status of your deployed function using the `getstatus` command.

```bash

$ bin/pulsar-admin functions getstatus \
  --tenant public \
  --namespace default \
  --name exclamation

```

You will see the following JSON output.

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

As you can see, the instance is currently running, and an instance with the ID of `0` is running. With the `get` command, you can get other information about the function, for example, topics, tenant, namespace, and so on.

```bash

$ bin/pulsar-admin functions get \
  --tenant public \
  --namespace default \
  --name exclamation

```

You will see the following JSON output.

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

As you can see, only one instance of the function is running in your cluster. Update the parallel functions to `3` using the `update` command.

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

You will see `Updated successfully` in the output. If you enter the `get` command, you see that the parallel functions are increased to `3`, meaning that three instances of the function are running in your cluster.

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

Shut down the running function with the `delete` command.

```bash

$ bin/pulsar-admin functions delete \
  --tenant public \
  --namespace default \
  --name exclamation

```

When you see `Deleted successfully` in the output, you've successfully run, updated, and shut down functions running in cluster mode. 

## Write and run a new function

In order to write and run [Python](functions-api.md#functions-for-python) functions, you need to install some dependencies.

```bash

$ pip install pulsar-client

```

In the examples above, you run and manage pre-written Pulsar Functions and learn how they work. You can also write your own functions with Python API. In the following example, the function takes a string as input, reverses the string, and publishes the reversed string to the specified topic.

First, create a new Python file.

```bash

$ touch reverse.py

```

Add the following information in the Python file.

```python

def process(input):
    return input[::-1]

```

The `process` method defines the processing logic of Pulsar Functions. It uses Python slice magic to reverse each incoming string. You can deploy the function using the `create` command.

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

If you see `Created successfully`, the function is ready to accept incoming messages. Because the function is running in cluster mode, you can **trigger** the function using the [`trigger`](reference-pulsar-admin.md#trigger) command. This command sends a message that you specify to the function and returns the function output. The following is an example.

```bash

$ bin/pulsar-admin functions trigger \
  --name reverse \
  --tenant public \
  --namespace default \
  --trigger-value "sdrawrof won si tub sdrawkcab saw gnirts sihT"

```

You will get the following output.

```

This string was backwards but is now forwards

```

You have created a new Pulsar Function, deployed it in your Pulsar standalone cluster in [cluster mode](#run-a-pulsar-function-in-cluster-mode), and triggered the Function. 

## Write and run a Go function
Go function depends on `pulsar-client-go`. Make sure that you have built `pulsar-client-go` before using Go function.

To write and run a Go function, complete the following steps.

1. Create a new Go file.

```

touch helloFunc.go

```

2. Append a byte for messages from the input topic.    
The following is a `helloFunc.go` example. Each message from the input topic is appended with a `110` byte, and then delivered to the output topic.

```

package main

import (
	"context"

	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func HandleResponse(ctx context.Context, in []byte) ([]byte, error) {
	res := append(in, 110)
	return res, nil
}

func main() {
	pf.Start(HandleResponse)
}

```

3. Compile code.

```

go build -o examplepulsar helloFunc.go

```

4. Run Go function. 

```

$ bin/pulsar-admin functions create \
  --go examplepulsar \
  --inputs persistent://public/default/backwards \
  --output persistent://public/default/forwards \
  --tenant public \
  --namespace default \
  --name gofunc

```

If you see `Created successfully`, the function is ready to accept incoming messages. Start a producer and produce messages to the `backwards` input topic. Start a consumer and consume messages from the `forwards` output topic, you will see `110` is appended to all messages.

The `--classname` parameter is not specified when running Go function, because there is no `Class` concept in Go, which is different from Java and Python.

:::note

When you use the `--go` command to specify an executable file, make sure you have executable permissions.

:::

## Package Python dependencies

When you deploy Python functions in a cluster offline, you need to package the required dependencies in a ZIP file before deployment.

### Client requirements

The following programs are required to be installed on the client machine.

```

pip \\ required for getting python dependencies
zip \\ for building zip archives

```

### Python dependencies

A file named **requirements.txt** is needed with required dependencies for the Python function.

```

sh==1.12.14

```

Prepare the Pulsar Function in the **src** folder.

Run the following command to gather Python dependencies in the **deps** folder.

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

:::note

`pulsar-client` is not needed as a dependency as it has already installed in the worker node.

:::

#### Package
Create a destination folder with the desired package name, for example, **exclamation**. Copy the **src** and **deps** folders into it, and compress the folder into a ZIP archive.

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

After package all the required dependencies into the **exclamation.zip** file, you can deploy functions in a Pulsar worker. The Pulsar worker does not need internet connectivity to download packages, because they are all included in the ZIP file.
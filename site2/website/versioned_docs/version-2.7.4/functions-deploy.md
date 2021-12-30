---
id: version-2.7.4-functions-deploy
title: Deploy Pulsar Functions
sidebar_label: How-to: Deploy
original_id: functions-deploy
---

## Requirements

To deploy and manage Pulsar Functions, you need to have a Pulsar cluster running. There are several options for this:

* You can run a [standalone cluster](getting-started-standalone.md) locally on your own machine.
* You can deploy a Pulsar cluster on [Kubernetes](deploy-kubernetes.md), [Amazon Web Services](deploy-aws.md), [bare metal](deploy-bare-metal.md), [DC/OS](deploy-dcos.md), and more.

If you run a non-[standalone](reference-terminology.md#standalone) cluster, you need to obtain the service URL for the cluster. How you obtain the service URL depends on how you deploy your Pulsar cluster.

If you want to deploy and trigger Python user-defined functions, you need to install [the pulsar python client](http://pulsar.apache.org/docs/en/client-libraries-python/) on all the machines running [functions workers](functions-worker.md).

## Command-line interface

Pulsar Functions are deployed and managed using the [`pulsar-admin functions`](reference-pulsar-admin.md#functions) interface, which contains commands such as [`create`](reference-pulsar-admin.md#functions-create) for deploying functions in [cluster mode](#cluster-mode), [`trigger`](reference-pulsar-admin.md#trigger) for [triggering](#triggering-pulsar-functions) functions, [`list`](reference-pulsar-admin.md#list-2) for listing deployed functions.

To learn more commands, refer to [`pulsar-admin functions`](reference-pulsar-admin.md#functions).

### Default arguments

When managing Pulsar Functions, you need to specify a variety of information about functions, including tenant, namespace, input and output topics, and so on. However, some parameters have default values if you do not specify values for them. The following table lists the default values.

Parameter | Default
:---------|:-------
Function name | You can specify any value for the class name (except org, library, or similar class names). For example, when you specify the flag `--classname org.example.MyFunction`, the function name is `MyFunction`.
Tenant | Derived from names of the input topics. If the input topics are under the `marketing` tenant, which means the topic names have the form `persistent://marketing/{namespace}/{topicName}`, the tenant is `marketing`.
Namespace | Derived from names of the input topics. If the input topics are under the `asia` namespace under the `marketing` tenant, which means the topic names have the form `persistent://marketing/asia/{topicName}`, then the namespace is `asia`.
Output topic | `{input topic}-{function name}-output`. For example, if an input topic name of a function is `incoming`, and the function name is `exclamation`, then the name of the output topic is `incoming-exclamation-output`.
Subscription type | For `at-least-once` and `at-most-once` [processing guarantees](functions-overview.md#processing-guarantees), the [`SHARED`](concepts-messaging.md#shared) mode is applied by default; for `effectively-once` guarantees, the [`FAILOVER`](concepts-messaging.md#failover) mode is applied.
Processing guarantees | [`ATLEAST_ONCE`](functions-overview.md#processing-guarantees)
Pulsar service URL | `pulsar://localhost:6650`

### Example of default arguments

Take the `create` command as an example.

```bash
$ bin/pulsar-admin functions create \
  --jar my-pulsar-functions.jar \
  --classname org.example.MyFunction \
  --inputs my-function-input-topic1,my-function-input-topic2
```

The function has default values for the function name (`MyFunction`), tenant (`public`), namespace (`default`), subscription type (`SHARED`), processing guarantees (`ATLEAST_ONCE`), and Pulsar service URL (`pulsar://localhost:6650`).

## Local run mode

If you run a Pulsar Function in **local run** mode, it runs on the machine from which you enter the commands (on your laptop, an [AWS EC2](https://aws.amazon.com/ec2/) instance, and so on). The following is a [`localrun`](reference-pulsar-admin.md#localrun) command example.

```bash
$ bin/pulsar-admin functions localrun \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/input-1 \
  --output persistent://public/default/output-1
```

By default, the function connects to a Pulsar cluster running on the same machine, via a local [broker](reference-terminology.md#broker) service URL of `pulsar://localhost:6650`. If you use local run mode to run a function but connect it to a non-local Pulsar cluster, you can specify a different broker URL using the `--brokerServiceUrl` flag. The following is an example.

```bash
$ bin/pulsar-admin functions localrun \
  --broker-service-url pulsar://my-cluster-host:6650 \
  # Other function parameters
```

## Cluster mode

When you run a Pulsar Function in **cluster** mode, the function code is uploaded to a Pulsar broker and runs *alongside the broker* rather than in your [local environment](#local-run-mode). You can run a function in cluster mode using the [`create`](reference-pulsar-admin.md#create-1) command. 

```bash
$ bin/pulsar-admin functions create \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/input-1 \
  --output persistent://public/default/output-1
```

### Update functions in cluster mode 

You can use the [`update`](reference-pulsar-admin.md#update-1) command to update a Pulsar Function running in cluster mode. The following command updates the function created in the [cluster mode](#cluster-mode) section.

```bash
$ bin/pulsar-admin functions update \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/new-input-topic \
  --output persistent://public/default/new-output-topic
```

### Parallelism

Pulsar Functions run as processes or threads, which are called **instances**. When you run a Pulsar Function, it runs as a single instance by default. With one localrun command, you can only run a single instance of a function. If you want to run multiple instances, you can use localrun command multiple times. 

When you create a function, you can specify the *parallelism* of a function (the number of instances to run). You can set the parallelism factor using the `--parallelism` flag of the [`create`](reference-pulsar-admin.md#functions-create) command. 

```bash
$ bin/pulsar-admin functions create \
  --parallelism 3 \
  # Other function info
```

You can adjust the parallelism of an already created function using the [`update`](reference-pulsar-admin.md#update-1) interface.

```bash
$ bin/pulsar-admin functions update \
  --parallelism 5 \
  # Other function
```

If you specify a function configuration via YAML, use the `parallelism` parameter. The following is a config file example.

```yaml
# function-config.yaml
parallelism: 3
inputs:
- persistent://public/default/input-1
output: persistent://public/default/output-1
# other parameters
```

The following is corresponding update command.

```bash
$ bin/pulsar-admin functions update \
  --function-config-file function-config.yaml
```

### Function instance resources

When you run Pulsar Functions in [cluster mode](#cluster-mode), you can specify the resources that are assigned to each function [instance](#parallelism).

Resource | Specified as | Runtimes
:--------|:----------------|:--------
CPU | The number of cores | Kubernetes
RAM | The number of bytes | Process, Docker
Disk space | The number of bytes | Docker

The following function creation command allocates 8 cores, 8 GB of RAM, and 10 GB of disk space to a function.

```bash
$ bin/pulsar-admin functions create \
  --jar target/my-functions.jar \
  --classname org.example.functions.MyFunction \
  --cpu 8 \
  --ram 8589934592 \
  --disk 10737418240
```

> #### Resources are *per instance*
> The resources that you apply to a given Pulsar Function are applied to each instance of the function. For example, if you apply 8 GB of RAM to a function with a parallelism of 5, you are applying 40 GB of RAM for the function in total. Make sure that you take the parallelism (the number of instances) factor into your resource calculations.

## Trigger Pulsar Functions

If a Pulsar Function is running in [cluster mode](#cluster-mode), you can **trigger** it at any time using the command line. Triggering a function means that you send a message with a specific value to the function and get the function output (if any) via the command line.

> Triggering a function is to invoke a function by producing a message on one of the input topics. With the [`pulsar-admin functions trigger`](reference-pulsar-admin.md#trigger) command, you can send messages to functions without using the [`pulsar-client`](reference-cli-tools.md#pulsar-client) tool or a language-specific client library.

To learn how to trigger a function, you can start with Python function that returns a simple string based on the input.

```python
# myfunc.py
def process(input):
    return "This function has been triggered with a value of {0}".format(input)
```

You can run the function in [local run mode](functions-deploy.md#local-run-mode).

```bash
$ bin/pulsar-admin functions create \
  --tenant public \
  --namespace default \
  --name myfunc \
  --py myfunc.py \
  --classname myfunc \
  --inputs persistent://public/default/in \
  --output persistent://public/default/out
```

Then assign a consumer to listen on the output topic for messages from the `myfunc` function with the [`pulsar-client consume`](reference-cli-tools.md#consume) command.

```bash
$ bin/pulsar-client consume persistent://public/default/out \
  --subscription-name my-subscription
  --num-messages 0 # Listen indefinitely
```

And then you can trigger the function.

```bash
$ bin/pulsar-admin functions trigger \
  --tenant public \
  --namespace default \
  --name myfunc \
  --trigger-value "hello world"
```

The consumer listening on the output topic produces something as follows in the log.

```
----- got message -----
This function has been triggered with a value of hello world
```

> #### Topic info is not required
> In the `trigger` command, you only need to specify basic information about the function (tenant, namespace, and name). To trigger the function, you do not need to know the function input topics.

---
id: version-2.2.0-functions-deploying
title: Deploying and managing Pulsar Functions
sidebar_label: Deploying functions
original_id: functions-deploying
---

At the moment, there are two deployment modes available for Pulsar Functions:

Mode | Description
:----|:-----------
Local run mode | The function runs in your local environment, for example on your laptop
Cluster mode | The function runs *inside of* your Pulsar cluster, on the same machines as your Pulsar brokers

> #### Contributing new deployment modes
> The Pulsar Functions feature was designed, however, with extensibility in mind. Other deployment options will be available in the future. If you'd like to add a new deployment option, we recommend getting in touch with the Pulsar developer community at [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org).

## Requirements

In order to deploy and manage Pulsar Functions, you need to have a Pulsar cluster running. There are several options for this:

* You can run a [standalone cluster](getting-started-standalone.md) locally on your own machine
* You can deploy a Pulsar cluster on [Kubernetes](deploy-kubernetes.md), [Amazon Web Services](deploy-aws.md), [bare metal](deploy-bare-metal.md), [DC/OS](deploy-dcos.md), and more

If you're running a non-[standalone](reference-terminology.md#standalone) cluster, you'll need to obtain the service URL for the cluster. How you obtain the service URL will depend on how you deployed your Pulsar cluster.

If you're going to deploy and trigger python user-defined functions, you should install [the pulsar python client](http://pulsar.apache.org/docs/en/client-libraries-python/) first.

## Command-line interface

Pulsar Functions are deployed and managed using the [`pulsar-admin functions`](reference-pulsar-admin.md#functions) interface, which contains commands such as [`create`](reference-pulsar-admin.md#functions-create) for deploying functions in [cluster mode](#cluster-mode), [`trigger`](reference-pulsar-admin.md#trigger) for [triggering](#triggering-pulsar-functions) functions, [`list`](reference-pulsar-admin.md#list-2) for listing deployed functions, and several others.

### Fully Qualified Function Name (FQFN)

Each Pulsar Function has a **Fully Qualified Function Name** (FQFN) that consists of three elements: the function's tenant, namespace, and function name. FQFN's look like this:

```http
tenant/namespace/name
```

FQFNs enable you to, for example, create multiple functions with the same name provided that they're in different namespaces.

### Default arguments

When managing Pulsar Functions, you'll need to specify a variety of information about those functions, including tenant, namespace, input and output topics, etc. There are some parameters, however, that have default values that will be supplied if omitted. The table below lists the defaults:

Parameter | Default
:---------|:-------
Function name | Whichever value is specified for the class name (minus org, library, etc.). The flag `--classname org.example.MyFunction`, for example, would give the function a name of `MyFunction`.
Tenant | Derived from the input topics' names. If the input topics are under the `marketing` tenant---i.e. the topic names have the form `persistent://marketing/{namespace}/{topicName}`---then the tenant will be `marketing`.
Namespace | Derived from the input topics' names. If the input topics are under the `asia` namespace under the `marketing` tenant---i.e. the topic names have the form `persistent://marketing/asia/{topicName}`, then the namespace will be `asia`.
Output topic | `{input topic}-{function name}-output`. A function with an input topic name of `incoming` and a function name of `exclamation`, for example, would have an output topic of `incoming-exclamation-output`.
Subscription type | For at-least-once and at-most-once [processing guarantees](functions-guarantees.md), the [`SHARED`](concepts-messaging.md#shared) is applied by default; for effectively-once guarantees, [`FAILOVER`](concepts-messaging.md#failover) is applied
Processing guarantees | [`ATLEAST_ONCE`](functions-guarantees.md)
Pulsar service URL | `pulsar://localhost:6650`

#### Example use of defaults

Take this `create` command:

```bash
$ bin/pulsar-admin functions create \
  --jar my-pulsar-functions.jar \
  --classname org.example.MyFunction \
  --inputs my-function-input-topic1,my-function-input-topic2
```

The created function would have default values supplied for the function name (`MyFunction`), tenant (`public`), namespace (`default`), subscription type (`SHARED`), processing guarantees (`ATLEAST_ONCE`), and Pulsar service URL (`pulsar://localhost:6650`).

## Local run mode

If you run a Pulsar Function in **local run** mode, it will run on the machine from which the command is run (this could be your laptop, an [AWS EC2](https://aws.amazon.com/ec2/) instance, etc.). Here's an example [`localrun`](reference-pulsar-admin.md#localrun) command:

```bash
$ bin/pulsar-admin functions localrun \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/input-1 \
  --output persistent://public/default/output-1
```

By default, the function will connect to a Pulsar cluster running on the same machine, via a local [broker](reference-terminology.md#broker) service URL of `pulsar://localhost:6650`. If you'd like to use local run mode to run a function but connect it to a non-local Pulsar cluster, you can specify a different broker URL using the `--brokerServiceUrl` flag. Here's an example:

```bash
$ bin/pulsar-admin functions localrun \
  --broker-service-url pulsar://my-cluster-host:6650 \
  # Other function parameters
```

## Cluster mode

When you run a Pulsar Function in **cluster mode**, the function code will be uploaded to a Pulsar broker and run *alongside the broker* rather than in your [local environment](#local-run-mode). You can run a function in cluster mode using the [`create`](reference-pulsar-admin.md#create-1) command. Here's an example:

```bash
$ bin/pulsar-admin functions create \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/input-1 \
  --output persistent://public/default/output-1
```

### Updating cluster mode functions

You can use the [`update`](reference-pulsar-admin.md#update-1) command to update a Pulsar Function running in cluster mode. This command, for example, would update the function created in the section [above](#cluster-mode):

```bash
$ bin/pulsar-admin functions update \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/new-input-topic \
  --output persistent://public/default/new-output-topic
```

### Parallelism

Pulsar Functions run as processes called **instances**. When you run a Pulsar Function, it runs as a single instance by default (and in [local run mode](#local-run-mode) you can *only* run a single instance of a function).

You can also specify the *parallelism* of a function, i.e. the number of instances to run, when you create the function. You can set the parallelism factor using the `--parallelism` flag of the [`create`](reference-pulsar-admin.md#functions-create) command. Here's an example:

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

If you're specifying a function's configuration via YAML, use the `parallelism` parameter. Here's an example config file:

```yaml
# function-config.yaml
parallelism: 3
inputs:
- persistent://public/default/input-1
output: persistent://public/default/output-1
# other parameters
```

And here's the corresponding update command:

```bash
$ bin/pulsar-admin functions update \
  --function-config-file function-config.yaml
```

### Function instance resources

When you run Pulsar Functions in [cluster run](#cluster-mode) mode, you can specify the resources that are assigned to each function [instance](#parallelism):

Resource | Specified as... | Runtimes
:--------|:----------------|:--------
CPU | The number of cores | Docker (coming soon)
RAM | The number of bytes | Process, Docker
Disk space | The number of bytes | Docker

Here's an example function creation command that allocates 8 cores, 8 GB of RAM, and 10 GB of disk space to a function:

```bash
$ bin/pulsar-admin functions create \
  --jar target/my-functions.jar \
  --classname org.example.functions.MyFunction \
  --cpu 8 \
  --ram 8589934592 \
  --disk 10737418240
```

> #### Resources are *per instance*
> The resources that you apply to a given Pulsar Function are applied to each [instance](#parallelism) of the function. If you apply 8 GB of RAM to a function with a parallelism of 5, for example, then you are applying 40 GB of RAM total for the function. You should always make sure to factor parallelism---i.e. the number of instances---into your resource calculations

## Triggering Pulsar Functions

If a Pulsar Function is running in [cluster mode](#cluster-mode), you can **trigger** it at any time using the command line. Triggering a function means that you send a message with a specific value to the function and get the function's output (if any) via the command line.

> Triggering a function is ultimately no different from invoking a function by producing a message on one of the function's input topics. The [`pulsar-admin functions trigger`](reference-pulsar-admin.md#trigger) command is essentially a convenient mechanism for sending messages to functions without needing to use the [`pulsar-client`](reference-cli-tools.md#pulsar-client) tool or a language-specific client library.

To show an example of function triggering, let's start with a simple [Python function](functions-api.md#functions-for-python) that returns a simple string based on the input:

```python
# myfunc.py
def process(input):
    return "This function has been triggered with a value of {0}".format(input)
```

Let's run that function in [local run mode](functions-deploying.md#local-run-mode):

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

Now let's make a consumer listen on the output topic for messages coming from the `myfunc` function using the [`pulsar-client consume`](reference-cli-tools.md#consume) command:

```bash
$ bin/pulsar-client consume persistent://public/default/out \
  --subscription-name my-subscription
  --num-messages 0 # Listen indefinitely
```

Now let's trigger that function:

```bash
$ bin/pulsar-admin functions trigger \
  --tenant public \
  --namespace default \
  --name myfunc \
  --trigger-value "hello world"
```

The consumer listening on the output topic should then produce this in its logs:

```
----- got message -----
This function has been triggered with a value of hello world
```

> #### Topic info not required
> In the `trigger` command above, you may have noticed that you only need to specify basic information about the function (tenant, namespace, and name). To trigger the function, you didn't need to know the function's input topic(s).

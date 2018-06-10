---
title: Deploying and managing Pulsar Functions
lead: A guide to Pulsar Functions from an operational perspective
preview: true
---

At the moment, there are two deployment modes available for Pulsar Functions:

Mode | Description
:----|:-----------
Local run mode | The function runs in your local environment, for example on your laptop
Cluster mode | The function runs *inside of* your Pulsar cluster, on the same machines as your Pulsar {% popover brokers %}

{% include admonition.html type="info" title="Contributing new deployment modes" content="The Pulsar Functions feature was designed, however, with extensibility in mind. Other deployment options will be available in the future. If you'd like to add a new deployment option, we recommend getting in touch with the Pulsar developer community at [dev@pulsar.incubator.apache.org](mailto:dev@pulsar.incubator.apache.org)." %}

## Requirements

In order to deploy and manage Pulsar Functions, you need to have a Pulsar {% popover cluster %} running. There are several options for this:

* You can run a [standalone cluster](../../getting-started/LocalCluster) locally on your own machine
* You can deploy a Pulsar cluster on [Kubernetes](../../deployment/Kubernetes), [Amazon Web Services](../../deployment/aws-cluster), [bare metal](../../deployment/instance), [DC/OS](../../deployment/dcos), and more

If you're running a non-{% popover standalone %} cluster, you'll need to obtain the service URL for the cluster. How you obtain the service URL will depend on how you deployed your Pulsar cluster.

## Command-line interface {#cli}

Pulsar Functions are deployed and managed using the [`pulsar-admin functions`](../../reference/CliTools#pulsar-admin-functions) interface, which contains commands such as [`create`](../../reference/CliTools#pulsar-admin-functions-create) for deploying functions in [cluster mode](#cluster-mode), [`trigger`](../../reference/CliTools#pulsar-admin-functions-trigger) for [triggering](#triggering) functions, [`list`](../../reference/CliTools#pulsar-admin-functions-list) for listing deployed functions, and several others.

### Fully Qualified Function Name (FQFN) {#fqfn}

Each Pulsar Function has a **Fully Qualified Function Name** (FQFN) that consists of three elements: the function's {% popover tenant %}, {% popover namespace %}, and function name. FQFN's look like this:

{% include fqfn.html tenant="tenant" namespace="namespace" name="name" %}

FQFNs enable you to, for example, create multiple functions with the same name provided that they're in different namespaces.

### Default arguments

When managing Pulsar Functions, you'll need to specify a variety of information about those functions, including {% popover tenant %}, {% popover namespace %}, input and output topics, etc. There are some parameters, however, that have default values that will be supplied if omitted. The table below lists the defaults:

Parameter | Default
:---------|:-------
Function name | Whichever value is specified for the class name (minus org, library, etc.). The flag `--className org.example.MyFunction`, for example, would give the function a name of `MyFunction`.
Tenant | Derived from the input topics' names. If the input topics are under the `marketing` tenant---i.e. the topic names have the form `persistent://marketing/{namespace}/{topicName}`---then the tenant will be `marketing`.
Namespace | Derived from the input topics' names. If the input topics are under the `asia` namespace under the `marketing` tenant---i.e. the topic names have the form `persistent://marketing/asia/{topicName}`, then the namespace will be `asia`.
Output topic | `{input topic}-{function name}-output`. A function with an input topic name of `incoming` and a function name of `exclamation`, for example, would have an output topic of `incoming-exclamation-output`.
Subscription type | For at-least-once and at-most-once [processing guarantees](../guarantees), the [`SHARED`](../../getting-started/ConceptsAndArchitecture#shared) is applied by default; for effectively-once guarantees, [`FAILOVER`](../../getting-started/ConceptsAndArchitecture#failover) is applied
Processing guarantees | [`ATLEAST_ONCE`](../guarantees)
Pulsar service URL | `pulsar://localhost:6650`

#### Example use of defaults

Take this `create` command:

```bash
$ bin/pulsar-admin functions create \
  --jar my-pulsar-functions.jar \
  --className org.example.MyFunction \
  --inputs my-function-input-topic1,my-function-input-topic2
```

The created function would have default values supplied for the function name (`MyFunction`), tenant (`public`), namespace (`default`), subscription type (`SHARED`), processing guarantees (`ATLEAST_ONCE`), and Pulsar service URL (`pulsar://localhost:6650`).

## Local run mode {#local-run}

If you run a Pulsar Function in **local run** mode, it will run on the machine from which the command is run (this could be your laptop, an [AWS EC2](https://aws.amazon.com/ec2/) instance, etc.). Here's an example [`localrun`](../../CliTools#pulsar-admin-functions-localrun) command:

```bash
$ bin/pulsar-admin functions localrun \
  --py myfunc.py \
  --className myfunc.SomeFunction \
  --inputs persistent://public/default/input-1 \
  --output persistent://public/default/output-1
```

By default, the function will connect to a Pulsar cluster running on the same machine, via a local {% popover broker %} service URL of `pulsar://localhost:6650`. If you'd like to use local run mode to run a function but connect it to a non-local Pulsar cluster, you can specify a different broker URL using the `--brokerServiceUrl` flag. Here's an example:

```bash
$ bin/pulsar-admin functions localrun \
  --brokerServiceUrl pulsar://my-cluster-host:6650 \
  # Other function parameters
```

## Cluster mode

When you run a Pulsar Function in **cluster mode**, the function code will be uploaded to a Pulsar {% popover broker %} and run *alongside the broker* rather than in your [local environment](#local-run). You can run a function in cluster mode using the [`create`](../../CliTools#pulsar-admin-functions-create) command. Here's an example:

```bash
$ bin/pulsar-admin functions create \
  --py myfunc.py \
  --className myfunc.SomeFunction \
  --inputs persistent://public/default/input-1 \
  --output persistent://public/default/output-1
```

### Updating cluster mode functions {#updating}

You can use the [`update`](../../CliTools#pulsar-admin-functions-update) command to update a Pulsar Function running in cluster mode. This command, for example, would update the function created in the section [above](#cluster-mode):

```bash
$ bin/pulsar-admin functions update \
  --py myfunc.py \
  --className myfunc.SomeFunction \
  --inputs persistent://public/default/new-input-topic \
  --output persistent://public/default/new-output-topic
```

### Parallelism

Pulsar Functions run as processes called **instances**. When you run a Pulsar Function, it runs as a single instance by default (and in [local run mode](#local-run) you can *only* run a single instance of a function).

You can also specify the *parallelism* of a function, i.e. the number of instances to run, when you create the function. You can set the parallelism factor using the `--parallelism` flag of the [`create`](../../references/CliTools#pulsar-admin-functions-create) command. Here's an example:

```bash
$ bin/pulsar-admin functions create \
  --parallelism 3 \
  # Other function info
```

You can adjust the parallelism of an already created function using the [`update`](../../reference/CliTools#pulsar-admin-functions-update) interface.

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
  --functionConfigFile function-config.yaml
```

### Function instance resources {#resources}

When you run Pulsar Functions in [cluster run](#cluster-run) mode, you can specify the resources that are assigned to each function [instance](#parallelism):

Resource | Specified as... | Runtimes
:--------|:----------------|:--------
CPU | The number of cores | Docker (coming soon)
RAM | The number of bytes | Process, Docker
Disk space | The number of bytes | Docker

Here's an example function creation command that allocates 8 cores, 8 GB of RAM, and 10 GB of disk space to a function:

```bash
$ bin/pulsar-admin functions create \
  --jar target/my-functions.jar \
  --className org.example.functions.MyFunction \
  --cpu 8 \
  --ram 8589934592 \
  --disk 10737418240
```

{% include admonition.html type="warning" title="Resources are *per instance*"
   content="The resources that you apply to a given Pulsar Function are applied to each [instance](#parallelism) of the function. If you apply 8 GB of RAM to a function with a paralellism of 5, for example, then you are applying 40 GB of RAM total for the function. You should always make sure to factor paralellism---i.e. the number of instances---into your resource calculations." %}

## Triggering Pulsar Functions {#triggering}

If a Pulsar Function is running in [cluster mode](#cluster-mode), you can **trigger** it at any time using the command line. Triggering a function means that you send a message with a specific value to the function and get the function's output (if any) via the command line.

{% include admonition.html type="info" content="Triggering a function is ultimately no different from invoking a function by producing a message on one of the function's input topics. The [`pulsar-admin functions trigger`](../../CliTools#pulsar-admin-functions-trigger) command is essentially a convenient mechanism for sending messages to functions without needing to use the [`pulsar-client`](../../CliTools#pulsar-client) tool or a language-specific client library." %}

To show an example of function triggering, let's start with a simple [Python function](../api#python) that returns a simple string based on the input:

```python
# myfunc.py
def process(input):
    return "This function has been triggered with a value of {0}".format(input)
```

Let's run that function in [local run mode](../deployment#local-run):

```bash
$ bin/pulsar-admin functions create \
  --tenant public \
  --namespace default \
  --name myfunc \
  --py myfunc.py \
  --className myfunc \
  --inputs persistent://public/default/in \
  --output persistent://public/default/out
```

Now let's make a consumer listen on the output topic for messages coming from the `myfunc` function using the [`pulsar-client consume`](../../CliTools#pulsar-client-consume) command:

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
  --triggerValue "hello world"
```

The consumer listening on the output topic should then produce this in its logs:

```
----- got message -----
This function has been triggered with a value of hello world
```

{% include admonition.html type="success" title="Topic info not required" content="In the `trigger` command above, you may have noticed that you only need to specify basic information about the function (tenant, namespace, and name). To trigger the function, you didn't need to know the function's input topic(s)." %}

<!--
## Subscription types

Pulsar supports three different [subscription types](../../getting-started/ConceptsAndArchitecture#subscription-modes) (or subscription modes) for Pulsar clients:

* With [exclusive](../../getting-started/ConceptsAndArchitecture#exclusive) subscriptions, only a single {% popover consumer %} is allowed to attach to the subscription.
* With [shared](../../getting-started/ConceptsAndArchitecture#shared) . Please note that strict message ordering is *not* guaranteed with shared subscriptions.
* With [failover](../../getting-started/ConceptsAndArchitecture#failover) subscriptions

Pulsar Functions can also be assigned a subscription type when you [create](#cluster-mode) them or run them [locally](#local-run). In cluster mode, the subscription can also be [updated](#updating) after the function has been created.
-->

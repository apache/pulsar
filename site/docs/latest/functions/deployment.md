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

## Local run mode {#local-run}

If you run a Pulsar Function in **local run** mode, it will run on the machine from which the command is run (this could be your laptop, an [AWS EC2](https://aws.amazon.com/ec2/) instance, etc.). Here's an example [`localrun`](../../CliTools#pulsar-admin-functions-localrun) command:

```bash
$ bin/pulsar-admin functions localrun \
  --py myfunc.py \
  --className myfunc.SomeFunction \
  --inputs persistent://sample/standalone/ns1/input-1 \
  --output persistent://sample/standalone/ns1/output-1
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
  --inputs persistent://sample/standalone/ns1/input-1 \
  --output persistent://sample/standalone/ns1/output-1
```

### Updating cluster mode functions {#updating}

You can use the [`update`](../../CliTools#pulsar-admin-functions-update) command to update a Pulsar Function running in cluster mode. This command, for example, would update the function created in the section [above](#cluster-mode):

```bash
$ bin/pulsar-admin functions update \
  --py myfunc.py \
  --className myfunc.SomeFunction \
  --inputs persistent://sample/standalone/ns1/new-input-topic \
  --output persistent://sample/standalone/ns1/new-output-topic
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
- persistent://sample/standalone/ns1/input-1
output: persistent://sample/standalone/ns1/output-1
# other parameters
```

And here's the corresponding update command:

```bash
$ bin/pulsar-admin functions update \
  --functionConfigFile function-config.yaml
```

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
  --tenant sample \
  --namespace ns1 \
  --name myfunc \
  --py myfunc.py \
  --className myfunc \
  --inputs persistent://sample/standalone/ns1/in \
  --output persistent://sample/standalone/ns1/out
```

Now let's make a consumer listen on the output topic for messages coming from the `myfunc` function using the [`pulsar-client consume`](../../CliTools#pulsar-client-consume) command:

```bash
$ bin/pulsar-client consume persistent://sample/standalone/ns1/out \
  --subscription-name my-subscription
  --num-messages 0 # Listen indefinitely
```

Now let's trigger that function:

```bash
$ bin/pulsar-admin functions trigger \
  --tenant sample \
  --namespace ns1 \
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

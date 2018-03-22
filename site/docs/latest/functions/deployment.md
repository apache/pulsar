---
title: Deploying Pulsar Functions
new: true
---

At the moment, there are two deployment modes available for Pulsar Functions:

Mode | Description
:----|:-----------
Local run mode | The function runs in your local environment, for example on your laptop
Cluster mode | The function runs *inside of* your Pulsar cluster, on the same machines as your Pulsar {% popover brokers %}

{% include admonition.html type="info" title="Contributing new deployment modes" content="The Pulsar Functions feature was designed, however, with extensibility in mind. Other deployment options will be available in the future. If you'd like to add a new deployment option, we recommend getting in touch with the Pulsar developer community at [dev@pulsar.incubator.apache.org](mailto:dev@pulsar.incubator.apache.org)." %}

## Local run mode {#local-run}

If you run a Pulsar Function in **local run** mode, it will run on the local machine (this could be your laptop, an [AWS EC2](https://aws.amazon.com/ec2/) instance, etc.). Here's an example [`localrun`](../../CliTools#pulsar-admin-functions-localrun) command:

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

### Parallelism

Pulsar Functions run as processes called **instances**. When you run a Pulsar Function, it runs as a single instance by default (and in [local run mode](#local-run) you can *only* run a single instance of a function). You can also specify the parallelism of a function (i.e. the number of instances to run) via the [command line](../../references/CliTools#pulsar-admin-functions). Here's an example:

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

If you're specifying a function's configuration via YAML, using the `parallelism` parameter. Here's an example:

```yaml
parallelism: 3
inputs:
- persistent://sample/standalone/ns1/input-1
output: persistent://sample/standalone/ns1/output-1
# other parameters
```
---
id: functions-deploy
title: Pulsar Functions Deploy
sidebar_label: Deploy
---

## Requirements

To deploy and manage Pulsar Functions, you need to have a Pulsar cluster running. There are several options for this:

* You can run a [standalone cluster](getting-started-standalone.md) locally on your own machine.
* You can deploy a Pulsar cluster on [Kubernetes](deploy-kubernetes.md), [Amazon Web Services](deploy-aws.md), [bare metal](deploy-bare-metal.md), [DC/OS](deploy-dcos.md), and more.

If you run a non-[standalone](reference-terminology.md#standalone) cluster, you need to obtain the service URL for the cluster. How you obtain the service URL depends on how you deploy your Pulsar cluster.

If you want to deploy and trigger python user-defined functions, you need to install [the pulsar python client](http://pulsar.apache.org/docs/en/client-libraries-python/) first.


## Command-line interface

Pulsar Functions are deployed and managed using the [`pulsar-admin functions`](reference-pulsar-admin.md#functions) interface, which contains commands such as [`create`](reference-pulsar-admin.md#functions-create) for deploying functions in [cluster mode](#cluster-mode), [`trigger`](reference-pulsar-admin.md#trigger) for [triggering](#triggering-pulsar-functions) functions, [`list`](reference-pulsar-admin.md#list-2) for listing deployed functions.

To learn more commands, refer to [`pulsar-admin functions`](reference-pulsar-admin.md#functions).

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

When you run a Pulsar Function in **cluster mode**, the function code is uploaded to a Pulsar broker and runs *alongside the broker* rather than in your [local environment](#local-run-mode). You can run a function in cluster mode using the [`create`](reference-pulsar-admin.md#create-1) command. 

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

Pulsar Functions run as processes called **instances**. When you run a Pulsar Function, it runs as a single instance by default. In [local run mode](#local-run-mode), you can *only* run a single instance of a function.

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

Resource | Specified as... | Runtimes
:--------|:----------------|:--------
CPU | The number of cores | Docker (coming soon)
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
> The resources that you apply to a given Pulsar Function are applied to each [instance](#parallelism) of the function. For example, if you apply 8 GB of RAM to a function with a parallelism of 5, you are applying 40 GB of RAM for the function in total. Make sure that you take the parallelism (the number of instances) factor into your resource calculations.

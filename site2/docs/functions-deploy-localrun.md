---
id: functions-deploy-localrun
title: Deploy a function in localrun mode
sidebar_label: "Deploy a function in localrun mode"
---

When you deploy a function in localrun mode, it runs on the machine where you enter the commands – on your laptop, for example, or in an [AWS EC2](https://aws.amazon.com/ec2/) instance. 

You can use the `localrun` command to run a single instance of a function. To run multiple instances, you can use the `localrun` command multiple times. 

The following is an example of how to use the `localrun` command.

```bash

bin/pulsar-admin functions localrun \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/input-1 \
  --output persistent://public/default/output-1

```

:::note

In localrun mode, Java functions use thread runtime; Python and Go functions use process runtime.

:::

By default, the function connects with a Pulsar cluster running on the same machine via a local broker service URL. If you want to connect it to a non-local Pulsar cluster, you can specify a different broker service URL using the `--brokerServiceUrl` flag.

```bash

bin/pulsar-admin functions localrun \
  --broker-service-url pulsar://my-cluster-host:6650 \
  # Other function parameters

```

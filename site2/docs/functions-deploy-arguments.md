---
id: functions-deploy-arguments
title: Default arguments of CLI
sidebar_label: "Default arguments of CLI"
---

You can use function-related commands in the [`pulsar-admin`](/tools/pulsar-admin/) CLI to deploy functions. Pulsar provides a variety of commands, such as: 
* `create` command for deploying functions in [cluster mode](functions-deploy-cluster.md)
* `trigger` command for [triggering](functions-deploy-trigger.md) functions

The following table lists the parameters required in CLI and their default values.

| Parameter | Default value| 
|----------|----------------| 
| Function name | N/A <br />You can specify any value for the function name (except org, library, or similar class names). 
Tenant | N/A <br />The value is derived from the name of the input topics. For example, if the input topic form is `persistent://marketing/{namespace}/{topicName}`, the tenant name is `marketing`.| 
| Namespace | N/A <br />The value is derived from the input topic name. If the input topic form is `persistent://marketing/asia/{topicName}`, the namespace is `asia`.| 
| Output topic | `{input topic}-{function name}-output`. For example, if an input topic name of a function is `incoming` and the function name is `exclamation`, the output topic name is `incoming-exclamation-output`.| 
| [Processing guarantees](functions-concepts.md#processing-guarantees-and-subscription-types) | `ATLEAST_ONCE` |
| Pulsar service URL | `pulsar://localhost:6650`| 


Take the `create` command for example. The following function has default values for the function name (`MyFunction`), tenant (`public`), namespace (`default`), subscription type (`SHARED`), processing guarantees (`ATLEAST_ONCE`), and Pulsar service URL (`pulsar://localhost:6650`).

```bash

bin/pulsar-admin functions create \
  --jar my-pulsar-functions.jar \
  --classname org.example.MyFunction \
  --inputs my-function-input-topic1,my-function-input-topic2

```



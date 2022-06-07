---
id: functions-deploy-cluster
title: Deploy a function in cluster mode
sidebar_label: "Deploy a function in cluster mode"
---

Deploying a function in cluster mode uploads the function to a function worker, which means the function is scheduled by the worker. 

To deploy a function in cluster mode, use the `create` command. 

```bash

bin/pulsar-admin functions create \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/input-1 \
  --output persistent://public/default/output-1

```

To update a function running in cluster mode, you can use the `update` command.

```bash

bin/pulsar-admin functions update \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/new-input-topic \
  --output persistent://public/default/new-output-topic

```

**More options**
* [Allocate resources to function instance](functions-deploy-cluster-resource.md)
* [Enable parallel processing](functions-deploy-cluster-parallelism.md)
* [Enable end-to-end encryption](functions-deploy-cluster-encryption.md)
* [Enable package management service](functions-deploy-cluster-package.md)


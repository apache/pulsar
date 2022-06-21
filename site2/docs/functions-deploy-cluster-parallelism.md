---
id: functions-deploy-cluster-parallelism
title: Enable parallel processing
sidebar_label: "Enable parallel processing"
---

In cluster mode, you can specify the **parallelism** (the number of instances to run) to enable parallel processing for a function.

**Example1**

Specify the `--parallelism` flag of the `create` command when deploying a function. 

```bash

bin/pulsar-admin functions create \
  --parallelism 3 \
  # Other function info

```

:::tip

For an existing function, you can adjust the parallelism by using the `update` command.

:::


**Example2**

**Specify the `parallelism` parameter when deploying a function configuration through YAML.

```yaml

# function-config.yaml
parallelism: 3
inputs:
- persistent://public/default/input-1
output: persistent://public/default/output-1
# other parameters

```

For an existing function, you can adjust the parallelism by using the `update` command as follows.

```bash

bin/pulsar-admin functions update \
  --function-config-file function-config.yaml

```


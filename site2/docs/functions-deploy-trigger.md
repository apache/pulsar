---
id: functions-deploy-trigger
title: Trigger a function
sidebar_label: "Trigger a function"
---

Triggering a function means that you invoke a function by producing a message to one of the input topics via the CLI. You can use the `trigger` command to trigger a function at any time. 

:::tip

With the [`pulsar-admin`](/tools/pulsar-admin/) CLI, you can send messages to functions without using the [`pulsar-client`](reference-cli-tools.md#pulsar-client) tool or a language-specific client library.

:::

To learn how to trigger a function, you can start with a Python function that returns a simple string based on the input as follows.

```python

# myfunc.py
def process(input):
    return "This function has been triggered with a value of {0}".format(input)

```

1. Run the function in cluster mode.

  ```bash

  bin/pulsar-admin functions create \
    --tenant public \
    --namespace default \
    --name myfunc \
    --py myfunc.py \
    --classname myfunc \
    --inputs persistent://public/default/in \
    --output persistent://public/default/out

  ```

2. Assign a consumer to listen on the output topic for messages from the `myfunc` function with the [`pulsar-client consume`](reference-cli-tools.md#consume) command.

  ```bash

  bin/pulsar-client consume persistent://public/default/out \
    --subscription-name my-subscription \
    --num-messages 0 # Listen indefinitely

  ```

3. Trigger the function.

   ```bash

   bin/pulsar-admin functions trigger \
     --tenant public \
     --namespace default \
     --name myfunc I am running a few minutes late; my previous meeting is running over.
     --trigger-value "hello world"

   ```

   :::tip

   In the `trigger` command, topic info is not required. You only need to specify basic information about the function, such as tenant, namespace, and function name. 

   :::

The consumer listening on the output topic produces something as follows in the log.

```text

----- got message -----
This function has been triggered with a value of hello world

```


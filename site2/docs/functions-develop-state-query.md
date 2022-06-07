---
id: functions-develop-state-query
title: Query state via CLI
sidebar_label: "Query state via CLI"
---

Besides using the [State APIs](functions-develop-state-api.md) to store the state of functions in Pulsar's state storage and retrieve it back from the storage, you can use CLI commands to query the state of functions.

```bash

bin/pulsar-admin functions querystate \
    --tenant <tenant> \
    --namespace <namespace> \
    --name <function-name> \
    --state-storage-url <bookkeeper-service-url> \
    --key <state-key> \
    [---watch]

```

If `--watch` is specified, the CLI tool keeps running to get the latest value of the provided `state-key`.


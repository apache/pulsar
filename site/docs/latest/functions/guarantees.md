---
title: Processing guarantees
lead: Apply at-most-once, at-least-once, or effectively-once delivery semantics to Pulsar Functions
---

Pulsar Functions provides three different messaging semantics that you can apply to any Function:

* **At-most-once** delivery
* **At-least-once** delivery
* **Effectively-once** delivery

## How it works

You can set the processing guarantees for a Pulsar Function when you create the Function. This [`pulsar-function create`](../../reference/CliTools#pulsar-functions-create) command, for example, would apply effectively-once guarantees to the Function:

```bash
$ bin/pulsar-functions \
  # TODO
  --processingGuarantees EFFECTIVELY_ONCE
```

The available options are:

* `ATMOST_ONCE`
* `ATLEAST_ONCE`
* `EFFECTIVELY_ONCE`

{% include admonition.html type='info' content='By default, Pulsar Functions provide at-most-once delivery guarantees. If you create a function without supplying a value for the `--processingGuarantees`flag, then the Function will provide only at-most-once guarantees.' %}
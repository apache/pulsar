---
title: Tiered storage cookbook
tags: [tiered storage, s3, bookkeeper]
---

Pulsar's **tiered storage** feature enables you to offload message data from [BookKeeper](https://bookkeeper.apache.org) to another system. This cookbook walks you through using tiered storage in your Pulsar cluster.

{% include admonition.html type="info" content="For a more high-level, theoretical perspective on tiered storage, see the [Concepts and Architecture](../../getting-started/ConceptsAndArchitecture#tiered-storage) documentation. For a guide to creating custom tiered storage driver, see the [Custom tiered storage](../../project/tiered-storage) documentation." %}

## Supported tiered storage targets {#targets}

Pulsar currently supports the following tiered storage targets:

* [Amazon Simple Storage Service (S3)](#s3)

## Configuration

In order to use tiered storage in Pulsar, you'll need to adjust the {% popover broker %}-level configuration in each of your cluster's brokers. Broker configuration can be set in the [`broker.conf`](../../reference/Configuration#broker) file in the `conf` directory of your Pulsar installation. In order to use tiered storage, you'll first need to specify a ledger offload driver using the `managedLedgerOffloadDriver` parameter, using the name of the driver.

The following drivers are available:

Driver | Configuration name
:------|:------------------
[Amazon S3](#s3) | `S3`

In addition to specifying a driver, you can also specify the number of threads used by ledger-offloading-related processes using the `managedLedgerOffloadMaxThreads` parameter. The default is 2.

Here's an example configuration:

```conf
# Other configs
managedLedgerOffloadDriver=S3
managedLedgerOffloadMaxThreads=5
```

## Amazon Simple Storage Service (S3) {#s3}

Set `managedLedgerOffloadDriver` to `S3`

Parameter | Description | Default
:---------|:------------|:-------
`s3ManagedLedgerOffloadRegion` | The AWS region used for tiered storage ledger offload |
`s3ManagedLedgerOffloadBucket` | The AWS bucket used for tiered storage ledger offload |
`s3ManagedLedgerOffloadServiceEndpoint` | The alternative service endpoint used for Amazon S3 tiered storage offload, which can be useful for testing |
`s3ManagedLedgerOffloadMaxBlockSizeInBytes` | The maximum block size for Amazon S3 ledger offloading (in bytes) | 67108864 (64 MB)
`s3ManagedLedgerOffloadReadBufferSizeInBytes` | The read buffer size for Amazon S3 ledger offloading (in bytes) | 1048576 (1 MB)

## Creating your own driver

The [Amazon S3 driver](#s3) for tiered storage is the only driver that's currently available. You can also, however, create and run your own driver by following the instructions in the [Custom tiered storage](../../project/tiered-storage) documentation.
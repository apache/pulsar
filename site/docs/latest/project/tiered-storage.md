---
title: Custom tiered storage
tags: [tiered storage, s3, bookkeeper, storage, stream storage]
---

[`LedgerOffloader`](https://github.com/apache/incubator-pulsar/blob/master/managed-ledger/src/main/java/org/apache/bookkeeper/mledger/LedgerOffloader.java)

```java
interface LedgerOffloader {
    // The "write" action that offloads the BookKeeper ledger to the external system
    CompletableFuture<Void> offload(
            // The identifier of the ledger
            ReadHandle ledger,
            // A unique ID for the offload attempt
            UUID uid,
            // Any metadata you'd like to add
            Map<String, String> extraMetadata);
    
    // The "read" action that retrieves the ledger from the external system
    CompletableFuture<ReadHandle> readOffloaded(

            long ledgerId,
            UUID uid);

    // The "delete" action that 
    CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid);
}
```

## Example implementation

The following implementations are currently available for your perusal:

* [`S3ManagedLedgerOffloader`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/s3offload/S3ManagedLedgerOffloader.java) (for Amazon S3)

## Deployment

Once you've created a `LedgerOffloader` implementation, you need to:

1. Package the implementation in a [JAR](https://docs.oracle.com/javase/tutorial/deployment/jar/basicsindex.html) file.
1. Add that jar to the `lib` folder in your Pulsar [binary or source distribution](../../getting-started/LocalCluster#installing-pulsar).
1. Change the `managedLedgerOffloadDriver` configuration in [`broker.conf`](../../reference/Configuration#broker) to your custom class (e.g. `org.example.MyCustomLedgerOffloader`).
1. Start up Pulsar.
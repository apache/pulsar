# PIP-335: Support Oxia metadata store plugin

# Motivation
 
Oxia is a scalable metadata store and coordination system that can be used as the core infrastructure 
to build large scale distributed systems.

Oxia was created with the primary goal of providing an alternative Pulsar to replace ZooKeeper as the
long term preferred metadata store, overcoming all the current limitations in terms of metadata 
access throughput and data set size.

# Goals

Add a Pulsar MetadataStore plugin that uses Oxia client SDK.

Users will be able to start a Pulsar cluster using just Oxia, without any ZooKeeper involved.

## Not in Scope

It's not in the scope of this proposal to change any default behavior or configuration of Pulsar.

# Detailed Design

## Design & Implementation Details

Oxia semantics and client SDK were already designed with Pulsar and MetadataStore plugin API in mind, so
there is not much integration work that needs to be done here.

Just few notes:
 1. Oxia client already provides support for transparent batching of read and write operations,
    so there will be no use of the batching logic in `AbstractBatchedMetadataStore`
 2. Oxia does not treat keys as a walkable file-system like interface, with directories and files. Instead
    all the keys are independent. Though Oxia sorting of keys is aware of '/' and provides efficient key 
    range scanning operations to identify the first level children of a given key
 3. Oxia, unlike ZooKeeper, doesn't require the parent path of a key to exist. eg: we can create `/a/b/c` key
    without `/a/b` and `/a` existing. 
    In the Pulsar integration for Oxia we're forcing to create all parent keys when they are not there. This
    is due to several places in BookKeeper access where it does not create the parent keys, though it will
    later make `getChildren()` operations on the parents.
    
## Other notes

Unlike in the ZooKeeper implementation, the notification of events is guaranteed in Oxia, because the Oxia 
client SDK will use the transaction offset after server reconnections and session restarted events. This
will ensure that brokers cache will always be properly invalidated. We will then be able to remove the 
current 5minutes automatic cache refresh which is in place to prevent the ZooKeeper missed watch issue.

# Links

Oxia: https://github.com/streamnative/oxia
Oxia Java Client SDK: https://github.com/streamnative/oxia-java

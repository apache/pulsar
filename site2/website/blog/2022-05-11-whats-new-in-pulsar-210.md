---
title: "What’s New in Apache Pulsar 2.10"
date: 2022-05-11
author: "Penghui Li, Dave Duggins"
---

The Apache Pulsar community releases version 2.10. 99 contributors provided improvements and bug fixes that delivered over 800 commits.

<!--truncate-->

# Highlights of this release:

- Pulsar provides automatic failure recovery between the primary and backup clusters. #13316
  - Original PIP [#13315](https://www.google.com/url?q=https://github.com/apache/pulsar/issues/13315&sa=D&source=docs&ust=1646058957138073&usg=AOvVaw3mGki2sHW2QpIsoYf5pt3w)
- Fewer producers needed and more efficient use of broker memory with lazy-loading feature added to `PartitionedProducer`. #10279
- Topic map support added with new `TableView` type using key values in received messages.

This blog documents the most noteworthy changes in this release. For the complete list including all features, enhancements, and bug fixes, check out the [Pulsar 2.10.1 Release Notes](https://pulsar.apache.org/release-notes/#placeholder).

# Notable bug fixes and enhancements
***
#### Cluster
***

##### Pulsar cluster level auto failover on client side #13316

**Issue:** A Pulsar administrator must manually failover a cluster.

**Resolution:** Added Pulsar cluster-level auto-failover, which automatically and seamlessly switches from primary to one or more secondary clusters when a failover event is detected. When the primary cluster recovers, the client automatically switches back.

##### Topic policy across multiple clusters #12517

**Issue:** Some topic policies for a geo-replicated cluster affect the entire geo-replicated cluster while some only affect the local cluster. 

**Resolution:** Topic policies now support cross-cluster replication. 
- For local topic policies, set the `replicateTo` property of the message to avoid being replicated to the remote.
- Retention supports setting global parameters.
- Added global topic policies for `SystemTopicBasedTopicPoliciesService`. 

***
#### Producer
***

##### Add lazy-loading feature to PartitionedProducer #10279

**Issue:** With the number of partitions set according to the highest rate producer, the lowest rate producer does not always need to connect to every partition, so extra producers take up broker memory.

**Resolution:** Reduced the number of producers to use broker memory more efficiently by introducing lazy-loading for partitioned producers; also added round-robin routing mode class to limit the number of partitions.

##### [Client] Introduce chunk message ID #12403

**Issue:** When sending chunked messages, the producer returns the message-id of the last chunk, causing incorrect behaviors in some processes.

**Resolution:** Introduced the new `ChunkMessage-ID` type. The chunk message-id inherits from ``MessageIdImpl`` and adds two new methods: ``getFirstChunkMessageId`` and ``getLastChunkMessageID``. For other method implementations, the ``lastChunkMessageID`` is called directly, which is compatible with much of the existing business logic. 

***
#### Broker
***

##### Broker extensions to allow operators of enterprise wide cluster better control and flexibility #12536

**Issue:** Operators of enterprise Pulsar cluster(s) need greater flexibility and control to intercept broker events (including ledger writes/reads) for template validations, observability and access control.

**Resolution:** 
- Enhanced org.apache.pulsar.broker.intercept.BrokerInterceptor interface to include additional events for tracing
- Created a new interface org.apache.pulsar.common.intercept.MessagePayloadProcessor to allow interception of ledger write/read operations
- Enhanced PulsarAdmin to give operators a control in managing super-users

***
#### Consumer
***

##### Redeliver command add epoch #10478

**Issue:** Pull and redeliver operations are asynchronous, so the client consumer may receive a new message, execute a cumulative ack based on a new messageID, and fail to consume older messages. 

**Resolution:** The Pulsar client synchronizes redeliver and pull messages operations using an incrementing epoch for the server and client consumer. 

##### Support pluggable entry filter in Dispatcher #12269

**Issue:** Message tagging is not natively supported. 

**Resolution:** Implemented an entry filter framework at the broker level.  Working to support namespace and topic level in an upcoming release.

##### Create init subscription before sending message to DLQ #13355

**Issue:** DLQ data in unprocessed messages is removed automatically without a data retention policy for the namespace or a subscription for the DLQ.  

**Resolution:** Initial subscription is now created before sending messages to the DLQ.
When ``deadLetterProducer`` is initialized, the consumer sets the initial subscription according to ``DeadLetterPolicy``.

##### Apply redelivery backoff policy for ack timeout #13707

**Issue:** The redelivery backoff policy recently introduced in PIP 106 only applies to the negative acknowledgment API. If ack timeout is used to trigger the message
redelivery instead of the negative acknowledgment API, the backoff policy is bypassed.

**Resolution:** 
- Applied message redelivery policy for ack timeout.
- Alerted ``NegativeAckBackoff`` interface to ``RedeliveryBackoff``.
- Exposed ``AckTimeoutRedeliveryBackoff`` in ``ConsumerBuilder``.
- Added unit test case.

Currently only the Java client is modified.

##### Resolve produce chunk messages failed when topic level maxMessageSize is set #13599

**Issue:** Currently, chunk messages produce fails if topic level maxMessageSize is set to [1]. 

**Resolution:** Added ``isChunked`` in ``PublishContext``. Skips the``maxMessageSize`` check if it's chunked.

***
#### Function
***

##### Pulsar Functions: Preload and release external resources #13205

**Issue:**  External resource initialization and release was accomplished either manually or through use of a complicated initialization logic.

**Resolution:** Introduced ``RichFunction`` interface to extend ``Function`` by providing a setup and tearDown API. 

***
##### Update Authentication Interfaces to Include Async Authentication Methods #12104

**Issue:** Pulsar's current ``AuthenticationProvider`` interface only exposes synchronous methods for authenticating a connection. To date, this has been sufficient because we do not have any providers that rely on network calls. However, in looking at the OAuth2.0 spec, there are some cases where network calls are necessary to verify a token.

**Resolution:** 
###### AuthenticationProvider
- Added ``AuthenticationProvider#authenticateAsync``. Included a default implementation that calls the authenticate method.
- Deprecated ``AuthenticationProvider#authenticate``.
- Added ``AuthenticationProvider#authenticateHttpRequestAsync``.
- Deprecated ``AuthenticationProvider#authenticateHttpRequest``.
##### AuthenticationState
- Added ``AuthenticationState#authenticateAsync``.
- Deprecated ``AuthenticationState#authenticate``. The preferred method is ``AuthenticationState#authenticateAsync``.
- Deprecated ``AuthenticationState#isComplete``. This method can be avoided by inferring authentication completeness from the result of ``AuthenticationState#authenticateAsync``.
##### AuthenticationDataSource
- Deprecated ``AuthenticationDataSource#authenticate``. There is no need for an async version of this method.

***
##### Initial commit for TableView #12838

**Issue:** In many use cases, applications use Pulsar consumers or readers to fetch
all the updates from a topic and construct a map with the latest value of each
key for received messages. This is common when constructing a local cache of the data. We do not offer support for This access pattern was not included in the Pulsar client API. 

**Resolution:** Added new ``TableView`` type and updated the PulsarClient.

***
#### Topic
***
##### Support Topic metadata - PART-1 create topic with properties #12818

**Issue:** Can’t store topic metadata.

**Resolution:** 
- Added new storage methods in topics.java.
- Added two new paths to REST API to reduce compatibility issues.

***
#### Metadata Store
***
##### Added Etcd MetadataStore implementation #13225

**Issue:** We’re working to add metadata backends that support non-Zookeeper implementations.

**Resolution:** Added Etcd support for:
- Batching of read/write requests
- Session watcher
- Lease manager



  





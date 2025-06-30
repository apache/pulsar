# PIP-431: Add Creation and Last Publish Timestamps to Topic Stats

## Abstract
This Pulsar Improvement Proposal (PIP) addresses an observability gap in Apache Pulsar by proposing the addition of two new fields to the topic statistics API: topicCreationTimeStamp and lastPublishTimeStamp. Currently, operators and developers lack a direct and efficient method to determine a topic's age or its most recent message activity, hindering effective lifecycle management, troubleshooting, and compliance auditing. This proposal outlines a plan to introduce these timestamps into the TopicStats object, accessible via the Admin REST API and pulsar-admin CLI. The topicCreationTimeStamp will be retrieved directly from the topic's underlying metadata node statistics, providing an immutable value for all topics. The lastPublishTimeStamp will be maintained as a high-performance, in-memory atomic value on the owning broker, with a robust recovery mechanism from the managed ledger to ensure accuracy across broker restarts and topic reloads. These additions will be implemented in a backward-compatible manner, providing essential lifecycle visibility without impacting system performance.

## Motivation
The primary motivation for this proposal is to enhance the operational observability of Apache Pulsar topics by providing fundamental lifecycle metadata. Administrators, developers, and automated systems currently face significant challenges in managing topics at scale due to the absence of two key data points: when a topic was created and when it last received a message. This lack of information complicates several critical operational tasks.

- **Compliance, Auditing, and Governance**: Many organizations operate under strict data governance and compliance regimes that mandate the tracking of data asset lifecycles. A Pulsar topic, as a container for data streams, is often considered such an asset. The ability to programmatically retrieve a topic's creation date is essential for auditing purposes.
- **Troubleshooting and Operational Insight**: During incident response or general debugging, understanding the context of a topic is crucial. Knowing when a topic was created can help correlate its appearance with specific application deployments or configuration changes. Knowing when it last received data helps differentiate between a producer-side issue (no data being sent) and a consumer-side issue (data sent but not processed).
- **Insufficiency of Existing Metrics**:
  The current set of topic statistics, while comprehensive for performance monitoring, does not adequately address these lifecycle management use cases. Several existing fields might seem related but are not suitable substitutes:
  - lastLedgerCreatedTimestamp: This field, available in the internal topic stats, tracks the creation time of the current ledger being written to. Since topics can have many ledgers over their lifetime due to rollovers, this timestamp changes frequently and does not represent the topic's original creation time. 
  - connectedSince, lastConsumedTimestamp, lastAckedTimestamp: These timestamps are associated with specific producer and consumer connections or subscription activity. They are transient and only reflect the state of currently active clients. A topic can hold persisted data and be perfectly valid without any clients connected, rendering these metrics useless for assessing the topic's intrinsic state.

This proposal aims to fill this observability gap by introducing dedicated, reliable timestamps for topic creation and last publish activity, thereby empowering users with the necessary tools for more sophisticated and automated management of their Pulsar deployments.

## Proposed Changes
This proposal introduces two new fields to the data structures that represent topic statistics. These fields will be exposed in the JSON response of the Admin API endpoints for both non-partitioned and partitioned topics

The new fields are defined as follows:

- `topicCreationTimeStamp`: A long value representing the UTC timestamp in epoch milliseconds when the topic was first durably created in the metadata store. This value is immutable for the lifetime of the topic.
- `lastPublishTimeStamp`: A long value representing the UTC timestamp in epoch milliseconds corresponding to the publish_time field of the last message successfully persisted by the broker for this topic. This value is updated upon every successful message publication. If no message has ever been published to the topic, this field will return 0 Upon topic load (e.g., after a broker restart), this value will be recovered from the last entry in the managed ledger; until then, it may temporarily be 0.

## High-Level Design
The implementation strategy for the two new fields is designed to balance durability, accuracy, and performance, leveraging existing Pulsar components and patterns to minimize complexity.

### topicCreationTimeStamp: Metadata Store Retrieval
The topicCreationTimeStamp is an intrinsic property of the metadata node (e.g., a z-node in ZooKeeper) that represents the topic's managed ledger. This timestamp is automatically recorded by the metadata store upon the node's creation and is available for all topics, regardless of when they were created.

- **Storage Mechanism**: The timestamp is not stored as a separate property but is part of the standard metadata maintained by the metadata store for every node. The org.apache.pulsar.metadata.api.Stat object, which can be retrieved for any metadata path, contains the creation timestamp (ctime).
- **Write Path**: No explicit write path is needed for the timestamp itself. The underlying metadata store (e.g., ZooKeeper, etcd) is responsible for setting the creation timestamp atomically when the managed ledger's metadata node is first created.
- **Read Path**: To avoid performance overhead on the stats-gathering path, the `topicCreationTimeStamp` will be fetched asynchronously when the PersistentTopic object is initialized. The value will be cached in a `long` field for immediate access during stats requests. This is a one-time operation per topic load that leverages the existing MetadataStore interface.

### lastPublishTimeStamp: In-Memory with Durable Recovery
The lastPublishTimeStamp must be updated on every message publish, a high-frequency operation. Persisting this value to the metadata store on every publish would introduce unacceptable latency and contention, creating a severe performance bottleneck. Therefore, a hybrid in-memory and durable-recovery approach is proposed.

- **In-Memory State**: The lastPublishTimeStamp will be maintained as a volatile long field within the PersistentTopic object on the broker that currently owns the topic.
- **Update Path**: On each successful message publish, the broker will update the field with the current timestamp.
- **Recovery on Topic Load**: The long value is lost when a topic is unloaded or a broker restarts. To ensure the lastPublishTimeStamp is accurate and resilient, a recovery mechanism is required and targeted I/O operation that does not block the topic from becoming available for producers and consumers. But adding the recovery to managed ledger initialization will increase the topic load time so here propose to only asynchronously trigger a read of the metadata of the last confirmed entry in its ManagedLedger when getting users triggered getting topic stats. If the topic is empty, the value will correctly remain 0.

This hybrid design ensures that updating the lastPublishTimeStamp is extremely fast (an in-memory atomic operation), while the recovery mechanism provides strong durability and accuracy guarantees across broker failures and topic lifecycle events.

## Impact Analysis
The proposed changes are designed to be minimally invasive while providing significant value.

### Performance
topicCreationTimeStamp: The performance impact is negligible. It involves a single, one-time asynchronous read from the metadata store during topic initialization, an infrequent operation. Reading the value for stats requests is an in-memory operation from a cached field, resulting in zero I/O overhead.
lastPublishTimeStamp: The impact on the message publish path is extremely low. The update consists of a single, non-blocking long value update operation. The recovery process does not block the topic from becoming available to clients.

### Backward and Forward Compatibility
The proposal is fully backward and forward compatible.

- **API Compatibility**: The changes are purely additive to the TopicStats JSON response. Older admin clients or tools that are unaware of the new fields will simply ignore them, continuing to function without error.
- **Broker Compatibility**: A cluster can be upgraded in a rolling fashion.
    - Brokers running the new version will serve the new fields. 
    - Brokers running an older version will not serve the fields. 
    - Clients will receive the new fields only when their request is served by an upgraded broker.

- **Client Compatibility**: The client-broker binary protocol is not being changed. This proposal only affects the administrative API.

## Observation

No required change for now, but we can consider to add metrics for the new fields in topic stats if there is a strong requirement.

## Testing Plan
A thorough testing plan is essential to validate the correctness, performance, and resilience of the implementation by adding unit test with topic unloading for lastPublishTimeStamp recovery.
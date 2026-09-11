# PIP-90: Expose broker entry metadata to the client

* **Status**: 'Under Discussion'
* **Author**: Bowen Li (https://github.com/LeBW)
* **Pull Request**: https://github.com/apache/pulsar/pull/11553
* **Mailing List discussion**: https://lists.apache.org/thread.html/rb83be5600ba552ae965619d692574c8cc816b63ba616f8f228af7f5d%40%3Cdev.pulsar.apache.org%3E
* **Release**: 2.9.0

## Background

In PIP-70, Apache Pulsar has introduced broker-level entry metadata and already supports adding a timestamp for the message.

Besides, in PR-9039, broker entry metadata provides a continuous message index for messages in one topic-partition which is useful for Protocol Handler like KOP.

However, the client can't get the broker-level entry metadata for now, since the broker skips this information when sending messages to the client. This limits some use cases of the broker-level message metadata for the client, so we plan to implement a feature to send messages with the broker-level entry metadata to clients.

## Solution

First, we change the client protocol version from 17 to 18, as the broker may send the broker entry metadata to the client, and the client should parse it correctly.

```proto
enum ProtocolVersion {
  ...
  v18 = 18; // Add client support for broker entry metadata
}
```

Second, we add a configuration item in the broker to enable or disable the feature of exposing broker entry metadata to the client.

```java
@FieldContext(
      category = CATEGORY_SERVER,
      doc = "Enable or disable exposing broker entry metadata to client.")
privateboolean exposingBrokerEntryMetadataToClientEnabled = false;
```

Then, when the broker sends messages to the client, it should decide whether to send broker entry metadata to the client according to the above configuration item and the client protocol version, which looks like:

```java
// skip broker entry metadata if consumer-client doesn't support broker entry metadata or the
// features is not enabled

if (cnx.getRemoteEndpointProtocolVersion() < ProtocolVersion.v18.getValue()
      || !cnx.isEnableExposingBrokerEntryMetadataToClient()) {
  Commands.skipBrokerEntryMetadataIfExist(metadataAndPayload);
}
```

As for the client, it needs to parse the broker entry metadata correctly, and then it is stored in MessageImpl.brokerEntryMetadata.

Besides, we plan to add two API in org.apache.pulsar.client.api.Message

```java
/**
* Get broker publish time from broker entry metadata.
* Note that only if the feature is enabled in the broker then the value is available.
*
* @since 2.9.0
* @return broker publish time from broker entry metadata, or empty if the feature is not enabled in the broker.
*/
Optional<Long> getBrokerPublishTime();

/**
* Get index from broker entry metadata.
* Note that only if the feature is enabled in the broker then the value is available.
*
* @since 2.9.0
* @return index from broker entry metadata, or empty if the feature is not enabled in the broker.
*/
Optional<Long> getIndex();
```

So the client can get and use these fields from broker entry metadata.

## Compatibility

Let’s talk about the compatibility from two aspects: new broker with old client, and old broker with new client.

For new broker with old client, the client protocol version of the client is less than 18, so the broker will not send broker entry metadata to the client, which avoids the compatibility problem.

For old broker with new client, there is no compatibility problem either, as the client will judge whether there is broker entry metadata by the magic number. If there is no broker entry metadata, the client will not parse it. Besides, the two added API `getIndex()` and `getBrokerPublishTime()` return Optional, which considers the situation that the broker doesn’t send broker entry metadata.

## Test Plan

Make sure the client can get the broker entry metadata correctly, and the two API `getBrokerPublishTime()` and `getIndex()` work correctly.

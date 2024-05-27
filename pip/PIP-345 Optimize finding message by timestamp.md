# PIP-345: Optimize finding message by timestamp

# Background knowledge

Finding message by timestamp is widely used in Pulsar:
* It is used by the `pulsar-admin` tool to get the message id by timestamp[[1]](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/PersistentTopics.java#L1979), 
and expire messages by timestamp[[2]](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v1/PersistentTopics.java#L631), 
and reset cursor[[3]](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v1/PersistentTopics.java#L716).
* It is used by the `pulsar-client` to reset the subscription to a specific timestamp[[4]](https://github.com/apache/pulsar/blob/master/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/Consumer.java#L497).
* And also used by the `expiry-monitor` to find the messages that are expired[[5]](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/persistent/PersistentMessageExpiryMonitor.java#L73).

Pulsar uses binary search to find the message by timestamp, it will reduce the number of iterations to find the message,
and make it more efficient and faster.

# Motivation

Even though the current implementation is correct, and using binary search to speed-up, but it's still not efficient *enough*.
The current implementation is to scan all the ledgers to find the message by timestamp.
This is a performance bottleneck, especially for large topics with many messages.
Say, if there is a topic which has 1m entries, through the binary search, it will take 20 iterations to find the message.

In some extreme cases, it may lead to a timeout, and the client will not be able to seeking by timestamp.

The motivation of this PIP is to optimize the finding message by timestamp, to make it more efficient and faster.

# Goals

Add the `properties` to `LedgerInfo` to store `minPublishTimestamp` and `maxPublishTimestamp` of the ledger, to speed up the finding message by timestamp.

Before read entries from the ledger, we can get `minPublishTimestamp` and `maxPublishTimestamp` from the `LedgerInfo#properties` to calculate the 
range of the entries, and just scan the entries in the range, so we don't need to scan all the ledgers to find the message by timestamp.

It will make the finding message by timestamp more efficient and faster, and reduce the number of iterations to find the message.

## In Scope

* Introduce `properties` to `LedgerInfo`, to speed-up the seeking by timestamp.

## Out of Scope

N/A

# High Level Design
1. Add `properties` to `LedgerInfo`.
2. After the broker writes the message to the ledger finished, update the `minPublishTimestamp` and `maxPublishTimestamp` of the ledger and store them as a temporary value to `PersistentTopic`.
3. Before close a `LedgerHandle`, get the `minPublishTimestamp` and `maxPublishTimestamp` from the `PersistentTopic`, and set them to `LedgerInfo#properties`.
4. When the client/admin/expiry-monitor finds message by timestamp, get `minPublishTimestamp` and `maxPublishTimestamp` from `LedgerInfo#properties` to speed up the finding.

# Detailed Design

## Design & Implementation Details

#### 1. Add the `properties` to `LedgerInfo`, persist them to the ledger metadata store.
```protobuf
  message LedgerInfo {
        required int64 ledgerId = 1;
        optional int64 entries = 2;
        optional int64 size = 3;
        optional int64 timestamp = 4;
        optional OffloadContext offloadContext = 5;
        // Add per-ledgerInfo properties
        repeated KeyValue properties = 6;
}
```
#### 2. Add new methods to `ManagedLedger` to operate the `properties` of `LedgerInfo`.

```java
public interface ManagedLedger {
    Map<String, String> getLedgerInfoProperties(long ledgerId);

    void addLedgerInfoProperties(long ledgerId, Map<String, String> properties);

    void removeLedgerInfoProperties(long ledgerId, Collection<String> keys);
}
```

#### 3. Deserialize the MessageMetadata from the message payload once the broker received the message, and pass it to Producer.

Since there are many places can deserialize the `MessageMetadata` from the message payload, 
deserializing the `MessageMetadata` once the broker received the message and pass it to Producer should improve the performance in some cases.

```java
public class ServerCnx {
    // ...
    protected void handleSend(CommandSend send, ByteBuf headersAndPayload) {
        // ...
        // Deserialize the MessageMetadata from the message payload
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        // ...
        // Pass the MessageMetadata to Producer if the message is transactional
        if (send.hasTxnidMostBits() && send.hasTxnidLeastBits()) {
            TxnID txnID = new TxnID(send.getTxnidMostBits(), send.getTxnidLeastBits());
            producer.publishTxnMessage(txnID, producer.getProducerId(), send.getSequenceId(),
                    send.getHighestSequenceId(), headersAndPayload, /* pass the metadata */metadata, send.getNumMessages(), send.isIsChunk(),
                    send.isMarker());
            return;
        }
        // ...
        // Pass the MessageMetadata to Producer if the message is not transactional
        if (send.hasHighestSequenceId() && send.getSequenceId() <= send.getHighestSequenceId()) {
            producer.publishMessage(send.getProducerId(), send.getSequenceId(), send.getHighestSequenceId(), 
                    headersAndPayload, /* pass the metadata*/ metadata, send.getNumMessages(), send.isIsChunk(), send.isMarker(), position);
        } else {
            producer.publishMessage(send.getProducerId(), send.getSequenceId(), headersAndPayload, 
                    /* pass the metadata*/ metadata, send.getNumMessages(), send.isIsChunk(), send.isMarker(), position);
        }
    }
}
```

#### 4. Pass the `MessageMetadata` to `Producer#MessagePublishContext`, add `getMessageMetadata` method to expose the `MessageMetadata`.

```java
public class Producer {
    // ...
    public class MessagePublishContext implements Topic.PublishContext {
        private MessageMetadata messageMetadata;

        public MessageMetadata getMessageMetadata() {
            return messageMetadata;
        }
    }
}
```

#### 5. Change `TransactionBuffer#appendToTxn(TxnId, long, ByteBuf)` to `TransactionBuffer#appendToTxn(TxnId,PublishContext,ByteBuf)`.
```java
public interface TransactionBuffer {
    // ...
    void appendToTxn(TxnID txnID, Producer.MessagePublishContext context, ByteBuf headersAndPayload);
}
```

#### 6. Pass `PublishContext` to `LedgerHandle#addEntry(ByteBuf, AddCallback, Object)` when adding the entry to the ledger in `TopicTransactionBuffer`.

Add normal message to ledger is already passed the `PublishContext` to `LedgerHandle#addEntry(ByteBuf, AddCallback, Object)`, only need to change the `TopicTransactionBuffer`.

```java
public class TopicTransactionBuffer implements TransactionBuffer {
    // ...
    public void appendToTxn(TxnID txnID, Producer.MessagePublishContext context, ByteBuf headersAndPayload) {
        // ...
        ledger.addEntry(headersAndPayload, new AddCallback() {
            // ...
        }, /* pass the context when adding entry*/context);
    }
}
```

#### 7. Handle add entries finished, update the `minPublishTimestamp` and `maxPublishTimestamp` in `PersistentTopic`.

```java
public class PersistentTopic {
    private final NavigableMap<Long, MutablePair<Long, Long>> ledgerMinMaxPublishTimestamp = new ConcurrentSkipListMap<>();
    
    @Override
    public void addComplete(Position pos, ByteBuf entryData, Object ctx) {
        // ...
        // Update the minPublishTimestamp and maxPublishTimestamp of the ledger after add entry finished.
        updatePublishTimestamp(position, context);
        // ...
    }

    @Override
    public void publishTxnMessage(TxnID txnID, ByteBuf headersAndPayload, PublishContext publishContext) {
        switch (status) {
            case NotDup:
                transactionBuffer.appendBufferToTxn(txnID, publishContext, headersAndPayload)
                        .thenAccept(position -> {
                            // ...
                            // Update the minPublishTimestamp and maxPublishTimestamp of the ledger after add entry finished.
                            updatePublishTimestamp(position, context);
                            // ...
                        });
        }
    }

    // Record minPublishTimestamp and maxPublishTimestamp of the ledger
    private void updatePublishTimestamp(Position position, PublishContext context) {
        MessageMetadata metadata = context.getMessageMetadata();
        if (null == metadata || !metadata.hasPublishTime()) {
            return;
        }
        long ledgerId = position.getLedgerId();
        long publishTimestamp = metadata.getPublishTime();
        MutablePair<Long, Long> pair =
                ledgerMinMaxPublishTimestamp.computeIfAbsent(ledgerId, k -> MutablePair.of(Long.MAX_VALUE, Long.MIN_VALUE));
        long start = pair.getLeft();
        long end = pair.getRight();
        if (publishTime < start) {
            pair.setLeft(publishTime);
        }
        if (publishTime > end) {
            pair.setRight(publishTime);
        }
    }
}

```

### 8. Before close a `LedgerHandle`, set properties to `LedgerInfo#properties`.

```java
import java.util.function.Consumer;

public class ManagedLedgerImpl implements ManagedLedger {
    private volatile Consumer<Long> ledgerClosedListener = null;

    synchronized void ledgerClosed(final LedgerHandle lh) {
        //...
        if (entriesInLedger > 0) {
            // Trigger the listener before close the ledger handle, set the properties to LedgerInfo#properties via the listener.
            if (ledgerClosedListener != null) {
                ledgerClosedListener.accept(lh.getId());
            }
            LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(lh.getId()).setEntries(entriesInLedger)
                    .setSize(lh.getLength()).setTimestamp(clock.millis()).addAllProperties(properties).build();
            ledgers.put(lh.getId(), info);
        }
        // ...
    }
}
```


#### 9. Add a new method to `ManageCursor` to find the newest matching entry.
```java

public class ManagedCursor {
    // ...
    // Add a new method to find the newest matching entry
    // Start and end are defined the binary search range
    void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                 Position start, Position end, FindEntryCallback callback, Object ctx, boolean isFindFromLedger);
}

// Make `ManagedCursorImpl` to implement the new method
public class ManagedCursorImpl implements ManagedCursor {
    // ...
    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        Position start, Position end, FindEntryCallback callback, Object ctx, boolean isFindFromLedger) {
        Position startPosition;
        long max = 0;
        // Calculate the startPosition and max based on the start and end
        startPosition = ...;
        max = ...;
        // ...
        // Find the newest matching entry
        if (isFindFromLedger) {
            ledger.asyncFindNewestMatching(ledger, condition, startPosition, max, callback, ctx);
        } else {
            cache.asyncFindNewestMatching(cursor, condition, startPosition, max, callback, ctx);
        }
    }
}
```

#### 10. Calculate the `start`/`end` position based on the `LedgerInfo#Properties`, and pass them to `ManagedCursor#asyncFindNewestMatching`.

```java
public class PersistentMessageFinder {
    public void findMessages(final long timestamp, AsyncCallbacks.FindEntryCallback callback) {
        this.timestamp = timestamp;
        // ...
        // Find the start/end position based on the begin/end publish timestamp of the ledger
        Position startPosition = null;
        Position endPosition = null;
        for (LedgerInfo info : ledger.getLedgersInfo.values()) {
            List<KeyValue> properties = info.getPropertiesList();
            long minPublishTimestamp = getMinPublishTimestamp(properties);
            long maxPublishTimestamp = getMaxPublishTimestamp(properties);
            // ... 
            startPosition = ...;
            endPosition = ...;
        }
        // ... pass the start/end position to ManagedCursor#asyncFindNewestMatching
        cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, entry -> {
            try {
                long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());
                return MessageImpl.isEntryPublishedEarlierThan(entryTimestamp, timestamp);
            } catch (Exception e) {
                log.error("[{}][{}] Error deserializing message for message position find", topicName, subName, e);
            } finally {
                entry.release();
            }
            return false;
        }, start, end, this, callback, true);
        // ...
    }
}
```

# Backward & Forward Compatibility

This change is *fully* backward compatible.

# Alternatives

N/A


# General Notes

N/A

# Links

* Mailing List discussion thread: 
* Mailing List voting thread:

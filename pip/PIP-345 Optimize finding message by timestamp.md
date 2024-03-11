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

Add the `beginPublishTimestamp` and `endPublishTimestamp` to `LedgerInfo`, to speed up the finding message by timestamp.

Before read entries from the ledger, we can use the `beginPublishTimestamp` and `endPublishTimestamp` to calculate the 
range of the entries, and just scan the entries in the range, so we don't need to scan all the ledgers to find the message by timestamp.

It will make the finding message by timestamp more efficient and faster, and reduce the number of iterations to find the message.

## In Scope

* Record the begin/end publish timestamp to `LedgerInfo`, to speed-up the seeking by timestamp.

## Out of Scope

N/A

# High Level Design
1. Add the `beginPublishTimestamp` and `endPublishTimestamp` to `LedgerInfo`, persist them to the ledger metadata store.
2. When the broker writes the message to the ledger, it will record the `publishTimestamp` to the `LedgerInfo`.
3. When the client/admin/expiry-monitor finds message by timestamp, it will use the `beginPublishTimestamp` and `endPublishTimestamp` to speed up the finding.

# Detailed Design

## Design & Implementation Details

#### 1. Add the `beginPublishTimestamp` and `endPublishTimestamp` to `LedgerInfo`, persist them to the ledger metadata store.
```protobuf
  message LedgerInfo {
        required int64 ledgerId = 1;
        optional int64 entries = 2;
        optional int64 size = 3;
        optional int64 timestamp = 4;
        optional OffloadContext offloadContext = 5;
        // Add the begin/end publish timestamp
        optional int64 beginPublishTimestamp = 6;
        optional int64 endPublishTimestamp = 7;
}
```

#### 2. Deserialize the MessageMetadata from the message payload once the broker received the message, and pass it to Producer.
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

#### 3. Add a new interface `PublishTimestampProvider` to `Entry` to get the `publishTimestamp` of the message.
```java
public interface Entry {

    interface PublishTimestampProvider {
        default Long getPublishTimestamp() {
            return null;
        }
    }
}
```

#### 4. Make `MessagePublishContext` to extend `Entry.PublishTimestampProvider` to get the `publishTimestamp` of the message.
```java
public class Topic {
    // ...
    public interface PublishContext extends Entry.PublishTimestampProvider {
        // ...
    }
}
```

#### 5. Pass the `MessageMetadata` to `Producer#MessagePublishContext`, implement the `getPublishTimestamp` method to get the `publishTimestamp` of the message.
```java
public class Producer {
    // ...
    public class MessagePublishContext implements Topic.PublishContext {
        private MessageMetadata messageMetadata;
        
        public Long getPublishTimestamp() {
            if (messageMetadata == null) {
                return null;
            }
            return messageMetadata.getPublishTime();
        }
    }
}
```

#### 6. Change `TransactionBuffer#appendToTxn(TxnId, long, ByteBuf)` to `TransactionBuffer#appendToTxn(TxnId,PublishContext,ByteBuf)`.
```java
public interface TransactionBuffer {
    // ...
    void appendToTxn(TxnID txnID, Producer.MessagePublishContext context, ByteBuf headersAndPayload);
}
```

#### 7. Pass `PublishContext` to `LedgerHandle#addEntry(ByteBuf, AddCallback, Object)` when adding the entry to the ledger in `TopicTransactionBuffer`.
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

#### 8. Add normal message to ledger is already passed the `PublishContext` to `LedgerHandle#addEntry(ByteBuf, AddCallback, Object)`, so we don't need to change it.
```java
public class PersistentTopic {
    // ...
    // Add normal message to ledger, already passed the PublishContext to LedgerHandle#addEntry(ByteBuf, AddCallback, Object), dont need to change it.
    private void addEntry(ByteBuf headersAndPayload, Producer.MessagePublishContext context) {
        if (brokerService.isBrokerEntryMetadataEnabled()) {
            ledger.asyncAddEntry(headersAndPayload,
                    (int) publishContext.getNumberOfMessages(), this, publishContext);
        } else {
            ledger.asyncAddEntry(headersAndPayload, this, publishContext);
        }
    }
}
```

#### 9. Add a new method `updatePublishTimestamp` to `ManagedLedgerImpl` to update the `beginPublishTimestamp` and `endPublishTimestamp` of the ledger, after the ledger closed, set them to `LedgerInfo`.
```java
public class ManagedLedgerImpl {
    // New field
    // Add a map to record the begin/end publish timestamp of the ledger
    private final NavigableMap<Long, MutablePair</* begin publish timestamp*/Long, /* end publish timestamp*/Long>> publishTimestamps 
            = new ConcurrentSkipListMap<>();
    
    // Update the begin/end publish timestamp of the ledger after the entry is added to the ledger.
    // New method
    protected void updatePublishTimestamp(long ledgerId, long publishTimestamp) {
        MutablePair<Long, Long> pair = publishTimestamps.computeIfAbsent(ledgerId, k -> new MutablePair<>(Long.MAX_VALUE, Long.MIN_VALUE));
        pair.setLeft(Math.min(pair.getLeft(), publishTimestamp));
        pair.setRight(Math.max(pair.getRight(), publishTimestamp));
    }

    // Existed method
    // Set the begin/end publish timestamp to LedgerInfo after the ledger closed.
    synchronized void ledgerClosed(final LedgerHandle lh) {
        // ...
        if (entriesInLedger > 0) {
            // Set the begin/end publish timestamp to LedgerInfo
            MutablePair<Long, Long> pair = publishTimestamps.remove(lh.getId());
            LedgerInfo ledgerInfo = LedgerInfo.newBuilder().setLedgerId(lh.getId())
                    .setEntries(lh.getLastAddConfirmed() + 1).setSize(lh.getLength())
                    .setTimestamp(clock.millis()).setBeginPublishTimestamp(pair.getLeft())
                    .setEndPublishTimestamp(pair.getRight()).build();
            ledgers.put(lh.getId(), ledgerInfo);
        }
        // ...
    }
}
```

#### 10. Once add entry to the ledger, update the `beginPublishTimestamp` and `endPublishTimestamp` of the ledger.
```java
public class OpAddEntry implements AddCallback, CloseCallback, Runnable {
    // ...
    private Object ctx;
    
    @Override
    public void run() {
        // ...
        // Update the begin/end publish timestamp of the ledger after the entry is added to the ledger.
        if (ctx instanceof Entry.PublishTimestampProvider) {
            Entry.PublishTimestampProvider provider = (Entry.PublishTimestampProvider) ctx;
            Long publishTimestamp = provider.getPublishTimestamp();
            if (publishTimestamp != null) {
                ledger.updatePublishTimestamp(ledgerId, publishTimestamp);
            }
        }
    }
}
```

#### 11. Add a new method to `ManageCursor` to find the newest matching entry.
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

#### 12. Calculate the `start`/`end` position based on the `beginPublishTimestamp`/`endPublishTimestamp` of the ledger, and pass them to `ManagedCursor#asyncFindNewestMatching`.
```java
public class PersistentMessageFinder {
    public void findMessages(final long timestamp, AsyncCallbacks.FindEntryCallback callback) {
        this.timestamp = timestamp;
        // ...
        // Find the start/end position based on the begin/end publish timestamp of the ledger
        Position start = null;
        Position end = null;
        for (LedgerInfo info : ledger.getLedgersInfo.values()) {
            if (!info.hasBeginPublishTimestamp() || !info.hasEndPublishTimestamp()) {
                start = end = null;
                break;
            }
            // ... 
            start = ...;
            end = ...;
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

#### 13. For the `expiry-monitor`, we can also use the `beginPublishTimestamp` and `endPublishTimestamp` to speed-up.
```java
public class PersistentMessageExpiryMonitor implements FindEntryCallback, MessageExpirer {
    @Override
    public boolean expireMessages(int messageTTLInSeconds) {
        // ...
        // Calculate the expiredTime
        long expiredTime = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(messageTTLInSeconds);
        // Use the begin/end publish timestamp to speed-up
        PersistentMessageFinder finder = new PersistentMessageFinder(topic, cursor);
        finder.findMessages(expiredTime, this);
    }
}
```

# Backward & Forward Compatibility

This change is backward compatible, the old clients can still work with the new broker.

# Alternatives

An alternative is introducing a new configuration `enableSpeedUpFindingMessageByTimestamp` to broker.conf,
if it set to `true`, the broker will calculate the `start`/`end` position based on the `LedgerInfo#timestamp` of the ledger, 
and pass them to `ManagedCursor#asyncFindNewestMatching` to speed-up the finding message by timestamp,
so we don't need to change the `LedgerInfo`, deserialize `MessageMetadata` once broker received the message,
and add additional logic to manage the `beginPublishTimestamp` and `endPublishTimestamp` of the ledger.

But the `LedgerInfo#timestamp` is the broker's timestamp, it's not accurate, 
and it's not suitable for finding message by timestamp if clients' clocks are not synchronized with the broker's clock.


# General Notes

N/A

# Links

* Mailing List discussion thread: https://lists.apache.org/thread/5owc9os6wmy52zxbv07qo2jrfjm17hd2
* Mailing List voting thread:

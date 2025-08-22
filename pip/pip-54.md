# PIP-54: Support acknowledgement at batch index level

* **Status**: Proposal
* **Author**: Penghui Li
* **Pull Request**: https://github.com/apache/pulsar/pull/6052
* **Mailing List discussion**: https://lists.apache.org/thread.html/rdcc306653be6ac6d910e7302096ddae8e1a9e563df20cf6ca73085de%40%3Cdev.pulsar.apache.org%3E
* **Release**:2.6.0


## Motivation

Currently, the managed cursor maintains the acked messages by mark delete position and individual delete messages. All of the mark delete position and individual delete messages are faced to batch messages(ledgerId,entryId). For users, the single message is the publishing and consuming unit. Since the managed cursor can’t maintain the local index of a batch message, broker will dispatch acked messages(not batch) to consumers such as https://github.com/apache/pulsar/issues/5969.

So this PIP is to support the ability to track the ack status of each batch index to avoid dispatch acked messages to users.

## Approach

This approach requires the cooperation of the client and server. When the broker dispatch messages, it will carry the batch index that has been acked. The client will filter out the batch index that has been acked.

The client needs to send the batch index ack information to the broker so that the broker can maintain the batch index ack status. 

The managed cursor maintains the batch index ack status in memory by using a BitSet and the BitSet can be persisted to a ledger and the metastore, so that can avoid broker crash. When the broker receives the batch index ack request, the acked batch index will be adding to the BitSet. When broker dispatch messages to the client will get the batch message index ack status from the managed cursor and send it to the client. When all indexes of the batch message are acked, the cursor will delete the batch message.

Be careful when calculating consumer permits because the client filter out the acked batch index, so the broker need to increase the available permits equals to acked batch indexes. Otherwise, the broker will stop dispatch messages to that consumer because it does not have enough available permits.

## Changes

### Wire protocol

```proto

message CommandMessage {
    repeated int64 ack_set = 4;
}
```

```proto
message MessageIdData {
    repeated int64 ack_set = 5;
}
```

### Managed ledger data formats

```proto
message BatchedEntryDeletionIndexInfo {
    required NestedPositionInfo position = 1;
    repeated int64 deleteSet = 2;
}
```

```proto
message ManagedCursorInfo {
    // Store which index in the batch message has been deleted
    repeated BatchedEntryDeletionIndexInfo batchedEntryDeletionIndexInfo = 7;
}
```

```proto

message PositionInfo {
    // Store which index in the batch message has been deleted
    repeated BatchedEntryDeletionIndexInfo batchedEntryDeletionIndexInfo = 5;
}
```

## Configuration
Added a flag named `acknowledgmentAtBatchIndexLevelEnabled` in broker.conf to enable or disable the batch index acknowledgement

# PIP-84: Pulsar client: Redeliver command add epoch

* **Status**: Proposal
* **Authors**: Bo Cong, Penghui Li
* **Pull Request**:
* **Mailing List discussion**:
* **Release**:

## Motivation

Pulsar client consumer cumulative ack will ack all the messages before this messageId. When consume fail can't cumulative ack this messageId and then need to redeliver this message. Need to make sure that the next message received is a redeliverd message.

Pulsar pull message and redeliver message is not a synchronous operation. When consumer send redeliver command to server, the pulled message arrive client but the messages is not old messages after server curosr rewind read. In this race condition, client consumer will receive the new message and then cumulative ack this new messageID may lose consume the old messages.

Pulsar client make redeliver messages operation and pull messages operation synchronized so add an epoch into server and client consumer. After client consumer invoke `redeliverUnacknowledgedMessages` , the epoch increase 1 and send `redeliverUnacknowledgedMessages` command carry the epoch and then server cosumer will update the epoch. When dispatcher messages to client will carry the epoch which the cursor read at the time. Client consumer will filter the send messages command which is smaller than current epoch. In this way, after the client consumer send `redeliverUnacknowledgedMessages` command successfully, because it has passed the epoch filtering, the consumer will not receive a message with a messageID greater than the user previously received.

## Protocal change

### CommandRedeliverUnacknowledgedMessages

```
message CommandRedeliverUnacknowledgedMessages {
    required uint64 consumer_id = 1;
    repeated MessageIdData message_ids = 2;
    optional uint64 epoch = 3 [default = 0];
}
```

`CommandRedeliverUnacknowledgedMessages` command add `epoch` field, when client send comamnd to server successfully the server will change the server consumer epoch to the command epoch. The epoch only can bigger than the old epoch in server. Now redeliver message don't have reponse, client don't know the redeliver command success or fail it will receive the new message, so the redeliver command should add response.

### CommandMessage

```
message CommandMessage {
    required uint64 consumer_id       = 1;
    required MessageIdData message_id = 2;
    optional uint32 redelivery_count  = 3 [default = 0];
    repeated int64 ack_set = 4;
    optional uint64 epoch = 5 [default = 0];
}
```

`CommandMessage` must add epoch, when client receive `CommandMessage` will compare the command epoch and local epoch to handle this command.

```
message CommandSubscribe {
    // The consumer epoch, when exclusive and failover consumer redeliver unack message will increase the epoch
    optional uint64 epoch = 19 [default = 0];
}
```

client consumer connect broker will carry epoch
## Client Consumer API

```
/**
 * Redelivers all the unacknowledged messages. In Failover mode, the request is ignored if the consumer is not
 * active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed across all
 * the connected consumers. This is a non blocking call and doesn't throw an exception. In case the connection
 * breaks, the messages are redelivered after reconnect.
 */
CompletableFuture<Void> redeliverUnacknowledgedMessages();
```

Change `redeliverUnacknowledgedMessages()` method from return `void` to return `CompletableFuture<Void>`.

Only in `Exclusive` or `Failover` client consumer can increase epoch, other subscription types don't need.

## Cursor API

```
/**
 * Asynchronously read entries from the ManagedLedger.
 *
 * <p/>If no entries are available, the callback will not be triggered. Instead it will be registered to wait until
 * a new message will be persisted into the managed ledger
 *
 * @see #readEntriesOrWait(int)
 * @param numberOfEntriesToRead
 *            maximum number of entries to return
 * @param callback
 *            callback object
 * @param ctx
 *            opaque context
 * @param maxPosition
 *            max position can read
 * @param epoch
 *            consumer current epoch at this time
 */
void asyncReadEntriesOrWait(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx, PositionImpl maxPosition, long epoch);
```

Every read op will carry the current consumer epoch, because when read completed the epoch may have changed.

## Note

1. Consumer reconnect need reset epoch.
2. Increase epoch must before clear consumer `incommingQueue` .

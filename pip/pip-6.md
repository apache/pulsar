# PIP-6: Guaranteed Message Deduplication

* **Status**: Implemented
* **Authors**: [Matteo Merli](https://github.com/merlimat), [Sijie Guo](https://github.com/sijie)
* **Pull Request**: [#751](https://github.com/apache/incubator-pulsar/pull/751)
* **Mailing List discussion**: https://lists.apache.org/thread.html/58099b7c6bc10a41e575de68f45134f8668fea4baef3f3df76516aa2@%3Cdev.pulsar.apache.org%3E
* **Tasks break down**: https://github.com/apache/incubator-pulsar/projects/3


## Motivation

### Topic reader

In Pulsar v1.18, we have introduced the concept of *topic reader*
([Javadoc](https://pulsar.incubator.apache.org/api/client/org/apache/pulsar/client/api/Reader.html)).
An application can use the `Reader` interface as an alternative to the higher level `Consumer` API.

Being a low-level API, the `Reader` gives complete control to the application on which
messages to read, while leveraging the existing delivery mechanisms and flow control.

Using the `Reader` interface, the application can store the message id along side
with the received data. If the state change from processing the *data* and the
*message id* associated with the data are updated *atomically*, then the application
can make sure that the state transitions triggered by the messages read from the topic
are only applied once.

Data may be read and processed multiple times, though the effects of its processing
will be only applied once.

### Publish messages without duplicates

Since with topic reader we can cover the the "message consumption" side, to close the
circle we need to ensure that messages are published exactly one time in the topic.

To achieve this goal, the Pulsar brokers needs to be able to recognize and ignore
messages that are already stored in the topic. For this being useful, the mechanism needs to
be reliable in any failure scenario already handled by Pulsar.

## Design

To ensure data is written only once in the topic storage, we need to do preventive
de-deduplication to identify and reject messages that are being resent after failures.

To achieve de-deduplication we can rely on the `(producerName, sequenceId)` to
track the last sequence id that was committed on the log for each individual
producer.

The information needs to be kept in-memory and verified before persisting each
message and also stored as a "meta-entry" alongside with the data.

After a broker crash, the next broker to serve the topic must be able to reconstruct
the exact state of the sequence id map.

Additionally, for an application that is publishing messages, to avoid publishing duplicate
messages after application crashes, it needs to be able to restart publishing from a certain
messages, with a particular sequence id.

## Changes

### Client library

In the `ProducerConfiguration`, the application should be allowed to specify:
 * `setProducerName()`: If the name is not set, a globally unique name will be
    assigned by the Pulsar service. Application will then be able to use
    `Producer.getName()` to access the assigned name. If application chooses a
    custom name, it needs to independently ensure that the name is globally unique.
 * `setInitialSequenceId(long sequenceId)`: That's the sequence id for the first
    message that will be published on the producer.
    If not set, the producer will start with `0` and then it will increase the
    sequence id for each message.

Instead of relying on the client library to assign sequence ids, the application will be able to
specify the sequence id on each message:

```java
interface MessageBuilder {
    // ...
    MessageBuilder setSequenceId(long sequenceId);
}
```

This will allow the application to have custom sequence schemes, also with "holes" in the
middle. For example, if the producer is reading data from a file and publishing on a Pulsar
topic, it might want to use the offset in the file for a particular *record* as the sequence
id when publishing. This will simplify managing the sequence id for the application, as no
mapping will be required.

If the application uses the custom sequence id, we will enforce that every message will have
to carry it.

After creating a `Producer` instance, the application should also be able to recover few
informations:
 * `Producer.getProducerName()`: If the producer name was initially assigned from Pulsar (and not
     chosen by the application), it can be discovered after its creation.
 * `Producer.getLastSequenceId()`: Get the sequence id of the last message that was published by
    this producer.

### Broker

Broker needs to keep a per-topic hash-map that keeps track of the highest
`sequenceId` received from each unique `producerName`.

This map is used to reject messages that are duplicates and were already written
in the topic storage.

The map needs also to be snapshotted and stored persistently. After a crash, or a
topic failover, a broker will be able to reconstruct the exact sequence id map,
by loading the snapshot and replaying all the entries written after the snapshot,
to update the `last-sequenceId` for each producer.

#### Storing the sequence ids map

The proposed solution to snapshot the sequence ids map is to use a `ManagedCursor`
for this purpose. The goal is to make sure we can associate a "snapshot" with a
particular managed ledger position `(ledgerId, entryId)`.

The easiest solution is to attach the "snapshot" to the cursor and store it
alongside with the mark-delete position.

There are 2 maps with (`producerName` -> `sequenceId`):
 * `last-sequence-pushed`: This is checked and updated whenever a publish request is received by
   the broker and before pushing the entry to Bookies
 * `last-sequence-persisted`: This is updated when we receive a write acknowledgement for a certain
   entry. This is the map that will be included in the "snapshot".

The steps will look like:
 * Create a cursor dedicated for de-duplication
 * When a new message is being published, verify and update the *Last sequence pushed* map
 * After receiving each acknowledgement from bookies, update the sequence
   id map for the *persisted* entries.
 * Every `N` entries persisted (eg: 1000 entries), perform a "mark-delete" on the dedup cursor,
   attaching the `last-sequence-persisted` map as additional metadata on the cursor position.

On topic recovery, the broker will do:
 * Open the dedup cursor and get the recovered metadata properties
 * Replay all the entries from the mark-delete position to the end
 * For each entry, deserialize the message metadata, extract the `producerName` and `sequenceId`
   and update the sequence id map.

#### Changes to ManagedCursor

We need to allow attaching data to the cursor mark-delete position. This can
be done by extending the
 [`ManagedCursorInfo`](https://github.com/apache/incubator-pulsar/blob/59bb252f1cdc7e087ddae4d1a8451de9124290f2/managed-ledger/src/main/proto/MLDataFormats.proto#L57)
protocol buffer definition to include a new field:

```protobuf
message ManagedCursorInfo {
    // ...
    repeated LongProperty longProperty = 5;
}
```

where a `LongProperty` is defined as:

```protobuf
message LongProperty {
    required string name = 1;
    required long value   = 2;
}
```

The call to `ManagedCursor.markDelete()` should then be extended to also accept,
optionally, a properties map:

```java
interface ManagedCursor {
    // ....
    void markDelete(Position position);
    void markDelete(Position position, Map<String, Long> properties);
}
```

When opening a cursor, we then need a way to expose back the properties associated
with the recovered position.

```java
interface ManagedCursor {
    // ...

    /**
     * Return the properties that that were attached to the current cursor position
     */
    Map<String, Long> getProperties();
}
```

Using a `String` --> `Long` map, provides a good tradeoff between the compactness of storing the
data in protobuf format and the visibility of the internal state to be exposed for debugging/stats
purposes.

#### Enabling the feature

There will be a new namespace level policy that allows to turn on or off the de-deduplication, as
well as a broker wide flag.

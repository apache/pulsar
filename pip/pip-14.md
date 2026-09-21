# PIP-14: Topic compaction

* **Status**: Implemented
* **Authors**: Ivan Kelly
* **Top Level Issue **: https://github.com/apache/incubator-pulsar/issues/933
* **Mailing List discussion**: http://mail-archives.apache.org/mod_mbox/incubator-pulsar-dev/201712.mbox/ajax/%3CCAJdLeK1_Sgoh_LtXwG0kFbw67ScsekziN6cQW25LC%3DzpeSa--Q%40mail.gmail.com%3E
* **Tasks break down**: https://github.com/apache/incubator-pulsar/projects/5
* **Prototype**: https://github.com/ivankelly/incubator-pulsar/tree/compaction-prototype

## Motivation

In some usecases, a pulsar client will want to read the whole history of a log to get an up to date image. If the log is very long this can take a long time. The client will also end up reading a lot of useless data, as some entries will be obscured by later entries.

To avoid this problem we need to do log compaction. Periodically some process will go through the backlog of a topic and remove messages which would be obscured by a later message, i.e. there is a later message in the topic with the same key.

## Design

This document is concerned with the compaction mechanism, which takes a topic, compacts it down, and notifies the broker that compaction has occurred. It is no concerned with the scheduling of compaction. This can be added in a later proposal. Compaction only applies to persistant topics.

To trigger compaction on a topic, the user calls
```
$ bin/pulsar-admin persistent compact <TOPIC_URI>
```
This command will kick off a compaction process which runs on the machine that the command is run from.

The Pulsar compaction mechanism is very similar to that of Kafka. The compactor will first iterate over the message of the topic from the start. For each key it finds it will keep a record of the latest occurance of that key. Once it gets to the end of the topic, it will create a BookKeeper ledger to store the compacted topic. It will iterate over the topic a second time. During the second iteration, it will check the key of each message. If the key matches the latest instance of that key or the message has no key, the message Id, metadata and payload is written to the compacted topic ledger. Once the second run reaches the end point of the first run, compaction terminates. The compacted topic ledger is sealed/closed, and the ID of the ledger is written to the metadata of the topic, along with the message ID of the last message compacted (we'll call this the compaction horizon).

Messages with an empty payload will be considered deleted, so if the last message for a key has an empty payload, that key will be excluded from the compacted ledger.

If compaction is enabled for the topic, the pulsar broker serving the topic will watch the metadata for changes to the compacted topic ledger ID. When a client requests to read a message, and has enabled reads from compacted data, the ID of the message to read is compared to the compaction horizon. If the ID is higher than or equal to the horizon, nothing new happens. If the ID is lower than the compaction horizon, then the read is served from the compaction horizon. As the compacted topic ledger does not contain all messages, the specified ID may not exist. When reading from the compacted topic ledger, we do a binary search to find the first message with a ID greater or equal to the ID specified in the read request. Any messages which do not exist in the compacted topic ledger between the specified ID and the first message found, are messages which have been compacted away - there is another messages further along in the topic with the same key.

If the reading client has not enabled reads from compacted data, it will read from the message backlog, even if a compacted ledger exists.

Both reading and writing will have explicit configuration options for throttling in compaction.

Compaction doesn't directly interact with message expiration. Once a topic is compacted, the backlog still exists. However, subscribers with cursors before the compaction horizon will move quickly through the backlog, so it will eventually get cleaned up as there'll be no subscribers.

Open Question: Should the compactor have a subscription so that it can cause messages to be retained until compaction occurs.

## Changes

### The Compactor

The compactor will be a command line application. The application will take the URI as an argument. It will require a valid client configuration.

The compactor worker will be written in a way that it can later be embedded in a daemon. The algorithm will be as described above.

### Client library

It would be better to avoid completely deserializaing and reserializing each message, so that we can sidestep the need for special handling for compression and encryption. To this end, the client needs to provide a "raw" client, that allows the user to access the metadataAndPayload ByteBuf that is currently sent to the client. This ByteBuf is the data as it stored in the managed ledger, and all deserialation of it usually takes place at the client. The compactor will take this ByteBuf, partially deserialize it to get the key from the metadata, and then if it is a candidate for compaction, push it into the compacted topic ledger, prepended with the serialized message id.

Messages read from a compacted topic by a normal client will be no different to messages read from a non-compacted topic, though there will be gaps in the message IDs.

### Broker

The broker requires changes in the PersistentDispatcher and PersistentTopic, and has a new class CompactedTopic.

#### CompactedTopic

The broker has a class CompactedTopic. There are two important calls on this class, #setCompactionSpec and #asyncReadEntries.

setCompactionSpec is called to update the compaction horizon and compacted topic ledger for the topic.

asyncReadEntries is very similar to ManagedCursor, but it instead of reading from a ManagedLedger it reads from the compacted topic ledger.

#### PersistentDispatcher#readMoreEntries

when readMoreEntries is called on the dispatcher, we will check whether cursor is lower than the compaction horizon. If so it reads from the CompactedTopic. Otherwise it reads from the compactor as it does now.

#### PersistentTopic

PersistentTopic will check if compaction is enabled on the topic, and if so, start watching the metadata for changes to the compaction horizon and compacted topic ledger. When it sees an update, the CompactedTopic object will be updated.

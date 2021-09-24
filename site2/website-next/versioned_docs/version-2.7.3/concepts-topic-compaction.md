---
id: concepts-topic-compaction
title: Topic Compaction
sidebar_label: Topic Compaction
original_id: concepts-topic-compaction
---

Pulsar was built with highly scalable [persistent storage](concepts-architecture-overview.md#persistent-storage) of message data as a primary objective. Pulsar topics enable you to persistently store as many unacknowledged messages as you need while preserving message ordering. By default, Pulsar stores *all* unacknowledged/unprocessed messages produced on a topic. Accumulating many unacknowledged messages on a topic is necessary for many Pulsar use cases but it can also be very time intensive for Pulsar consumers to "rewind" through the entire log of messages.

> For a more practical guide to topic compaction, see the [Topic compaction cookbook](cookbooks-compaction.md).

For some use cases consumers don't need a complete "image" of the topic log. They may only need a few values to construct a more "shallow" image of the log, perhaps even just the most recent value. For these kinds of use cases Pulsar offers **topic compaction**. When you run compaction on a topic, Pulsar goes through a topic's backlog and removes messages that are *obscured* by later messages, i.e. it goes through the topic on a per-key basis and leaves only the most recent message associated with that key.

Pulsar's topic compaction feature:

* Allows for faster "rewind" through topic logs
* Applies only to [persistent topics](concepts-architecture-overview.md#persistent-storage)
* Triggered automatically when the backlog reaches a certain size or can be triggered manually via the command line. See the [Topic compaction cookbook](cookbooks-compaction.md)
* Is conceptually and operationally distinct from [retention and expiry](concepts-messaging.md#message-retention-and-expiry). Topic compaction *does*, however, respect retention. If retention has removed a message from the message backlog of a topic, the message will also not be readable from the compacted topic ledger.

> #### Topic compaction example: the stock ticker
> An example use case for a compacted Pulsar topic would be a stock ticker topic. On a stock ticker topic, each message bears a timestamped dollar value for stocks for purchase (with the message key holding the stock symbol, e.g. `AAPL` or `GOOG`). With a stock ticker you may care only about the most recent value(s) of the stock and have no interest in historical data (i.e. you don't need to construct a complete image of the topic's sequence of messages per key). Compaction would be highly beneficial in this case because it would keep consumers from needing to rewind through obscured messages.


## How topic compaction works

When topic compaction is triggered [via the CLI](cookbooks-compaction.md), Pulsar will iterate over the entire topic from beginning to end. For each key that it encounters the compaction routine will keep a record of the latest occurrence of that key.

After that, the broker will create a new [BookKeeper ledger](concepts-architecture-overview.md#ledgers) and make a second iteration through each message on the topic. For each message, if the key matches the latest occurrence of that key, then the key's data payload, message ID, and metadata will be written to the newly created ledger. If the key doesn't match the latest then the message will be skipped and left alone. If any given message has an empty payload, it will be skipped and considered deleted (akin to the concept of [tombstones](https://en.wikipedia.org/wiki/Tombstone_(data_store)) in key-value databases). At the end of this second iteration through the topic, the newly created BookKeeper ledger is closed and two things are written to the topic's metadata: the ID of the BookKeeper ledger and the message ID of the last compacted message (this is known as the **compaction horizon** of the topic). Once this metadata is written compaction is complete.

After the initial compaction operation, the Pulsar [broker](reference-terminology.md#broker) that owns the topic is notified whenever any future changes are made to the compaction horizon and compacted backlog. When such changes occur:

* Clients (consumers and readers) that have read compacted enabled will attempt to read messages from a topic and either:
  * Read from the topic like normal (if the message ID is greater than or equal to the compaction horizon) or
  * Read beginning at the compaction horizon (if the message ID is lower than the compaction horizon)



---
id: version-2.7.1-transactions
title: Transactions
sidebar_label: Overview
original_id: transactions
---

Transactional semantics enable event streaming applications to consume, process, and produce messages in one atomic operation. In Pulsar, a producer or consumer can work with messages across multiple topics and partitions and ensure those messages are processed as a single unit. 

The following concepts help you understand Pulsar transactions.

## Transaction coordinator and transaction log
The transaction coordinator maintains the topics and subscriptions that interact in a transaction. When a transaction is committed, the transaction coordinator interacts with the topic owner broker to complete the transaction.

The transaction coordinator maintains the entire life cycle of transactions, and prevents a transaction from incorrect status.

The transaction coordinator handles transaction timeout, and ensures that the transaction is aborted after a transaction timeout.

All the transaction metadata is persisted in the transaction log. The transaction log is backed by a Pulsar topic. After the transaction coordinator crashes, it can restore the transaction metadata from the transaction log.

## Transaction ID
The transaction ID (TxnID) identifies a unique transaction in Pulsar. The transaction ID is 128-bit. The highest 16 bits are reserved for the ID of the transaction coordinator, and the remaining bits are used for monotonically increasing numbers in each transaction coordinator. It is easy to locate the transaction crash with the TxnID.

## Transaction buffer
Messages produced within a transaction are stored in the transaction buffer. The messages in transaction buffer are not materialized (visible) to consumers until the transactions are committed. The messages in the transaction buffer are discarded when the transactions are aborted. 

## Pending acknowledge state
Message acknowledges within a transaction are maintained by the pending acknowledge state before the transaction completes. If a message is in the pending acknowledge state, the message cannot be acknowledged by other transactions until the message is removed from the pending acknowledge state.

The pending acknowledge state is persisted to the pending acknowledge log. The pending acknowledge log is backed by a Pulsar topic. A new broker can restore the state from the pending acknowledge log to ensure the acknowledgement is not lost.

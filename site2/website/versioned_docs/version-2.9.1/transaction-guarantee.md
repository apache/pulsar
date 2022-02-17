---
id: transactions-guarantee
title: Transactions Guarantee
sidebar_label: "Transactions Guarantee"
original_id: transactions-guarantee
---

Pulsar transactions support the following guarantee.

## Atomic multi-partition writes and multi-subscription acknowledges
Transactions enable atomic writes to multiple topics and partitions. A batch of messages in a transaction can be received from, produced to, and acknowledged by many partitions. All the operations involved in a transaction succeed or fail as a single unit. 

## Read transactional message
All the messages in a transaction are available only for consumers until the transaction is committed.

## Acknowledge transactional message
A message is acknowledged successfully only once by a consumer under the subscription when acknowledging the message with the transaction ID.
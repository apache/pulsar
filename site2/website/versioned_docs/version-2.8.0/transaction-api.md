---
id: version-2.8.0-transactions-api
title: Transactions API
sidebar_label: Transactions API
original_id: transactions-api
---

All messages in a transaction are available only to consumers after the transaction has been committed. If a transaction has been aborted, all the writes and acknowledgments in this transaction roll back. 

## Prerequisites

1. To enable transactions in Pulsar, you need to configure the parameter in `broker.conf` file or `standalone.conf` file.

```
transactionCoordinatorEnabled=true
```

2. Initialize transaction coordinator metadata, so the transaction coordinators can leverage advantages of the partitioned topic, such as load balance.

```
bin/pulsar initialize-transaction-coordinator-metadata -cs 127.0.0.1:2181 -c standalone
```

After initializing transaction coordinator metadata, you can use the transactions API. The following APIs are available.

## Initialize Pulsar client 

You can enable transaction for transaction client and initialize transaction coordinator client.

```
PulsarClient pulsarClient = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .enableTransaction(true)
        .build();
```

## Start transactions
You can start transaction in the following way.

```
Transaction txn = pulsarClient
        .newTransaction()
        .withTransactionTimeout(5, TimeUnit.MINUTES)
        .build()
        .get();
```

## Produce transaction messages

A transaction parameter is required when producing new transaction messages. The semantic of the transaction messages in Pulsar is `read-committed`, so the consumer cannot receive the ongoing transaction messages before the transaction is committed.

```
producer.newMessage(txn).value("Hello Pulsar Transaction".getBytes()).sendAsync();
```

## Acknowledge the messages with the transaction

The transaction acknowledgement requires a transaction parameter. The transaction acknowledgement marks the messages state to pending-ack state. When the transaction is committed, the pending-ack state becomes ack state. If the transaction is aborted, the pending-ack state becomes unack state.

```
Message<byte[]> message = consumer.receive();
consumer.acknowledgeAsync(message.getMessageId(), txn);
```

## Commit transactions 

When the transaction is committed, consumers receive the transaction messages and the pending-ack state becomes ack state.

```
txn.commit().get();
```

## Abort transaction

When the transaction is aborted, the transaction acknowledgement is canceled and the pending-ack messages are redelivered.

```
txn.abort().get();
```

### Example
The following example shows how messages are processed in transaction.
 
```
PulsarClient pulsarClient = PulsarClient.builder()
        .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
        .statsInterval(0, TimeUnit.SECONDS)
        .enableTransaction(true)
        .build();

String sourceTopic = "public/default/source-topic";
String sinkTopic = "public/default/sink-topic";

Producer<String> sourceProducer = pulsarClient
        .newProducer(Schema.STRING)
        .topic(sourceTopic)
        .create();
sourceProducer.newMessage().value("hello pulsar transaction").sendAsync();

Consumer<String> sourceConsumer = pulsarClient
        .newConsumer(Schema.STRING)
        .topic(sourceTopic)
        .subscriptionName("test")
        .subscriptionType(SubscriptionType.Shared)
        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
        .subscribe();

Producer<String> sinkProducer = pulsarClient
        .newProducer(Schema.STRING)
        .topic(sinkTopic)
        .create();

Transaction txn = pulsarClient
        .newTransaction()
        .withTransactionTimeout(5, TimeUnit.MINUTES)
        .build()
        .get();

// source message acknowledgement and sink message produce belong to one transaction,
// they are combined into an atomic operation.
Message<String> message = sourceConsumer.receive();
sourceConsumer.acknowledgeAsync(message.getMessageId(), txn);
sinkProducer.newMessage(txn).value("sink data").sendAsync();

txn.commit().get();
```

## Enable batch messages in transactions

To enable batch messages in transactions, you need to enable the batch index acknowledgement feature. The transaction acks check whether the batch index acknowledgement conflicts.

To enable batch index acknowledgement, you need to set `acknowledgmentAtBatchIndexLevelEnabled` to `true` in the `broker.conf` or `standalone.conf` file.

```
acknowledgmentAtBatchIndexLevelEnabled=true
```

And then you need to call the `enableBatchIndexAcknowledgment(true)` method in the consumer builder.

```
Consumer<byte[]> sinkConsumer = pulsarClient
        .newConsumer()
        .topic(transferTopic)
        .subscriptionName("sink-topic")
        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
        .subscriptionType(SubscriptionType.Shared)
        .enableBatchIndexAcknowledgment(true) // enable batch index acknowledgement
        .subscribe();
```

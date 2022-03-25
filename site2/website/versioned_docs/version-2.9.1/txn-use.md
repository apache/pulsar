---
id: version-2.9.1-txn-use
title: How to use transactions?
sidebar_label: How to use transactions?
original_id: txn-use
---

## Transaction API

The transaction feature is primarily a server-side and protocol-level feature. You can use the transaction feature via the [transaction API](https://pulsar.apache.org/api/admin/), which is available in **Pulsar 2.8.0 or later**. 

To use the transaction API, you do not need any additional settings in the Pulsar client. **By default**, transactions is **disabled**. 

Currently, transaction API is only available for **Java** clients. Support for other language clients will be added in the future releases.

## Quick start

This section provides an example of how to use the transaction API to send and receive messages in a Java client. 

1. Start Pulsar 2.8.0 or later. 

2. Enable transaction. 

    Change the configuration in the `broker.conf` file.

    ```
    transactionCoordinatorEnabled=true
    ```

    If you want to enable batch messages in transactions, follow the steps below.

    Set `acknowledgmentAtBatchIndexLevelEnabled` to `true` in the `broker.conf` or `standalone.conf` file.

      ```
      acknowledgmentAtBatchIndexLevelEnabled=true
      ```

3. Initialize transaction coordinator metadata.

    The transaction coordinator can leverage the advantages of partitioned topics (such as load balance).

    **Input**

    ```
    bin/pulsar initialize-transaction-coordinator-metadata -cs 127.0.0.1:2181 -c standalone
    ```

    **Output**

    ```
    Transaction coordinator metadata setup success
    ```

4. Initialize a Pulsar client.

    ```
    PulsarClient client = PulsarClient.builder()

    .serviceUrl(“pulsar://localhost:6650”)

    .enableTransaction(true)

    .build();
    ```

Now you can start using the transaction API to send and receive messages. Below is an example of a `consume-process-produce` application written in Java.

![](assets/txn-9.png)

Let’s walk through this example step by step.

<table>
  <tr>
   <td>Step
   </td>
   <td>Description
   </td>
  </tr>
  <tr>
   <td>1. Start a transaction.
   </td>
   <td>The application opens a new transaction by calling PulsarClient.newTransaction. It specifics the transaction timeout as 1 minute. If the transaction is not committed within 1 minute, the transaction is automatically aborted.
   </td>
  </tr>
  <tr>
   <td>2. Receive messages from topics.
   </td>
   <td>The application creates two normal consumers to receive messages from topic input-topic-1 and input-topic-2 respectively.<br><br>If you want to enable batch messages ack in transactions, call the enableBatchIndexAcknowledgment(true) method in the consumer builder. For the example, see [1] below this table.
   </td>
  </tr>
  <tr>
   <td>3. Publish messages to topics with the transaction.
   </td>
   <td>The application creates two producers to produce the resulting messages to the output topic _output-topic-1_ and output-topic-2 respectively. The application applies the processing logic and generates two output messages. The application sends those two output messages as part of the transaction opened in the first step via Producer.newMessage(Transaction).
   </td>
  </tr>
  <tr>
   <td>4. Acknowledge the messages with the transaction.
   </td>
   <td>In the same transaction, the application acknowledges the two input messages.
   </td>
  </tr>
  <tr>
   <td>5. Commit the transaction.
   </td>
   <td>The application commits the transaction by calling Transaction.commit() on the open transaction. The commit operation ensures the two input messages are marked as acknowledged and the two output messages are written successfully to the output topics. 
   <br><br>Tip: You can also call Transaction.abort() to abort the open transaction.
   </td>
  </tr>
</table>

[1] Example of enabling batch messages ack in transactions in the consumer builder.

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


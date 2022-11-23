---
id: txn-use
title: How to use transactions?
sidebar_label: "How to use transactions?"
---

## Transaction API

The transaction feature is primarily a server-side and protocol-level feature. You can use the transaction feature via the [transaction API](/api/admin/), which is available in **Pulsar 2.8.0 or later**. 

To use the transaction API, you do not need any additional settings in the Pulsar client. **By default**, transactions are **disabled**. 

Currently, transaction API is only available for **Java** clients. Support for other language clients will be added in future releases.

## Quick start

This section provides an example of how to use the transaction API to send and receive messages in a Java client. 

1. Start Pulsar 2.8.0 or later. 

2. Enable transaction. 

   Change the configuration in the `broker.conf` or `standalone.conf` file.

   ```conf
   //mandatory configuration, used to enable transaction coordinator
   transactionCoordinatorEnabled=true
   
   //mandtory configuration, used to create systemTopic used for transaction buffer snapshot
   systemTopicEnabled=true
   ```

   * If you want to acknowledge batch messages in transactions, set `acknowledgmentAtBatchIndexLevelEnabled` to `true` in the `broker.conf` or `standalone.conf` file.

     ```conf
     acknowledgmentAtBatchIndexLevelEnabled=true
     ```

   * If you want to guarantee exactly-once semantics, you need to enable [message deduplication](cookbooks-deduplication.md).
   You can enable message deduplication at the broker level, the namespace level, or the topic level according to your needs.
   

3. Initialize transaction coordinator metadata.

   The transaction coordinator can leverage the advantages of partitioned topics (such as load balance).

   **Input**

   ```shell
   bin/pulsar initialize-transaction-coordinator-metadata -cs 127.0.0.1:2181 -c standalone
   ```

   **Output**

   ```shell
   Transaction coordinator metadata setup success
   ```

4. Initialize a Pulsar client.

   ```shell
   PulsarClient client = PulsarClient.builder()
      .serviceUrl("pulsar://localhost:6650")
      .enableTransaction(true)
      .build();
   ```

Now you can start using the transaction API to send and receive messages. Below is an example of a `consume-process-produce` application written in Java.

![](/assets/txn-9.png)

Let’s walk through this example step by step.

| Step  |  Description  | 
| --- | --- |
| 1. Start a transaction.  |  The application opens a new transaction by calling PulsarClient.newTransaction. It specifics the transaction timeout as 1 minute. If the transaction is not committed within 1 minute, the transaction is automatically aborted.  | 
| 2. Receive messages from topics.  |  The application creates two normal consumers to receive messages from topic input-topic-1 and input-topic-2 respectively. | 
| 3. Publish messages to topics with the transaction.  |  The application creates two producers to produce the resulting messages to the output topic _output-topic-1_ and output-topic-2 respectively. The application applies the processing logic and generates two output messages. The application sends those two output messages as part of the transaction opened in the first step via `Producer.newMessage(Transaction)`.  | 
| 4. Acknowledge the messages with the transaction.  |  In the same transaction, the application acknowledges the two input messages.  | 
| 5. Commit the transaction.  |  The application commits the transaction by calling `Transaction.commit()` on the open transaction. The commit operation ensures the two input messages are marked as acknowledged and the two output messages are written successfully to the output topics.  | 

[1] Example of enabling batch messages ack for transactions in the consumer builder.

```java
Consumer<byte[]> consumer = pulsarClient
    .newConsumer()
    .topic(transferTopic)
    .subscriptionName("transaction-sub")
    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
    .subscriptionType(SubscriptionType.Shared)
    .enableBatchIndexAcknowledgment(true) // enable batch index acknowledgment
    .subscribe();
```
[2] Example of using transactions to ack messages individually
```java
// resource prepare
String sourceTopicName = "persistent://" + NAMESPACE1 + "/sourceTopic";
String sinkTopicName = "persistent://" + NAMESPACE1 + "/sinkTopic";
String subName = "shared-subscription";
String producerName = "txn-message-producer";

try {
  @Cleanup
  Producer<String> sinkProducer = pulsarClient.newProducer(Schema.STRING)
          .topic(sinkTopicName)
          .producerName(producerName)
          .create();
  @Cleanup
  Consumer<String> sourceConsumer = pulsarClient.newConsumer(Schema.STRING)
          .topic(sourceTopicName)
          .subscriptionName(subName)
          .subscriptionType(SubscriptionType.Shared)
          .subscribe();

  Message<String> message = null;
  Transaction transaction = null;
  while (true) {
      try {
          message = sourceConsumer.receive();
          //Open a transaction to handle the received message
          transaction = pulsarClient.newTransaction()
                  .withTransactionTimeout(5, TimeUnit.SECONDS)
                  .build()
                  .get();

          //Do some things there

          //Send message to another topic
          sinkProducer.newMessage(transaction)
                  .value("handle message " + message.getValue())
                  .send();
          //Ack the message that has been consumed
          sourceConsumer.acknowledgeAsync(message.getMessageId(), transaction).get();

          //Commit the transaction
          transaction.commit().get();

      } catch (ExecutionException e) {
          Throwable exception = e.getCause();
          if (!(exception instanceof PulsarClientException.TransactionConflictException)) {
              //The message may not be handled, so we need to redeliver it
              sourceConsumer.negativeAcknowledge(message);
          }
          if (!(exception instanceof TransactionCoordinatorClientException.TransactionNotFoundException) && transaction !=null) {
              //Abort the transaction if there is an exception and the transaction is not end.
              transaction.abort().get();
          }
      }
  }
} catch (Exception e) {
  log.error("Catch Exception", e);
}
```

[3] Example of using transactions to ack batch messages cumulatively
```java
// resource prepare
String sourceTopicName = "persistent://" + NAMESPACE1 + "/sourceTopic";
String sinkTopicName = "persistent://" + NAMESPACE1 + "/sinkTopic";
String subName = "failover-subscription";
String producerName = "txn-message-producer";

try {
  @Cleanup
  Producer<String> sinkProducer = pulsarClient.newProducer(Schema.STRING)
          .topic(sinkTopicName)
          .producerName(producerName)
          .create();
  @Cleanup
  Consumer<String> sourceConsumer = pulsarClient.newConsumer(Schema.STRING)
          .topic(sourceTopicName)
          .subscriptionName(subName)
          .subscriptionType(SubscriptionType.Failover)
          .subscribe();

  Messages<String> messages = null;
  Transaction transaction = null;
  while (true) {
      try {
          messages = sourceConsumer.batchReceive();
          if (messages.size() > 0) {
              //Open a transaction to handle the received message
              transaction = pulsarClient.newTransaction()
                      .withTransactionTimeout(5, TimeUnit.SECONDS)
                      .build()
                      .get();
              Message<String> finalMessage = null;
              for (Message<String> message : messages) {
                  //Do some things there

                  //Send message to another topic
                  sinkProducer.newMessage(transaction)
                          .value("handle message " + message.getValue())
                          .send();
                  finalMessage = message;
              }
              //Ack the messages that has been consumed
              sourceConsumer.acknowledgeCumulativeAsync(finalMessage.getMessageId(), transaction).get();

              //Commit the transaction
              transaction.commit().get();
          }
      } catch (ExecutionException e) {
          Throwable exception = e.getCause();
          if (!(exception instanceof TransactionCoordinatorClientException.TransactionNotFoundException) && transaction !=null) {
              //Abort the transaction if there is an exception and the transaction is not end.
              transaction.abort().get();
          }
          //Redeliver messages after the transaction is aborted
          sourceConsumer.redeliverUnacknowledgedMessages();
      }
  }
} catch (Exception e) {
  log.error("Catch Exception", e);
}
```

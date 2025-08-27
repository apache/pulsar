# PIP-93: Transaction performance tools

- Status: Proposal
- Authors: Xiangying Meng、Bo Cong
- Pull Request:
- Mailing List discussion:
- Release:

## Background / Description

Now, transaction basic development has been completed. The user must test the performance of the transaction before using the transaction. Developers also need to test the transaction to ensure that the performance of the transaction meets the needs of the production environment.

## Requirements

What  we need to do is create or update perf test tools to test the performance of the transaction component.

- test performance of `transactionBuffer`: 
   - compare the performance difference between sending messages with transaction and without transaction.
- test performance of `PendingAck`:
    - compare the performance difference with transaction and without transaction ack message
- test performance of `TransactionCoordinator`
    - compare the ack and produce performance difference with transaction and without transaction.
    - test the performance of service handle transaction ops.

## Solution

We implemented the performance tests of the three components of transaction separately.

- update `PerformanceProducer`     ———test `TransactionBuffer` 
    - Add `isTransactionEnable` to control whether transactions need to be opened.
    - Add `numMessagesPerTransaction` to control the number messages of a transaction produced
- update `PerformanceConsumer`  ———test `PendingAckHnadler`
    -  Add `isTransactionEnable` to control whether transactions need to be opened.
    - Add `numMessagesPerTransaction` to control the number messages of a transaction ack.
- add `PerformanceTransaction`. ———test `TransactionCoordinator`
    - `numTestThreads` We can create a ThreadPool with a fixed number of threads and use it to run normal transaction flow. When the number of messages produced and consumed in the transaction is small, the transaction will be frequently created and terminated,  which is executed by transactionCoordinator. Every thread we will create a producer and consumer to produce or consume specified topic. Try to reduce the impact of client-side production and consumption on transaction op(`CommandNewTxn`, `CommandAddPartitionToTxn` etc.)
    - Add `isTrasnactionEnable` to control whether transactions need to be opened.
    - `PerformanceTransaction` don't handle complexity logic of produce or consume and focus on the performance test of transaction coordinator. so we don't need to care about message size etc. If you want to test `PendingAck` or `TransactionBuffer`, you can use `PerformanceProducer` or `PerformanceConsumer` and enable transaction.

## Definition of the options

### Option parameters added in all three classes

- `isCommitedTransaction` = true;
- ` isEnableTransaction` = false;
   when this option is true, we will do any op with transaction.
- public int `numMessagesPerTransaction` = 1;
   this num is how many messages to produce or consume per transaction.
- public long `transactionTimeout` = 5;
   transaction timeout time.

### Option parameters added in performanceTransaction

- public long numTransactions = 0;
    - the number of transaction, if 0, it will keep opening
- Basic configuration
    - boolean help;
    - public List&lt;String&gt; consumeTopics = Collections.singletonList(&quot;sub&quot;);
    - public List&lt;String&gt; produceTopics = Collections.singletonList(&quot;test&quot;);
    - private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;
    - public String confFile;
    - public SubscriptionType subscriptionType = SubscriptionType.Exclusive;
    - public int numThreads = 1; 
    - public String adminURL;
    - public String serviceURL;
    - public Integer partitions = null;
    - public int maxConnections = 100;
    - public long testTime = 0;
    - public int ioThreads = 1;
    - public List&lt;String&gt; subscriptions = Collections.singletonList(&quot;sub&quot;);
    - public int numSubscriptions = 1;
    - public int openTxnRate = 0;

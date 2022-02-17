---
author: Penghui Li
authorURL: https://twitter.com/lipenghui6
title: Apache Pulsar 2.7.0
---
We are very glad to see the Apache Pulsar community has successfully released the wonderful 2.7.0 version after accumulated hard work. It is a great milestone for this fast-growing project and the whole Pulsar community. This is the result of a huge effort from the community, with over 450 commits and a long list of new features, improvements, and bug fixes.

Here is a selection of the most interesting and major features added to Pulsar 2.7.0.

<!--truncate-->

## Major features

### Transaction support

Transactional semantics enable event streaming applications to consume, process, and produce messages in one atomic operation. With transactions, Pulsar achieves the exactly-once semantics for a single partition and multiple partitions as well. This enables new use cases with Pulsar where a client (either as a producer or consumer) can work with messages across multiple topics and partitions and ensure those messages will all be processed as a single unit. This will strengthen the message delivery semantics of Apache Pulsar and processing guarantees for Pulsar Functions.

Currently, Pulsar transactions are in developer preview. The community will work further to enhance the feature to be used in the production environment soon.

To enable transactions in Pulsar, you need to configure the parameter in the `broker.conf` file.

```
transactionCoordinatorEnabled=true
```

Initialize transaction coordinator metadata, so the transaction coordinators can leverage advantages of the partitioned topic, such as load balance.

```
bin/pulsar initialize-transaction-coordinator-metadata -cs 127.0.0.1:2181 -c standalone
```

From the client-side, you can also enable the transactions for the Pulsar client.

```java
PulsarClient pulsarClient = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .enableTransaction(true)
        .build();
```

Here is an example to demonstrate the Pulsar transactions.

```java
// Open a transaction
Transaction txn = pulsarClient
        .newTransaction()
        .withTransactionTimeout(5, TimeUnit.MINUTES)
        .build()
        .get();

//  Publish messages with the transaction
producer.newMessage(txn).value("Hello Pulsar Transaction".getBytes()).send();

// Consume and acknowledge messages with the transaction
Message<byte[]> message = consumer.receive();
consumer.acknowledgeAsync(message.getMessageId(), txn);

// Commit the transaction
txn.commit()
```
For more details about the Pulsar transactions, refer to [here](http://pulsar.apache.org/docs/en/transactions/). For more details about the design of Pulsar transactions, refer to [here](https://github.com/apache/pulsar/wiki/PIP-31%3A-Transaction-Support).

### Topic level policy

Pulsar 2.7.0 introduces the system topic which can maintain all policy change events to achieve the topic level policy. All policies at the namespace level are now also available at the topic level, so users can set different policies at the topic level flexibly without using lots of metadata service resources. The topic level policy enables users to manage topics more flexibly and adds no burden to ZooKeeper.

To enable topic level policy in Pulsar, you need to configure the parameter in the `broker.conf` file.

```
systemTopicEnabled=true
topicLevelPoliciesEnabled=true
```

After topic level policy is enabled, you can use Pulsar Admin to update the policy of a topic. Here is an example for setting the data retention for a specific topic.

```
bin/pulsar-admin topics set-retention -s 10G -t 7d persistent://public/default/my-topic
```

For more details about the system topic and topic level policy, refer to [here](https://github.com/apache/pulsar/wiki/PIP-39%3A-Namespace-Change-Events)

### Support Azure BlobStore offloader

In Pulsar 2.7.0, we add support for Azure BlobStore offloader, which allows users to offload topic data into Azure BlobStore. You can configure the Azure BlobStore offloader driver in the configuration `broker.conf` file.

```
managedLedgerOffloadDriver=azureblob
```

For more details, refer to [here](https://github.com/apache/pulsar/pull/8436).

### Native protobuf schema support

Pulsar 2.7.0 introduces a native protobuf schema support, which can provide more ability for protobuf users who want to integrate with Pulsar. Here is an example to show how to use native protobuf schema in Java client:

```java
Consumer<PBMessage> consumer = client.newConsumer(Schema.PROTOBUFNATIVE(PBMessage.class))
.topic(topic)
.subscriptionName("my-subscription-name")
.subscribe();
```

For more details, refer to [here](https://github.com/apache/pulsar/pull/8372).

### Resource limitation

In Pulsar, tenant, namespace, and topic are the core resources of a cluster. Pulsar 2.7.0 enables you to limit the maximum tenants of a cluster, the maximum namespaces per tenant, the maximum topics per namespace, and the maximum subscriptions per topic.

You can configure the resource limitations in the `broker.conf` file.

```
maxTenants=0
maxNamespacesPerTenant=0
maxTopicsPerNamespace=0
maxSubscriptionsPerTopic=0
```

This provides Pulsar administrators with great convenience in resource management.

### Support e2e encryption for Pulsar Functions

Pulsar 2.7.0 enables you to add End-to-End (e2e) encryption for Pulsar Functions. You can use the public and private key pair that the application configured to perform encryption. Only consumers with a valid key can decrypt encrypted messages.

To enable End-to-End encryption on Functions Worker, you can set it by specifying `--producer-config` in the command line terminal. For more information, refer to [Pulsar Encryption](http://pulsar.apache.org/docs/en/security-encryption/).

For more details, you can see [here](https://github.com/apache/pulsar/pull/8432)

### Function rebalance

Before 2.7.0, there was no mechanism for rebalancing functions scheduler on workers. The workload for functions m become skewed. Pulsar 2.7.0 supports manual trigger functions rebalance and automatic periodic functions rebalance.

For more details, refer to https://github.com/apache/pulsar/pull/7388  and https://github.com/apache/pulsar/pull/7449.

## More information

- To download Apache Pulsar 2.7.0, click [here](https://pulsar.apache.org/en/download/).
- For more information about Apache Pulsar 2.7.0, see [2.7.0 release notes](https://pulsar.apache.org/release-notes/#2.7.0) and [2.7.0 PR list](https://github.com/apache/pulsar/pulls?q=milestone%3A2.7.0+-label%3Arelease%2F2.6.2+-label%3Arelease%2F2.6.1+).

If you have any questions or suggestions, contact us with mailing lists or slack.

- [users@pulsar.apache.org](mailto:users@pulsar.apache.org)
- [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org)
- Pulsar slack channel: https://apache-pulsar.slack.com/
- Self-registration at https://apache-pulsar.herokuapp.com/

Looking forward to your contributions to [Apache Pulsar](https://github.com/apache/pulsar).



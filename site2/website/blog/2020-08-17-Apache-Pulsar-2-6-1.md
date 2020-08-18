---
author: XiaoLong Ran
authorURL: https://twitter.com/wolf4j1
title: Apache Pulsar 2.6.1
---
We are very glad to see the Apache Pulsar community has successfully released the wonderful 2.6.1 version after accumulated hard work. It is a great milestone for this fast-growing project and the whole Pulsar community. This is the result of a huge effort from the community, with over 90 commits and a long list of improvements and bug fixes.

Here is a selection of some of the most interesting and major features added to Pulsar 2.6.1.

<!--truncate-->

## Broker

#### Limit the batch size to the minimum of the `maxNumberOfMessages` and `maxSizeOfMessages`

The Batch size is not limited to the minimum of the `maxNumberOfMessages` and `maxSizeOfMessages` from the `BatchReceive` policy.

1. Batch size is not limited to the minimum of the maxNumberOfMessages and maxSizeOfMessages from the BatchReceive policy.
2. When the batch size is greater than the receiveQ of the consumer (I used a batch size of 3000 and a receiveQ of 500), I notice the following issues:
	
	In a multi-topic (pattern) consumer, the client stops receiving any messages. I think it gets paused and never resumed when setting a timeout in the batch policy. Only one batch is fetched and the client is never resumed.

For more information about implementation details, see [PR-6865](https://github.com/apache/pulsar/pull/6865).

#### Fix hash range conflict issue in Key_Shared subscription with sticky hash range
In `Key_Shared` subscription where the`stickyHashRange` is used, consumers are not allowed to use interleaving hashes.

The pull request will fix hash range conflict issue in `Key_Shared` with sticky hash range.

For more information about implementation details, see [PR-7231](https://github.com/apache/pulsar/pull/7231).

#### Fix: get lookup permission error

Currently，when Pulsar AuthorizationService checks lookup permission, if the user has the role canProducer **or** canConsumer, it means that the user can perform canLookup operations, but actually in the code:

```java
try {
    return canLookupAsync(topicName, role, authenticationData)
            .get(conf.getZooKeeperOperationTimeoutSeconds(), SECONDS);
}
```
If the method `canProduce` or `canConsume` throw an exception, the `canLookup` method will just throw the exception and will not check the other permission.

The pull request will invoke `canLookupAsync` instead.

For more information about implementation details, see [PR-7234](https://github.com/apache/pulsar/pull/7234).

#### Avoid introducing null read position for the managed cursor

Avoid introducing null read position for the managed cursor. The most doubtful thing is `getNextValidPosition` method in the `ManagedLedgerImpl`. If a given position is greater than the last add position, it will return a null value. This may cause the read position to become null. But I have not found how this situation appears. So in the PR, I added a log and print the stack trace which can help us to find the root cause and fallback to the next position of the last position if the null next valid position occurs.
                                                           
For more information about implementation details, see [PR-7264](https://github.com/apache/pulsar/pull/7264).

#### Handling error in creation of non-durable cursor

We're getting an NPE when the creation of a non-durable cursor fails. The reason is that we fail the future but we go on in creating the subscription instance:
                                                                      
```java
try {
    cursor = ledger.newNonDurableCursor(startPosition, subscriptionName);
} catch (ManagedLedgerException e) {
    subscriptionFuture.completeExceptionally(e);
}

return new PersistentSubscription(this, subscriptionName, cursor, false);
```

Additionally, the NPE leads to the topic usage count to not be decremented, leaking 1 usage increment. At the time of deletion, this will prevent the topic from being deleted, even when using the force flag.

For more information about implementation details, see [PR-7355](https://github.com/apache/pulsar/pull/7355).

#### Avoid the NPE occurs in method `ManagedLedgerImpl.isOffloadedNeedsDelete`

When the default value of the `offload-deletion-lag` is set to null, it will cause an NPE problem. To fix this bug, we add null check in the method `ManagedLedgerImpl.isOffloadedNeedsDelete`.
                                                                                         
For more information about implementation details, see [PR-7389](https://github.com/apache/pulsar/pull/7389).

#### Fix producer stuck issue due to NPE thrown when creating a new ledger

NPE can be thrown when creating a ledger because the network address is unresolvable. If NPE is thrown before adding the timeout task, the timeout mechanism doesn't work. The unresolvable network address is commonly seen in the Kubernetes environment. It can happen when a bookie pod or a worker node restarts.

This pull request does the followings:

1. Catch the NPE when creating a new ledger.
2. When the timeout task is triggered, always execute the callback. It is totally fine because we already have the logic to ensure the callback is triggered only once.
3. Add a mechanism to detect the `CreatingLedger` state is not moving.

For more information about implementation details, see [PR-7401](https://github.com/apache/pulsar/pull/7401).

#### Avoid NPEs at ledger creation when DNS failures happen

As a followup to the fix in [PR-7401](https://github.com/apache/pulsar/pull/7401), use try/catch in all places where we're creating new ledgers to cover against NPEs triggered by DNS errors.

For more information about implementation details, see [PR-7403](https://github.com/apache/pulsar/pull/7403).

#### Decompress payload if needed in Key_Shared subscription

Decompress payload if needed in `Key_Shared` subscription.

For more information about implementation details, see [PR-7416](https://github.com/apache/pulsar/pull/7416).

#### Fix NPE when using advertisedListeners

The broker failed to acquire ownership for namespace bundle when using `advertisedListeners=internal:pulsar://node1:6650,external:pulsar://node1.external:6650` with external listener name. Correct `BrokerServiceUrlTls` when tls is not enabled.

For more information about implementation details, see [PR-7620](https://github.com/apache/pulsar/pull/7620).

#### Fix the issue that the deduplication cursor cannot be deleted after message deduplication is disabled

The issue occurs when enabling the message deduplication at the broker.conf and then disable it and restart the broker. The deduplication cursor cannot be deleted.

For more information about implementation details, see [PR-7656](https://github.com/apache/pulsar/pull/7656).

#### Get last entry is trying to read entry -1

A return statement is missed in the current code when the entry is set to -1. Therefore, after the code is sent, the response is trying to read the entry and sends a second response:  

```
16:34:25.779 [pulsar-io-54-7:org.apache.bookkeeper.client.LedgerHandle@748] ERROR org.apache.bookkeeper.client.LedgerHandle - IncorrectParameterException on ledgerId:0 firstEntry:-1 lastEntry:-1
16:34:25.779 [pulsar-client-io-82-1:org.apache.pulsar.client.impl.ConsumerImpl@1986] INFO  org.apache.pulsar.client.impl.ConsumerImpl - [persistent://external-repl-prop/pulsar-function-admin/assignment][c-use-fw-localhost-0-function-assignment-initialize-reader-b21f7607c9] Successfully getLastMessageId 0:-1
16:34:25.779 [pulsar-client-io-82-1:org.apache.pulsar.client.impl.ClientCnx@602] WARN  org.apache.pulsar.client.impl.ClientCnx - [id: 0xc78f4a0e, L:/127.0.0.1:55657 - R:localhost/127.0.0.1:55615] Received error from server: Failed to get batch size for entry org.apache.bookkeeper.mledger.ManagedLedgerException: Incorrect parameter input
16:34:25.779 [pulsar-client-io-82-1:org.apache.pulsar.client.impl.ClientCnx@612] WARN  org.apache.pulsar.client.impl.ClientCnx - [id: 0xc78f4a0e, L:/127.0.0.1:55657 - R:localhost/127.0.0.1:55615] Received unknown request id from server: 10
```

For more information about implementation details, see [PR-7495](https://github.com/apache/pulsar/pull/7495).

#### Fix update partitions error for non-persistent topic

When updating partitions on a non-persistent topic, Error 409 is returned. The pull request will fix partitions error for non-persistent topic.

For more information about implementation details, see [PR-7459](https://github.com/apache/pulsar/pull/7459).

## Zookeeper

#### Use hostname for bookie rack awareness mapping

In [PR-5607](https://github.com/apache/pulsar/pull/5607), the `useHostName()` was added with `return false`. That means that the rack-aware policy will try to resolve the Bookie's hostname into an IP address and then use that IP address to figure out to which rack the bookie belongs.

There are 2 problems: 
 1. The IP won't match the hostname which is recorded in the `/bookies` z-node
 2. If there is an error in resolving the bookie hostname (eg: transient DNS error), an NPE exception will be triggered and the BK client will never realize that this bookie was ever seen as available in the cluster.

The exception is thrown at Line 77, since `getAddress()` returns a `null` given that the address is unresolved.  

```java
74        if (dnsResolver.useHostName()) {
75            names.add(addr.getHostName());
76        } else {
77            names.add(addr.getAddress().getHostAddress());
78        }
```

The default implementation for the `DnsResolver.useHostName()` is to return true.

For more information about implementation details, see [PR-7361](https://github.com/apache/pulsar/pull/7361).

## Java Client

#### Fix the issue that the HTTP header used in Athenz authentication can not be renamed

The authentication plugin for Athenz allows users to change the name of the HTTP header for sending an authentication token to a broker server with a parameter named `roleHeader`. The change will hold the value of the `roleHeader` parameter on the `AuthenticationAthenz` side, and use it directly as the header name.
                                                                                                                                                                                                    
For more information about implementation details, see [PR-7311](https://github.com/apache/pulsar/pull/7311).

#### Fix batch ack set recycled multiple times

Fix batch ack set recycled multiple times. The root cause is a race condition in group ack flush and cumulative Ack. So add recycled state check for the ack set.

For more information about implementation details, see [PR-7409](https://github.com/apache/pulsar/pull/7409).

#### Add authentication client with OAuth2 support

Pulsar supports authenticating clients using OAuth 2.0 access tokens. You can use tokens to identify a Pulsar client and associate with some "principal" (or "role") that is permitted to do some actions (eg: publish messages to a topic or consume messages from a topic). 

This module is to support Pulsar Client Authentication Plugin for OAuth 2.0 directly. The client communicates with the Oauth 2.0 server, and then the client gets an `access token` from the Oauth 2.0 server, and passes this `access token` to Pulsar broker to do the authentication.

So, the broker could still use `org.apache.pulsar.broker.authentication.AuthenticationProviderToken`,
and the user can add their own `AuthenticationProvider` to work with this module.

For more information about implementation details, see [PR-7420](https://github.com/apache/pulsar/pull/7420).

#### Ensure the create subscription can be completed when the operation timeout happens

Ensure the create subscription can be completed when the operation timeout happens.

For more information about implementation details, see [PR-7522](https://github.com/apache/pulsar/pull/7522).

#### Do not try to subscribe to the topic if the consumer is closed

Fix race condition on the closed consumer while reconnecting to the broker.

The race condition happens while the consumer reconnects to the broker. The cnx of the consumer is set to null when the consumer reconnects to the broker. If the consumer is closed at this time, the client will not send close consumer command to the broker. So, when the consumer reconnects to the broker, the consumer will send the subscribe command again. 

This pull request adds state check when connection opened of the consumer is opened. If the consumer is in closing or closed state, we do not need to send the subscribe command.

For more information about implementation details, see [PR-7589](https://github.com/apache/pulsar/pull/7589).

#### Make OAuth2 authentication plugin to use AsyncHttpClient

The OAuth2 client authentication plugin is using Apache HTTP client lib to make request, but it would be better to use AsyncHttpClient as we are using everywhere else in client and broker. Apache HTTP client was only meant to be used for hostname validation and, as explained in [#7612](https://github.com/apache/pulsar/issues/7612) we had better get rid of that dependency.

For more information about implementation details, see [PR-7615](https://github.com/apache/pulsar/pull/7615).

#### Fix batch index filter issue in Consumer

Fix batch index filter issue in Consumer.

For more information about implementation details, see [PR-7654](https://github.com/apache/pulsar/pull/7654).

## CPP Client

#### CPP Oauth2 authentication client

Pulsar supports authenticating clients using OAuth 2.0 access tokens. You can use tokens to identify a Pulsar client and associate with some "principal" (or "role") that is permitted to do some actions (eg: publish messages to a topic or consume messages from a topic). This change tries to support it in cpp client.

For more information about implementation details, see [PR-7467](https://github.com/apache/pulsar/pull/7467).

#### Fix partition index error in close callback

In partitioned producer/consumer's close callback, the partition index is always 0. We need to pass `ProducerImpl/ConsumerImpl`'s internal partition index field to `PartitionedProducerImpl/PartitionedConsumerImpl`'s close callback.

For more information about implementation details, see [PR-7282](https://github.com/apache/pulsar/pull/7282).

#### Fix segment crashes that caused by race condition of timer in CPP client

Segment crashes happens in a race condition:
    - The close operation calls the `keepAliveTimer_.reset()`.
    - At the same time, the timer is accessed in methods `startConsumerStatsTimer` and `handleKeepAliveTimeout`.
This pull request is trying to fix this issue.

For more information about implementation details, see [PR-7572](https://github.com/apache/pulsar/pull/7572).

#### Add support to read credentials from file

Support reading credentials from a file to make it align with the Java client.

For more information about implementation details, see [PR-7606](https://github.com/apache/pulsar/pull/7606).

#### Fix multi-topic consumer segfault on connection error

The multi-topic consumer triggers a segfault when there's an error in creating the consumer. This is due to the calls, which close the partial consumers with a null callback.

For more information about implementation details, see [PR-7588](https://github.com/apache/pulsar/pull/7588).

## Functions

#### Use fully qualified hostname as default to advertise worker

There is a difference in getting hostnames between `Java 8` and `Java 11`. In Java 8, `InetAddress.getLocalHost().getHostName()` can return the fully qualified hostname while in Java 11, it returns a simple hostname. In this case, we should rather use the `getCanonicalHostName()`, which returns the fully qualified hostname. This is the same method to get the advertised address for workers as well.

For more information about implementation details, see [PR-7360](https://github.com/apache/pulsar/pull/7360).

#### Fix the function BC issue introduced in release 2.6.0

There was a backwards compatibility breakage introduced in [PR-5985](https://github.com/apache/pulsar/pull/5985). When the running function workers are separated from brokers, updating workers and brokers independently from release 2.5.0 to 2.6.0 will cause the following error:
                                                                                                                  
```text
java.lang.NullPointerException: null\n\tat java.net.URI$Parser.parse(URI.java:3104) ~[?:?]
java.net.URI.<init>(URI.java:600) ~[?:?]\n\tat java.net.URI.create(URI.java:881) ~[?:?]
org.apache.pulsar.functions.worker.WorkerUtils.initializeDlogNamespace(WorkerUtils.java:160) ~[org.apache.pulsar-pulsar-functions-worker-2.7.0-SNAPSHOT.jar:2.7.0-SNAPSHOT]
org.apache.pulsar.functions.worker.Worker.initialize(Worker.java:155) ~[org.apache.pulsar-pulsar-functions-worker-2.7.0-SNAPSHOT.jar:2.7.0-SNAPSHOT] 
org.apache.pulsar.functions.worker.Worker.start(Worker.java:69) ~[org.apache.pulsar-pulsar-functions-worker-2.7.0-SNAPSHOT.jar:2.7.0-SNAPSHOT] 
org.apache.pulsar.functions.worker.FunctionWorkerStarter.main(FunctionWorkerStarter.java:67) [org.apache.pulsar-pulsar-functions-worker-2.7.0-SNAPSHOT.jar:2.7.0-SNAPSHOT]
```

This is because the broker 2.5.0 supports "bookkeeperMetadataServiceUri" and the admin client returns a `null` field, thus causing the NPE.

For more information about implementation details, see [PR-7528](https://github.com/apache/pulsar/pull/7528).

#### Improve security setting of Pulsar Functions

For more information about implementation details, see [PR-7578](https://github.com/apache/pulsar/pull/7578).

## pulsar-perf

#### Support `tlsAllowInsecureConnection` in pulsar-perf produce/consume/read performance tests

Add `tlsAllowInsecureConnection` config to the CLI tool **pulsar-perf**, to support produce/consume/read performance tests to clusters with insecure TLS connections.

For more information about implementation details, see [PR-7300](https://github.com/apache/pulsar/pull/7300).

## More information

- To download Apache Pulsar 2.6.1, click [here](https://pulsar.apache.org/en/download/).
- For more information about Apache Pulsar 2.6.1, see [2.6.1 release notes](https://pulsar.apache.org/release-notes/#2.6.1 and [2.6.1 PR list](https://github.com/apache/pulsar/pulls?q=is%3Apr+label%3Arelease%2F2.6.1+is%3Aclosed).

If you have any questions or suggestions, contact us with mailing lists or slack.

- [users@pulsar.apache.org](mailto:users@pulsar.apache.org)
- [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org)
- Pulsar slack channel: https://apache-pulsar.slack.com/
- Self-registration at https://apache-pulsar.herokuapp.com/

Looking forward to your contributions to [Pulsar](https://github.com/apache/pulsar).


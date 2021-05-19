---
author: XiaoLong Ran
authorURL: https://twitter.com/wolf4j1
title: Apache Pulsar 2.6.1
---
We are excited to see that the Apache Pulsar community has successfully released 2.6.1 version after a lot of hard work. It is a great milestone for this fast-growing project and the Pulsar community. 2.6.1 is the result of a big effort from the community, with over 100 commits and a long list of improvements and bug fixes.

Here are some highlights and major features added in Pulsar 2.6.1.

<!--truncate-->

## Broker

### Limit the batch size to the minimum of the `maxNumberOfMessages` and `maxSizeOfMessages`

1. Batch size is not limited to the minimum of the `maxNumberOfMessages` and `maxSizeOfMessages` from the BatchReceive policy.
2. When the batch size is greater than the `receiveQ` of the consumer (for example, the batch size is 3000 and a receiveQ is 500), the following issue occurs:
	
	In a multi-topic (pattern) consumer, the client stops receiving any messages. The client gets paused and never resumed when setting a timeout in the batch policy. Only one batch is fetched and the client is never resumed.

For more information about implementation, see [PR-6865](https://github.com/apache/pulsar/pull/6865).

### Fix hash range conflict issue in Key_Shared subscription with sticky hash range
In `Key_Shared` subscription where the `stickyHashRange` is used, consumers are not allowed to use interleaving hashes.

The pull request fixes the hash range conflict issue in `Key_Shared` with sticky hash range.

For more information about implementation, see [PR-7231](https://github.com/apache/pulsar/pull/7231).

### Fix get lookup permission error

If the `canProduce` or `canConsume` method throws an exception, the `canLookup` method just throws the exception and does not check other permissions. The code snippet is as follows: 

```java
try {
    return canLookupAsync(topicName, role, authenticationData)
            .get(conf.getZooKeeperOperationTimeoutSeconds(), SECONDS);
}
```

PR-7234 invokes `canLookupAsync`. When Pulsar AuthorizationService checks lookup permission, if the user has the `canProducer` or `canConsumer` role, the user performs `canLookup` operations.

For more information about implementation, see [PR-7234](https://github.com/apache/pulsar/pull/7234).

### Avoid introducing null read position for the managed cursor

Avoid introducing null read position for the managed cursor. The most doubtful thing is the `getNextValidPosition` method in the `ManagedLedgerImpl`. If a given position is greater than the position added last time, it returns a `null` value, and the read position is also `null`.

In this PR, we add a log and print the stack trace to find the root cause and fallback to the next position if the `null` occurs at the next valid position.
                                                           
For more information about implementation, see [PR-7264](https://github.com/apache/pulsar/pull/7264).

### Fix error in creation of non-durable cursor

An NPE occurs when we fail to create a non-durable cursor and continue to create the subscription instance. 
                                                                      
```java
try {
    cursor = ledger.newNonDurableCursor(startPosition, subscriptionName);
} catch (ManagedLedgerException e) {
    subscriptionFuture.completeExceptionally(e);
}

return new PersistentSubscription(this, subscriptionName, cursor, false);
```

Additionally, the NPE leads to the topic usage count increasing to 1. When deleting a topic, the topic cannot be deleted even if you use the force flag.

For more information about implementation, see [PR-7355](https://github.com/apache/pulsar/pull/7355).

### Avoid an NPE occurs in the `ManagedLedgerImpl.isOffloadedNeedsDelete` method

When the default value of the `offload-deletion-lag` is set to `null`, an NPE occurs. To fix the bug, null check is added in the `ManagedLedgerImpl.isOffloadedNeedsDelete` method.
                                                                                         
For more information about implementation, see [PR-7389](https://github.com/apache/pulsar/pull/7389).

### Fix producer stuck issue due to NPE when creating a new ledger

NPE occurs when creating a ledger if the network address is unresolvable. If NPE occurs before adding the timeout task, the timeout mechanism does not work. The unresolvable network address is common in the Kubernetes environment. It happens when a bookie pod or a worker node restarts.

This pull request fixes from the following perspectives:

1. Catch the NPE when creating a new ledger.
2. When the timeout task is triggered, it always executes the callback. It is totally fine because we already have the logic to ensure the callback is triggered only once.
3. Add a mechanism to detect that the `CreatingLedger` state is not moving.

For more information about implementation, see [PR-7401](https://github.com/apache/pulsar/pull/7401).


### Fix NPE when using advertisedListeners

The broker failed to acquire ownership for the namespace bundle when using `advertisedListeners=internal:pulsar://node1:6650,external:pulsar://node1.external:6650` with external listener name. Correct `BrokerServiceUrlTls` when TLS is not enabled.

For more information about implementation, see [PR-7620](https://github.com/apache/pulsar/pull/7620).

### Fix the issue that the deduplication cursor cannot be deleted after message deduplication is disabled

When enabling the message deduplication in the `broker.conf` file, disabling it and then restarting the broker, the deduplication cursor is not deleted.

This PR fixes the issue, so when you disable message deduplication, you can delete the deduplication cursor.

For more information about implementation, see [PR-7656](https://github.com/apache/pulsar/pull/7656).

### Fix the issue that GetLastEntry() reads entry `-1`

Previously, the code does not include a return statement. If the entry is set to `-1`, after sending code, the response reads the entry and sends a second response, as shown in the following example.
```
16:34:25.779 [pulsar-io-54-7:org.apache.bookkeeper.client.LedgerHandle@748] ERROR org.apache.bookkeeper.client.LedgerHandle - IncorrectParameterException on ledgerId:0 firstEntry:-1 lastEntry:-1
16:34:25.779 [pulsar-client-io-82-1:org.apache.pulsar.client.impl.ConsumerImpl@1986] INFO  org.apache.pulsar.client.impl.ConsumerImpl - [persistent://external-repl-prop/pulsar-function-admin/assignment][c-use-fw-localhost-0-function-assignment-initialize-reader-b21f7607c9] Successfully getLastMessageId 0:-1
16:34:25.779 [pulsar-client-io-82-1:org.apache.pulsar.client.impl.ClientCnx@602] WARN  org.apache.pulsar.client.impl.ClientCnx - [id: 0xc78f4a0e, L:/127.0.0.1:55657 - R:localhost/127.0.0.1:55615] Received error from server: Failed to get batch size for entry org.apache.bookkeeper.mledger.ManagedLedgerException: Incorrect parameter input
16:34:25.779 [pulsar-client-io-82-1:org.apache.pulsar.client.impl.ClientCnx@612] WARN  org.apache.pulsar.client.impl.ClientCnx - [id: 0xc78f4a0e, L:/127.0.0.1:55657 - R:localhost/127.0.0.1:55615] Received unknown request id from server: 10
```

PR-7495 adds a return statement to code, so GetLastEntry() reads the last entry, instead of `-1`.  

For more information about implementation, see [PR-7495](https://github.com/apache/pulsar/pull/7495).

### Fix the error of updating partitions for non-persistent topic

When updating partitions on a non-persistent topic, Error 409 is returned. The pull request fixes partitions errors for non-persistent topics.

For more information about implementation, see [PR-7459](https://github.com/apache/pulsar/pull/7459).

## ZooKeeper

### Use hostname for bookie rack awareness mapping

In [PR-5607](https://github.com/apache/pulsar/pull/5607), the `useHostName()` is added with `return false`. The rack-aware policy passes the Bookie's hostname into an IP address and then uses that IP address to figure out to which rack the bookie belongs.

Then two issues occur: 
 1. The IP does not match the hostname which is recorded in the `/bookies` z-node
 2. If there is an error in parsing the bookie hostname (eg: transient DNS error), an NPE is triggered and the BK client never realizes that this bookie is available in the cluster.

The exception is thrown at Line 77(as shown in the following code snippet), since `getAddress()` returns a `null` given that the address is parsed.  

```java
74        if (dnsResolver.useHostName()) {
75            names.add(addr.getHostName());
76        } else {
77            names.add(addr.getAddress().getHostAddress());
78        }
```

The default implementation for the `DnsResolver.useHostName()` returns `true`.

For more information about implementation, see [PR-7361](https://github.com/apache/pulsar/pull/7361).

## Java Client

### Fix the issue that the HTTP header used in Athenz authentication can not be renamed

The authentication plugin for Athenz allows users to change the name of the HTTP header for sending an authentication token to a broker server with a parameter named `roleHeader`. The change uses the value of the `roleHeader` parameter on the `AuthenticationAthenz` side, and uses it directly as the header name.
                                                                                                                                                                                                    
For more information about implementation, see [PR-7311](https://github.com/apache/pulsar/pull/7311).

### Fix the issue that batch ack set is recycled multiple times

The batch ack sets are recycled multiple times, due to race condition in group ack flush and cumulative Ack. So we add a recycled state check for the ack set in PR-7409, and fix the recycle issue.

For more information about implementation, see [PR-7409](https://github.com/apache/pulsar/pull/7409).

### Add authentication client with OAuth2 support

Pulsar supports authenticating clients using OAuth 2.0 access tokens. You can use tokens to identify a Pulsar client and associate with some "principal" (or "role") that is permitted to do some actions, for example, publish messages to a topic or consume messages from a topic. 

This module is to support Pulsar Client Authentication Plugin for OAuth 2.0 directly. The client communicates with the Oauth 2.0 server, gets an `access token` from the Oauth 2.0 server, and passes the `access token` to Pulsar broker to do the authentication.

So, the broker can use `org.apache.pulsar.broker.authentication.AuthenticationProviderToken`,
and the user can add their own `AuthenticationProvider` to work with this module.

For more information about implementation, see [PR-7420](https://github.com/apache/pulsar/pull/7420).


### Not subscribe to the topic when the consumer is closed

Fix race condition on the closed consumer while reconnecting to the broker.

The race condition happens when the consumer reconnects to the broker. The connection of the consumer is set to `null` when the consumer reconnects to the broker. If the consumer is not connected to broker at this time, the client does not send the consumer command to the broker. So, when the consumer reconnects to the broker, the consumer sends the subscribe command again. 

This pull request adds a state check when the `connectionOpened()` of the consumer opens. If the consumer is in closing or closed state, the consumer does not send the subscribe command.

For more information about implementation, see [PR-7589](https://github.com/apache/pulsar/pull/7589).

### OAuth2 authentication plugin uses AsyncHttpClient

Previously, the OAuth2 client authentication plugin used Apache HTTP client lib to make requests, Apache HTTP client is used to validate hostname. As suggested in [#7612](https://github.com/apache/pulsar/issues/7612), we get rid of the dependency of using Apache HTTP client.

In PR-7615, OAuth2 client authentication plugin uses AsyncHttpClient, which is used in client and broker. For more information about implementation, see [PR-7615](https://github.com/apache/pulsar/pull/7615).


## CPP Client

### CPP Oauth2 authentication client

Pulsar supports authenticating clients using OAuth 2.0 access tokens. You can use tokens to identify a Pulsar client and associate with some "principal" (or "role") that is permitted to do some actions (eg: publish messages to a topic or consume messages from a topic). This change tries to support it in cpp client.

For more information about implementation, see [PR-7467](https://github.com/apache/pulsar/pull/7467).

### Fix partition index error in close callback

In partitioned producer/consumer's close callback, the partition index is always `0`. The `ProducerImpl/ConsumerImpl` internal partition index field should be passed to `PartitionedProducerImpl/PartitionedConsumerImpl` close callback.

For more information about implementation, see [PR-7282](https://github.com/apache/pulsar/pull/7282).

### Fix segment crashes caused by race condition of timer in CPP client

Segment crashes occur in a race condition:
    - The close operation calls the `keepAliveTimer_.reset()`.
    - The `keepAliveTimer` is called by `startConsumerStatsTimer` and `handleKeepAliveTimeout` methods. Actually, the `keepAliveTimer` should not be called by those two methods.

This pull request fixes those issues.

For more information about implementation, see [PR-7572](https://github.com/apache/pulsar/pull/7572).

### Add support to read credentials from file

Support reading credentials from a file to make it align with the Java client.

For more information about implementation, see [PR-7606](https://github.com/apache/pulsar/pull/7606).

### Fix multi-topic consumer segfault on connection error

The multi-topic consumer triggers a segfault when an error occurs in creating a consumer. This is due to the calls to close the partial consumers with a null callback.

For more information about implementation, see [PR-7588](https://github.com/apache/pulsar/pull/7588).

## Functions

### Use fully qualified hostname as default to advertise worker

There is a difference in getting hostnames between `Java 8` and `Java 11`. In Java 8, `InetAddress.getLocalHost().getHostName()` returns the fully qualified hostname; in Java 11, it returns a simple hostname. In this case, we should rather use the `getCanonicalHostName()`, which returns the fully qualified hostname. This is the same method to get the advertised address for workers as well.

For more information about implementation, see [PR-7360](https://github.com/apache/pulsar/pull/7360).

### Fix the function BC issue introduced in release 2.6.0

A backwards compatibility breakage is introduced in [PR-5985](https://github.com/apache/pulsar/pull/5985). When the running function workers are separated from brokers, updating workers and brokers independently from release 2.5.0 to 2.6.0 results in the following error:
                                                                                                                  
```text
java.lang.NullPointerException: null\n\tat java.net.URI$Parser.parse(URI.java:3104) ~[?:?]
java.net.URI.<init>(URI.java:600) ~[?:?]\n\tat java.net.URI.create(URI.java:881) ~[?:?]
org.apache.pulsar.functions.worker.WorkerUtils.initializeDlogNamespace(WorkerUtils.java:160) ~[org.apache.pulsar-pulsar-functions-worker-2.7.0-SNAPSHOT.jar:2.7.0-SNAPSHOT]
org.apache.pulsar.functions.worker.Worker.initialize(Worker.java:155) ~[org.apache.pulsar-pulsar-functions-worker-2.7.0-SNAPSHOT.jar:2.7.0-SNAPSHOT] 
org.apache.pulsar.functions.worker.Worker.start(Worker.java:69) ~[org.apache.pulsar-pulsar-functions-worker-2.7.0-SNAPSHOT.jar:2.7.0-SNAPSHOT] 
org.apache.pulsar.functions.worker.FunctionWorkerStarter.main(FunctionWorkerStarter.java:67) [org.apache.pulsar-pulsar-functions-worker-2.7.0-SNAPSHOT.jar:2.7.0-SNAPSHOT]
```

This is because the broker 2.5.0 supports "bookkeeperMetadataServiceUri" and the admin client returns a `null` field, thus causing the NPE.

For more information about implementation, see [PR-7528](https://github.com/apache/pulsar/pull/7528).

## pulsar-perf

### Support `tlsAllowInsecureConnection` in pulsar-perf produce/consume/read performance tests

Add `tlsAllowInsecureConnection` config to the CLI tool **pulsar-perf**, to support produce/consume/read performance tests to clusters with insecure TLS connections.

For more information about implementation, see [PR-7300](https://github.com/apache/pulsar/pull/7300).

## More information

- To download Apache Pulsar 2.6.1, click [download](https://pulsar.apache.org/en/download/).
- For more information about Apache Pulsar 2.6.1, see [2.6.1 release notes](https://pulsar.apache.org/release-notes/#2.6.1) and [2.6.1 PR list](https://github.com/apache/pulsar/pulls?q=is%3Apr+label%3Arelease%2F2.6.1+is%3Aclosed).

If you have any questions or suggestions, contact us with mailing lists or slack.

- [users@pulsar.apache.org](mailto:users@pulsar.apache.org)
- [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org)
- Pulsar slack channel: https://apache-pulsar.slack.com/
- Self-registration at https://apache-pulsar.herokuapp.com/

Looking forward to your contributions to [Pulsar](https://github.com/apache/pulsar).



---
author: Xiaolong Ran
authorURL: https://twitter.com/wolf4j1
title: Apache Pulsar 2.4.2
---

We are proud to publish Apache Pulsar 2.4.2. Thank the great efforts from Apache Pulsar community with over 110 commits, covering improvements and bug fixes.

For detailed changes related to 2.4.2 release, refer to <b>[release notes](/release-notes/#2.4.2)</b>.

I will highlight some improvements and bug fixes in this blog.

<!--truncate-->

## Use classLoaders to load Java functions
In Pulsar 2.4.2, windowed functions can work well whether Java Functions instances use shaded JAR or classLoaders, and functionClassLoader is set correctly when the `--output-serde-classname` option is enabled.

Before Pulsar 2.4.2, Java Functions instances are started with a shaded JAR, and different classLoaders are used to load the internal Pulsar code, user code, and the interfaces that the two interacts with each other. This change results in two issues:
- The windowed functions do not work well if Java Functions instances use classLoaders. 
- When using the `--output-serde-classname` option, functionClassLoader is not set correctly.  

## Start Broker with Functions worker  
In Pulsar 2.4.2, we can start Broker with Functions worker when broker client is enabled with TLS. Before Pulsar 2.4.2, when we run Functions worker with the broker, it checks whether TLS is enabled in the `function_worker.yml` file. If TLS is enabled, it uses TLS port. However, when TLS is enabled on Functions worker, it checks the `broker.conf`. Since Functions worker runs with the broker, it makes sense to check the `broker.conf` as the single source of truth about whether or not to use TLS. 

## Add error code and error message when a key does not exist
In Pulsar Functions, BookKeeper is supported to store the state of Functions. When users attempt to fetch a key that does not exist from function state, an NPE(NullPointerException) error occurs. In Pulsar 2.4.2, we add error code and error message for the case when a key does not exist.

## Deduplication
Deduplication removes messages based on the the largest sequence ID that pre-persisted. If an error is persisted to BookKeeper, a retry attempt is “deduplicated” with no message ever getting persisted. In version 2.4.2, we fix the issue from the following two aspects:                                                                                              
- Double check the pending messages and return error to the producer when the duplication status is uncertain. For example, when a message is still pending.
- Sync back the lastPushed map with the lastStored map after failures.

## Consume data from the earliest location
In Pulsar 2.4.2, we add `--subs-position` for Pulsar Sinks, so users can consume data from the latest and earliest locations. Before 2.4.2 release, data in topics is consumed from the latest location in Pulsar Sinks by default, and users can not consume the earliest data in sink topic. 

## Close previous dispatcher when the subscription type changes

In Pulsar 2.4.2, when the type of a subscription changes, a new dispatcher is created, and the old dispatcher is closed, thus avoiding memory leaks. Before 2.4.2, when the subscription type of a topic changes, a new dispatcher is created and the old one is discarded, yet not closed, which causes memory leaks. If the cursor is not durable, the subscription is closed and removed from the topic when all consumers are removed. The dispatcher should be closed at this time. Otherwise, RateLimiter instances are not garbage collected, which results in a memory leak. 

## Select an active consumer based on the subscription order
In Pulsar 2.4.2, the active consumer is selected based on the subscription order. The first consumer in the consumer list is selected as an active consumer without sorting. Before 2.4.2, the active consumer is selected based on the priority level and consumer name. In this case, the active consumer joins and leaves, and no consumer is actually elected as "active" or consumes messages. 

## Remove failed stale producer from the connection
In Pulsar 2.4.2, failed producer is removed correctly from the connection. Before Pulsar 2.4.2, broker cannot clean up the old failed producer correctly from the connection. When broker tries to clean up `producer-future` in the failed producer, it removes the newly created `producer-future` rather than the old failed producer, and the following error occurs in broker.

```text
17:22:00.700 [pulsar-io-21-26] WARN  org.apache.pulsar.broker.service.ServerCnx - [/1.1.1.1:1111][453] Producer with id persistent://prop/cluster/ns/topic is already present on the connection  
```  
                        
## Add new APIs for schema
In Pulsar 2.4.2, we add the following APIs for schema:
- `getAllVersions`: return the list of schema versions for a given topic.
- `testCompatibility`: be able to test the compatibility for a schema without registering it.
- `getVersionBySchema`: provide a schema definition and provide the schema version for it.

## Expose `getLastMessageId()` method in consumerImpl
In Pulsar 2.4.2, we expose `getLastMessageId()` method in consumerImpl. It benefits users when they want to know the lag messages, or only consume messages before the current time.                                                     

## Add new `send()` interface in C++/Go
In Pulsar 2.4.2, we add new `send()` interface in C++/Go, so the `MessageID` will be returned to users. The logic is consistent with that in Java. In Java client, the `MessageId send(byte[] message)` returns `MessageId` for users.

## Consumer background tasks are cancelled after subscription failures
In Pulsar 2.4.2, we ensure that consumer background tasks are cancelled after subscription failures. Before 2.4.2, some background consumer tasks are started in the ConsumerImpl constructor though these tasks are not cancelled if the consumer creation fails, leaving active references to these objects. 

## Delete topics attached with regex consumers
In Pulsar 2.4.2, we can delete topics attached with a regex consumer. The followings are detailed methods.
- Add a flag in CommandSubscribe so that a regex consumer will never trigger the creation of a topic.
- Subscribe to a non-existing topic. When a specific error occurs, the consumer is interpreted as a permanent failure and thus stopping retrying.

Before 2.4.2, it's not possible to delete topics when there is a regex consumer attached to them. The reason is that the regex consumer will immediately reconnect and re-create the topic. 

## Reference

Download Pulsar 2.4.2 [here](https://pulsar.apache.org/en/download/). 

If you have any questions or suggestions, contact us with mailing lists or slack. 
- [users@pulsar.apache.org](mailto:users@pulsar.apache.org) 
- [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org) 
- Pulsar slack channel: https://apache-pulsar.slack.com/
- You can self-register at https://apache-pulsar.herokuapp.com/

Looking forward to your contributions to [Pulsar](https://github.com/apache/pulsar).
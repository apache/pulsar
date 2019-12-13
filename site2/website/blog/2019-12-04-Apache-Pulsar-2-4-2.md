---
author: Xiaolong Ran
authorURL: https://twitter.com/wolf4j1
title: Apache Pulsar 2.4.2
---

We are proud to publish Apache Pulsar 2.4.2. Thank the great efforts from Apache Pulsar community with over 110 commits, covering improvements and bug fixes.

For detailed changes related to 2.4.2 release, refer to [release notes](../release-notes.md#2.4.2).

I will highlight some improvements and bug fixes in this blog.

<!--truncate-->
## Use classloaders to load Java functions
- In Pulsar 2.4.2, windowed functions can work well whether Java Functions instances use shaded JAR or classLoaders, and functionClassLoader is set correctly when the `--output-serde-classname` option is enabled.

Before Pulsar 2.4.2, Java Functions instances are started with a shaded JAR, and different classLoaders are used to load the internal Pulsar code, user code, and the interfaces that the two interacts with each other. This change results in two issues:
  - The windowed functions does not work well if Java Functions instances use classLoaders. 
  - When using the `--output-serde-classname` option, functionClassLoader is not set correctly.  

## Start Broker with Functions worker  
- In Pulsar 2.4.2, we can start Broker with Functions worker when broker client is enabled with TLS. 
Before Pulsar 2.4.2, when we run Functions worker with the broker, it checks whether TLS is enabled in the `function_worker.yml` file. If TLS is enabled, we will use TLS port. However, when we enable TLS on Functions worker, it checks the `broker.conf`. Since Functions worker runs with the broker, it makes sense to check the `broker.conf` as the single source of truth about whether or not to use TLS. 

- In Pulsar Functions, BookKeeper is supported to store the state of Functions. When users attempt to fetch a key that does not exist from function state, an NPE(NullPointerException) error occurs. In Pulsar 2.4.2, we add error code and error message for the case when a key does not exist.

## Deduplication
- Since deduplication removes messages based on the the largest sequence id recorded pre-persist, if there’s an error persisting to BK, a retry attempt will just be “deduplicated” with no message ever getting persisted. In version 2.4.2, we fix the issue from the following two aspects:                                                                                                
    - Double check the pending messages and return error to the producer when the duplication status is uncertain. For example, a message is still pending.
    - Sync back the lastPushed map with the lastStored map after failures.

## Consume data from the earliest location
- In Pulsar 2.4.2, we add `--subs-position` for Pulsar Sinks, so users can consume data from the earliest location. Before 2.4.2 release, data in topics is consumed from the latest location in Pulsar Sinks by default, and users can not consume the earliest data in sink topic. 

## Close previous dispatcher when subscription type changes

- If the subscription type of a topic changes, a new dispatcher is created and the old one is discarded. However, the old dispatcher is not closed, which causes memory leaks. If the cursor is not durable, the subscription is closed and removed from the topic when all consumers are removed. The dispatcher should be closed at this time. Otherwise, RateLimiter instances will not be garbage collected, causing a memory leak. 

In Pulsar 2.4.2, when the type of a subscription changes, a new dispatcher is created, and the old dispatcher is closed, thus avoiding memory leaks.

## Select an active consumer based on the subscription order
- Instead of sorting the consumers based on priority level and consumer name, an active consumer is selected, which causes subscription getting into a flaky state, where the "active" consumer joins and leaves, and no consumer is actually elected as "active" and consumes the messages. 

In Pulsar 2.4.2, based on the subscription order, the first consumer in the consumer list is selected as an active consumer. 

## Remove failed stale producer from the connection
- Before Pulsar 2.4.2, broker tries to clean up stale failed-producer from the connection however, while cleaning up producer-future, it tries to remove newly created producer-future rather old-failed producer because of that broker still gives below error:
    ```text
    17:22:00.700 [pulsar-io-21-26] WARN  org.apache.pulsar.broker.service.ServerCnx - [/1.1.1.1:1111][453] Producer with id persistent://prop/cluster/ns/topic is already present on the connection
    ```  

In Pulsar 2.4.2, we remove failed stale producer from the connection.                          
## Add new APIs for schema
- In Pulsar 2.4.2, we add the following APIs for schema:

    - `getAllVersions`: return the list of schema versions for a given topic.
    - `testCompatibility`: be able to test the compatibility for a schema without registering it.
    - `getVersionBySchema`: provide a schema definition and provide the schema version for it.

## Expose `getLastMessageId()` method in consumerImpl
- In Pulsar 2.4.2, we expose `getLastMessageId()` method in consumerImpl. It will benefit users when they want to know the lag messages, or only consume messages before the current time.                                                     

## Add new `send()` interface in C++/Go
- In Pulsar 2.4.2, we add new `send()` interface in C++/Go, so the `MessageID` will be returned to users. The logic is consistent with that in Java. In Java client, the `MessageId send(byte[] message)` returns `MessageId` for users.

## Ensure consumer background tasks are cancelled after subscribe failures
- In Pulsar 2.4.2, we ensure consumer background tasks are cancelled after subscribe failures. Some background consumer tasks are started in the ConsumerImpl constructor though these are not cancelled if the consumer creation fails, leaving active references to these objects. 

- Before 2.4.2, it's not possible to delete topics when there is a regex consumer attached to them. The reason is that the regex consumer will immediately reconnect and cause the topic to be re-created. 

## Delete topics with regex consumers
In Pulsar 2.4.2, we can delete topics with regex consumers. The followings are our methods:
  - Add a flag in CommandSubscribe so that a regex consumer will never trigger the creation of a topic.
  - Subscribe to a non-existing topic, and a specific error occurs, that the consumer will interpret as a permanent failure and thus will stop retrying.

## Offloading
- There's a bug in how user metadata is attached to a block that if the user doesn't specify both the region and the endpoint, offloading will throw an exception, as you can't add a null value to an immutable map. 

In Pulsar 2.4.2, change elides null to the empty string in these cases, so that offloading can continue.
  
## Reference

Download Pulsar 2.4.2 [here](https://pulsar.apache.org/en/download/). 

If you have any questions or suggestions, contact us with mailing lists or slack. 
- [users@pulsar.apache.org](mailto:users@pulsar.apache.org) 
- [dev@pulsar.apache.org](mailto:dev@pulsar.apache.org) 
- Pulsar slack channel: https://apache-pulsar.slack.com/
- You can self-register at https://apache-pulsar.herokuapp.com/

Looking forward to your contributions to [Pulsar](https://github.com/apache/pulsar).
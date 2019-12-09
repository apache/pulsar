---
author: Xiaolong Ran
authorURL: https://twitter.com/wolf4j1
title: Apache Pulsar 2.4.2
---

We are glad to publish Apache Pulsar 2.4.2. This is the result of a huge 
effort from the community, with over 110 commits, general improvements and bug fixes.

Check out the <b>[release notes](../release-notes.md/)</b> for a detailed list of 
the changes, with links to the relevant pull requests, discussions and documentation.

Regarding improvements and bug fixes introduced, I just want to highlight here a tiny subset of them:

<!--truncate-->

### 

- In Pulsar 2.4.1, we instead of using a shaded JAR to start a java function instance, use different classLoaders to 
load the internal pulsar code, user code, and the interfaces that the two interacts with each other. This is a good 
change, but this change will cause the following two problems:
    - The windowed functions were broken when we changed java function instance to use classLoaders. 
    - When using the `--output-serde-classname` option, functionClassLoader is not set correctly.  

  In Pulsar 2.4.2, we fixed this issue to make sure them works. 

- In Pulsar 2.4.1, Broker fails to start with function worker enabled and broker client using TLS. Looking at the 
startup code when running the function worker with the broker, it is checking for TLS enabled in the `function_worker.yml`
file to determine whether or not to use the TLS port, but when setting TLS enabled on the function worker, 
it is checking the `broker.conf`. Since the function worker is running with the broker, it makes sense to look to 
the `broker.conf` as the single source of truth about whether or not to use TLS. In Pulsar 2.4.2 changed the code to 
check the broker client is configured to use TLS. If it is, then use TLS for the function worker, otherwise use plain text.

- In Pulsar Functions, we support the use of Bookkeeper to store the state of functions. But when user attempts to 
fetch from function state a key that doesn't exist, an NPE will happen. In Pulsar 2.4.2, we add the correct error 
handling for keys that don't exist.

- Since deduplication drops messages based on the the largest sequence id recorded pre-persist, if there’s an error persisting 
to BK, a retry attempt will just be “deduplicated” with no message ever getting persisted. The fix is two-fold:                                                                                                   
    - Double check the pending messages and return error to producer when the dup status is uncertain (eg. message is still pending)
    - Sync back the lastPushed map with the lastStored map after the failures

- In Pulsar Sinks, the data in topics is consumed from the latest location by default. But in some scenarios, users want to 
consume earliest data in sink topic. In Pulsar 2.4.2, we add `--subs-position` for Pulsar Sinks, allow users to consume 
data from specified locations.

- When the cursor is recovered from a ledger, the ledgerHandle is kept open so that we can delete that ledger after 
we update the cursor status. If we attempt to close the cursor before having had any updates on the cursor itself, we 
would end up having a harmless error, saying that we're trying to append to a read-only ledger. Also, when we append on 
the ledger during the close operation, we need to explicitly close the ledger to avoid the expensive recovery when loading the topic.

- If the subscription type on a topic changes, a new dispatcher is created and the old one is discarded. However, this 
old dispatcher is not closed. This will cause a memory leak. If cursor is not durable, the subscription is closed and removed 
from the topic when all consumers are removed. The dispatcher also needs to be closed at this time. Otherwise, 
RateLimiter instances will not be garbage collected, causing a memory leak. In pulsar 2.4.2, When the type of a subscription changes 
and a new dispatcher is created, close the previous one, avoiding memory leaks.

- Instead of sorting the consumers based on priority level and consumer name then pick a active consumer, which could 
cause subscription getting into a flaky state, where the "active" consumer joins and leaves, no consumer is actually 
elected as "active" and consuming the messages. In Pulsar 2.4.2, fixed logic to always pick the first consumer in 
the consumer list without sorting consumers. So consumers will be picked as acive consumer based on the order of 
their subscription.

- In Pulsar 2.4.1, broker tries to clean up stale failed-producer from the connection however, while cleaning up 
producer-future, it tries to remove newly created producer-future rather old-failed producer because of that broker 
still gives below error:
    ```text
    17:22:00.700 [pulsar-io-21-26] WARN  org.apache.pulsar.broker.service.ServerCnx - [/1.1.1.1:1111][453] Producer with id persistent://prop/cluster/ns/topic is already present on the connection
    ```  
  In Pulsar 2.4.2, we remove failed stale producer from the connection.                          

- In Pulsar 2.4.2, we add a few new result api for schema:

    - `getAllVersions`: return the list of schema versions for a given topic.
    - `testCompatibility`: be able to test the compatibility for a schema without registering it
    - `getVersionBySchema`: give a schema definition and find the schema version for it.

- It would be good to expose method getLastMessageId in ConsumerImpl to a public method. eg. some times user would like 
to know the lag messages; or only consume messages before current time. In Pulsar 2.4.2, we expose `getLastMessageId()` method 
from consumerImpl.                                                     

- In Java client, the `MessageId send(byte[] message)` return `MessageId` for users. To make sure the API of C++/Go/Python 
consistent with Java. In Pulsar 2.4.2, we add new `send()` interface, return the `MessageID` to the user.

## Conclusion

If you want to download Pulsar 2.4.2, click [here](https://pulsar.apache.org/en/download/). You can send any questions or suggestions 
to our mailing lists, contribute to Pulsar on [GitHub](https://github.com/apache/pulsar) or join 
the Apache Pulsar community on [Slack](https://apache-pulsar.herokuapp.com/).
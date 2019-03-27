# Frequently Asked Questions
- Getting Started
- Concepts and Design
- Usage and Configuration
- Advanced Questions

---

## Getting Started

### What is the minimum requirements for Apache Pulsar ?
You need three kinds of clusters: bookie, broker, and zookeeper. If you do not have enough resources, you can also run the three clusters on the same machine.
---
## Concepts and Design

### Is ack tied to subscription?
Yes, ack is tied to a particular subscription.

### Where do I look into to tweak load balancing ?
There are a few parameters to look at :
1. The topic assignments to brokers are done in terms of “bundles”, that is in group of topic.
2. Topics are matched to bundles by hashing on the name.
3. A bundle is a hash-range where topics fall into.
4. The default is to have four “bundles” for a namespace.
5. When the traffic increases on a given bundle, it  is split in two and reassigned to a different broker.
6. There are some adjustable thresholds that can be used to control when the split happens, based on the number of topics/partitions, messages in/out, bytes in/out, and so on.
7. It’s also possible to specify a higher number of bundles when creating a namespace.
8. The load-manager threshold controls when a broker should offload some of the bundles to other brokers.

### What is the life-cycle of subscription?
When a subscription is created, it retains all messages published after that (minus explicit TTL). You can drop subscriptions by explicitly unsubscribing (in `Consumer` API) or through the REST/CLI .

### What is a bundle?
In Pulsar, "namespace" is an administrative unit: you can configure most options on a namespace, and the configuration is applied to the topics contained on the namespace. It is convenient to configure settings and operations on a group of topics rather than doing it once per topic.

In general, the pattern is to use a namespace for each user application. So a single user/tenant can create multiple namespaces to manage its own applications.

Concerning topics, topics are assigned to brokers, control the load and move them if a broker becomes overloaded. Rather than doing these operations per each single topic (ownership, load-monitoring, assigning), we do it in bundles, or "groups of topics".

The number of bundles determines the number of brokers you can spread the topics into for a given namespace.

From the perspective of client API or implementation, there is no concept of bundles. Clients look up the topics that they want to publish or consume individually.

On the broker side, the namespace is broken down into multiple bundles, and each bundle is assigned to a different broker. Effectively, bundle is the "unit of assignment" for topics into brokers and this is what the load-manager uses to track the traffic and decide where to place "bundles" and whether to offload them to other brokers.

A bundle is represented by a hash-range. The 32-bit hash space is initially divided equally into the requested bundles. Topics are matched to a bundle by hashing on the topic name.

The default number of bundles is configured in `broker.conf`: `defaultNumberOfNamespaceBundles=4`

When the traffic increases on a given bundle, it is split into two and reassigned to a different broker.

If you want to enable auto-split, set the parameter as `loadBalancerAutoBundleSplitEnable=true`. If you want to trigger unload and reassignment after splitting, set the parameter as `loadBalancerAutoUnloadSplitsEnable=true`.

If you want to have a high traffic on a particular namespace, it's a good practice to specify a higher number of bundles when creating the namespace: `bin/pulsar-admin namespaces create $NS --bundles 64`. This avoids the initial auto-adjustment phase.

You can configure all the thresholds for auto-splitting in the `broker.conf` file. For example, you can configure the number of topics and partitions, messages in/out, bytes in/out, and so on.

### How does the design deal with isolation between tenants? Which concepts enable that and to what extent? 
The isolation between tenants (and topics of the same tenant) happens at many different points. I'll start from the bottom up.

#### Storage
Pulsar uses Apache BookKeeper as its storage layer. BookKeeper's main strength is that each bookie can efficiently serve many different ledgers (segments of topic data). We have tested with 100s of thousand per single node. 

This is because there is a single journal (on its own device) where all the write operations get appended and then the entries are periodically flushed in background on the storage device.

This gives isolation between writes and reads in a bookie. You can read as fast as you can, maxing out the IO on the storage device, and your write throughput and latency are unaffected.

#### Broker
Everything in the broker happens asynchronously. The amount of memory that is used is also capped per broker.

Whenever a broker is marked as overloaded, traffic is quickly shifted (manually or without intervention) to less loaded brokers. The LoadManager component in brokers is dedicated to that.

There are several points of flow control:
- On the producer side, there are limits on the in-flight message for broker bookies, which controls users' speed in publishing messages, so that the speed is not faster than the system can absorb. 
- On the consumer side, it's possible to throttle the delivery to a certain rate.

#### Quotas
You can configure different storage quotas for different tenants/namespaces, and take different actions(block producer, give exception, drop older messages) when the quotas are filled up.

#### Broker level isolation
There is an option to isolate certain tenants/namespaces to a particular set of broker. Typically, you use the option when you are to experiment with different configurations, debug or quickly react to unexpected situations.

For example, a particular user might be triggering a bad behavior in the broker that impacts performance for other tenants.

In this case, the particular user is "isolated" to a subset of brokers that do not serve any other traffic, until a proper fix that correctly handles the condition is deployed.

This is a lightweight option of having multiple clusters for different users, since most of the other parts are still shared (ZK, BK,...).


### Is there a "regex" topic in Pulsar?
There is a regex subscription in Pulsar 2.0 release. See [PIP-13](https://github.com/apache/pulsar/wiki/PIP-13:-Subscribe-to-topics-represented-by-regular-expressions).

### Does Pulsar have, or plan to have, a concept of log compaction where only the latest message with the same key will be kept ?
Yes, see [PIP-9](https://github.com/ivankelly/pulsar-wiki/pull/1/files) for more details.

### When I use an exclusive subscription to a partitioned topic, is the subscription attached to the "whole topic" or to a "topic partition"? 
On a partitioned topic, you can use all the three supported subscription types (exclusive, failover, shared), which are the same as non partitioned topics. 
The “subscription” concept is roughly similar to a “consumer-group” in Kafka. You can have multiple of them in the same topic, with different names.

If you use the “exclusive” type, a consumer tries to consume messages from all partitions. The consumer fails to consume messages if any partition has already been consumed.

The “failover” subscription mode is similar to Kafka consumption mode. In this case, you have an active consumer per partition, the active/stand-by decision is made at the partition level, and Pulsar makes sure to spread the partition assignments evenly across consumers.

### What is the proxy component?
It’s a stateless proxy that speaks Pulsar binary protocol. The motivation is to avoid (or overcome the impossibility of) direct connection between clients and brokers.

--- 

## Usage and Configuration
### Can I manually change the number of bundles after creating namespaces?
Yes, you can split a given bundle manually.

### Is the producer kafka wrapper thread-safe?
The producer wrapper is thread-safe.

### Can I just remove a subscription?
Yes, you can remove a subscription by using the cli tool `bin/pulsar-admin persistent unsubscribe $TOPIC -s $SUBSCRIPTION`.

### How to set subscription modes? Can I create new subscriptions over the WebSocket API?
Yes, you can set most of the producer/consumer configuration option in websocket, by passing them as HTTP query parameters as follows:
`ws://localhost:8080/ws/consumer/persistent/sample/standalone/ns1/my-topic/my-sub?subscriptionType=Shared`

You can create new subscriptions over the WebSocket API. For details, see [Pulsar's WebSocket API](http://pulsar.apache.org/docs/latest/clients/WebSocket/#RunningtheWebSocketservice-1fhsvp).

### Is there any sort of order of operations or best practices on the upgrade procedure for a geo-replicated Pulsar cluster?
In general, it is easy to update the Pulsar brokers, since the brokers don't have local state. The typical rollout is a rolling upgrade, either updating a broker at a time or some percentage of brokers in parallel.

There are no complicated requirements to upgrade geo-replicated clusters, we ensure backward and forward compatibility.

Both clients and brokers report their own protocol versions, and they can disable newer features if the other side does not support them yet.

Additionally, when making metadata breaking format changes (if the need arises), we make sure to spread the changes along at least two releases.

So, when Pulsar cluster recognizes the new formats in a release, it starts using new formats in the next release. 

It is allowed to downgrade a running cluster to a previous version, in case any server problem is identified in production.

### Since Pulsar has configurable retention per namespace, can I set a "forever" value, and keep all data in the namespaces forever?
Retention applies to "consumed" messages, for which the consumer has already acknowledged the processing. By default, retention is set to 0, which means data is deleted as soon as all consumers acknowledge. You can set retention to delay the retention.

It also means that data is kept forever, by default, if the consumers are not acknowledging.

### How can a consumer "replay" a topic from the beginning? Where can I set an offset for the consumer?
1. Use admin API (or CLI tool):
    - Reset to a specific point in time (3h ago)
    - Reset to a message id
2. You can use the client API `seek`.

### When creating a consumer, is the default set to "tail" from "now" on the topic, or from the "last acknowledged" or something else?
When you spin up a consumer, it tries to subscribe to the topic. If the subscription doesn't exist, a new one is created, and it is positioned at the end of the topic ("now"). 

Once you reconnect, the subscription is still there and it is positioned on the last acknowledged messages from the previous session.

### I want some produce lock, i.e., to pessimistically or optimistically lock a specified topic, so only one producer can write at a time and all further producers know that they have to reprocess data before trying again to write a topic.
To ensure only one producer is connected, you just need to use the same "producerName". The broker ensures that no two producers with the same name are publishing on a given topic.

### Is there any work on a Mesos Framework for Pulsar/Bookkeeper at this point? Would this be useful?
We don’t have anything available for Mesos/DCOS, yet nothing could prevent it.
Surely, it is useful.

### Where can I find information about `receiveAsync` parameters? In particular, is there a timeout on `receive`?
There’s no other information about the `receiveAsync()` method. The method doesn’t take any parameters. Currently there’s no timeout on it. You can always set a timeout on the `CompletableFuture` instance, but the problem is how to cancel the future and avoid “getting” the message.

### I'm facing some issues using `.receiveAsync` that it seems to be related with `UnAckedMessageTracker` and `PartitionedConsumerImpl`. We are consuming messages with `receiveAsync`, doing instant `acknowledgeAsync` when message is received, after that the process will delay the next execution of itself. In such scenario we are consuming a lot more messages (repeated) than the number of messages produced. We are using Partitioned topics with setAckTimeout 30 seconds and I believe this issue could be related with `PartitionedConsumerImpl` because the same test in a non-partitioned topic does not generate any repeated message.
PartitionedConsumer is composed of a set of regular consumers, one per partition. To have a single `receive()` abstraction, messages from all partitions are pushed into a shared queue. 

The thing is that the unacked message tracker works at the partition level. So when timeout happens, it’s able to request redelivery for the messages and clear them from the queue when that happens. But if messages were already pushed into the shared queue, the “clearing” part will not happen.

- a quick workaround is to increase the “ack-timeout” to a level in which timeout doesn’t occur in processing
- another option is to reduce the receiver queue size, so that less messages are sitting in the queue

### Can I use bookkeeper newer v3 wire protocol in Pulsar? How can I enable it?
Currently, you cannot use bookkeeper v3 wire protocol in Pulsar. The broker is designed to use v2 protocol, and this is not configurable at the moment.

### Is "kubernetes/generic/proxy.yaml" meant to be used whenever we want to expose a Pulsar broker outside the Kubernetes cluster?
Yes, the “proxy” is an additional component to deploy a stateless proxy frontend that can be exposed through a load balancer, and that doesn’t require direct connectivity to the actual brokers. You do not need to use the proxy within Kubernetes cluster. In some cases, it’s simpler to expose the brokers through `NodePort` or `ClusterIp` for other producer/consumers outsides.

### How can I know when I've reached the end of the stream during replay?
There is no direct way because messages can still be published in the topic, and relying on the `readNext(timeout)` is not precise because the client might be temporarily disconnected from broker at that moment.

One option is to use `publishTimestamp` of messages. When you start replaying, you can check current "now", and then you replay until you hit a message with timestamp >= now.

Another option is to "terminate" the topic. Once a topic is "terminated", no more message can be published on the topic. To know when a topic is terminated, you can check the `hasReachedEndOfTopic()` condition.

A third option is to check the topic stats. This is a tiny bit involved, because it requires the admin client (or using REST) to get the stats for the topic and checking the "backlog". If the backlog is 0, it means you've hit the end.

### How can I prevent an inactive topic to be deleted under any circumstance? I do not want to set time or space limit for a certain namespace.
You can set *infinite* retention time or size, by setting `-1` for either time or
size retention.
For more details, see [Pulsar retention policy](http://pulsar.apache.org/docs/en/cookbooks-retention-expiry/#retention-policies).

### Is there a profiling option in Pulsar, so that we can breakdown the time costed in every stage? For instance, message A stays in queue 1ms, bookkeeper writes in 2ms(interval between sending to bk and receiving ack from bk) and so on.
There are latency stats at different stages: client, bookie and broker. 
For example, the client reports every minute in info logs. 
The broker is accessible through the broker metrics, and bookies store several different latency metrics. 

In broker, there’s just the write latency on Bookkeeper, because there is no other queuing involved in the write path.

### How can multiple readers get all the messages from a topic from the beginning concurrently? I.e., no round-robin, no exclusivity.
You can create reader with `MessageId.earliest`.

### Does broker validate if a property exists or not when producer/consumer connects ?
Yes, broker performs auth&auth while creating producer/consumer and this information presents under namespace policies. So, if auth is enabled, and then broker performs validation.

### For subscribing to a large number of topics like that, would I need to call `subscribe` for each one individually, or is there some sort of wildcard capability?
Currently you can only subscribe individually, (though you can automate it by getting the list of topics and going through it), but we’re working on the wildcard subscribe and we’re targeting that for the next release.

### Is there any difference between a consumer and a reader?
The main difference is that a reader can be used when manually managing the offset/messageId, rather than relying on Pulsar to keep track of it with acknowledgments.
- consumer -> manages subscriptions with acks and auto retention
- reader -> always specifies start message id on creation
<!--not quite sure whether i've understood it correctly.-->

### Question on routing mode for partitioned topics. What is the default configuration and what is used in the Kafka adaptor?
The default is to use the hash of the key on a message. If the message has no key, the producer uses a default partition (picks a random partition and uses it for all messages it publishes). 

This is to maintain the same ordering guarantee when no partitions are there: per-producer ordering.

The same applies when using the Kafka wrapper.

### I'm setting up bookies on AWS d2.4xlarge instances (16 cores, 122G memory, 12x2TB raid-0 hd). Do you have any recommendation for memory configuration for this kind of setup? For configurations like java heap, direct memory and dbStorage_writeCacheMaxSizeMb, dbStorage_readAheadCacheMaxSizeMb, dbStorage_rocksDB_blockCacheSize. BTW, I'm going to use journalSyncData=false since we cannot recover machines when they shutdown. So no fsync is required for every message.
Since the VM has a lot of RAM, you can increase a lot from the defaults. It is recommended to allocate 24G for JVM heap, 16G for WriteCacheMaxSize, 16G for ReadAheadCacheMaxSize, 4G for index, and leave the rest to page cache. The reason for such allocation is that both WriteCacheMaxSize and ReadAheadCacheMaxSize are from JVM direct memory, and rocksdb block cache is allocated in JNI, which is completely out of JVM configuration. Ideally you want to cache most of the indexes, and 4GB rocksdb block cache is enough to index all the data in the 24Tb storage space.

### When there are multiple consumers for a topic, does the broker read once from bookies and send them to all consumers with some buffer? Or does the broker get from bookies all the time for each consumer?
In general, all dispatching is done directly by broker memory. We only read from bookies when consumers are falling behind.

## Advanced Questions
### Why do we choose bookkeeper to store consumer offset instead of zookeeper?
ZooKeeper（ZK） is a “consensus” system. While it exposes a key/value interface, it is not meant to support a large volume of writes per second.

ZK is not an “horizontally scalable” system, because each node receives every transaction and keeps the whole data set. Effectively, ZK is based on a single “log” that is replicated consistently across the participants. 

The max throughput we have observed on a well configured ZK on good hardware is around 10K writes/s. If you want to do more than that, you have to shard it.

To store consumers cursor positions, we write potentially a large number of updates per second. Typically we persist the cursor every second, though the rate is configurable. If you want to reduce the amount of potential duplicates, you can increase the persistent frequency.

With BookKeeper, it’s very efficient to have a large throughput across a huge number of different logs. In our case, we use one log per cursor, and it becomes feasible to persist every single cursor update.

### I tested the performance using PerformanceProducer between two server nodes with 10,000Mbits NIC(and I tested tcp throughput, it can be larger than 1GB/s). I saw that the max msg throughput was around 1000,000 msg/s when using little msg_size(such as 64/128Bytes). When I increased the msg_size to 1028 or larger , the msg/s was decreased sharply to 150,000msg/s, and both has max throughput around 1600Mbit/s, which is far from 1GB/s.  I'm curious why the throughput between producer and broker can't excess 1600Mbit/s ?  It seems that the Producer executor uses only one thread, is this the reason？Then I started two producer client jvm, the throughput increased just a little beyond 1600Mbit/s. Are there any other reasons?
Most probably, when increasing the payload size, you're reaching the disk max write rate on a single bookie.

The following two options can be used to increase throughput (other than just partitioning):

1. Configure it on a given pulsar namespace, refer to "namespaces set-persistence" command. You can enable striping in BK by setting ensemble to bigger than write quorum. E.g. e=5 w=2 a=2. Write two copies of each message but stripe them across five bookies.

2. Configure in bookies. If there are already multiple topics/partitions, you can try to configure the bookies with multiple journals (e.g. 4). This increases the throughput when the journal is on SSDs, since the controller has multiple IO queues and can efficiently sustain multiple threads, because each thread is doing sequential writes.

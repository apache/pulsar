# Frequently Asked Questions
- Getting Started
- Concepts and Design
- Usage and Configuration

---

## Getting Started

### What is the minimum requirements for Apache Pulsar ?
You need 3 kind of clusters: bookie, broker, zookeeper. But if not have enough resource, it's ok to run them on same machine.

---

## Concepts and Design

### Is ack tied to subscription?
Yes, ack is tied to a particular subscription.

### Where should I look into to tweak load balancing ?
There are few parameters to look at :
1. The topic assignments to brokers are done in terms of “bundles”, that is in group of topic
2. Topics are matched to bundles by hashing on the name
3. Effectively, a bundle is a hash-range where topics falls into
4. Initially the default is to have 4 “bundles” for a namespace
5. When the traffic increases on a given bundle, it will be split in 2 and reassigned to a different broker
6. There are some adjustable thresholds that can be used to control when the split happens, based on number of topics/partitions, messages in/out, bytes in/out, etc..
7. It’s also possible to specify a higher number of bundles when creating a namespace
8. There are the load-manager threshold that control when a broker should offload some of the bundles to other brokers

### What is the lifecycle of subscription?
Once it’s created, it retains all messages published after that (minus explicit TTL). Subscriptions can be dropped by explicitly unsubscribing (in `Consumer` API) or through the REST/CLI .

### What is a bundle?
In Pulsar, "namespaces" are the administrative unit: you can configure most options on a namespace and they will be applied on the topics contained on the namespace. It gives the convenience of doing settings and operations on a group of topics rather than doing it once per topic.

In general, the pattern is to use a namespace for each user application. So a single user/tenant, can create multiple namespaces to manage its own applications.

When it comes to topics, we need a way to assign topics to brokers, control the load and move them if a broker becomes overloaded. Rather that doing this operations per each single topic (ownership, load-monitoring, assigning), we do it in bundles, or "groups of topics".

In practical words, the number of bundles determines "into how many brokers can I spread the topics for a given namespace".

From the client API or implementation, there's no concept of bundles, clients will lookup the topics that want to publish/consumer individually.

On the broker side, the namespace is broken down into multiple bundles, and each bundle can be assigned to a different broker. Effectively, bundles are the "unit of assignment" for topics into brokers and this is what the load-manager uses to track the traffic and decide where to place "bundles" and whether to offload them to other brokers.

A bundle is represented by a hash-range. The 32-bit hash space is initially divided equally into the requested bundles. Topics are matched to a bundle by hashing on the topic name.

Default number of bundles is configured in `broker.conf`: `defaultNumberOfNamespaceBundles=4`

When the traffic increases on a given bundle, it will be split in 2 and reassigned to a different broker.

Enable auto-split: `loadBalancerAutoBundleSplitEnable=true` trigger unload and reassignment after splitting: `loadBalancerAutoUnloadSplitsEnable=true`.

If is expected to have a high traffic on a particular namespace, it's a good practice to specify a higher number of bundles when creating the namespace: `bin/pulsar-admin namespaces create $NS --bundles 64`. This will avoid the initial auto-adjustment phase.

All the thresholds for the auto-splitting can be configured in `broker.conf`, eg: number of topics/partitions, messages in/out, bytes in/out, etc...

### How the design deals with isolation between tenants, which concepts enable that and up to what extent, how huge difference can exist between tenants so that impact on each other is noticeable via degraded latency.
The isolation between tenants (and topics of same tenant) happens at many different points. I'll start from the bottom up.

#### Storage
You're probably familiar with BookKeeper, but of the main strength is that each bookie can efficiently serve many different ledger (segments of topic data). We tested with 100s of thousand per single node.

This is because there is a single journal (on its own device) where all the write operations gets appended and then the entries are periodically flushed in background on the storage device.

This gives isolation between writes and reads in a bookie. You can read as fast as you can, maxing out the IO on the storage device, but your write throughput and latency are going to be unaffected.

#### Broker
Everything in the broker happens asynchronously. The amount of memory that is used is also capped per broker.

Whenever the broker is marked as overloaded, traffic can be quickly shifted (manually or without intervention) to less loaded brokers. LoadManager component in brokers is dedicated to that.

There are several points of flow control:
- On the producer side, there are limits on the in-flight message for broker bookies, that will slow down users trying to publish faster that the system can absorb
- On the consumer side, it's possible to throttle the delivery to a certain rate

#### Quotas
Can configure different storage quotas for different tenants/namespaces and take different actions when the quotas are filled up (block producer, give exception, drop older messages).

#### Broker level isolation
There is the option to isolate certain tenants/namespaces to a particular set of broker. Typically the reason for using that was to experiment with different configurations, debugging and quickly react to unexpected situations.

For example, a particular user might be triggering a bad behavior in the broker that can impact performance for other tenants.

In this case, the particular user can be "isolated" a subset of brokers that will not serve any other traffic, until a proper fix that correctly handles the condition can be deployed.

This is a lightweight option of having multiple clusters for different users, since most of the other parts are still shared (ZK, BK,...).


### Is there "regex" topic in Pulsar?
There is regex subscription coming up in Pulsar 2.0. See [PIP-13](https://github.com/apache/pulsar/wiki/PIP-13:-Subscribe-to-topics-represented-by-regular-expressions).

### Does Pulsar have, or plan to have, a concept of log compaction where only the latest message with the same key will be kept ?
Yes, see [PIP-14](https://github.com/apache/pulsar/wiki/PIP-14:-Topic-compaction) for more details.

### When I use an exclusive subscription to a partitioned topic, is the subscription attached to the "whole topic" or to a "topic partition"? 
On a partitioned topic, you can use all the 3 supported subscription types (exclusive, failover, shared), same as with non partitioned topics. 
The “subscription” concept is roughly similar to a “consumer-group” in Kafka. You can have multiple of them in the same topic, with different names.

If you use “exclusive”, a consumer will try to consume from all partitions, or fail if any partition is already being consumed.

The mode similar to Kafka is “failover” subscription. In this case, you have 1 active consumer per partition, the active/stand-by decision is made at the partition level, and Pulsar will make sure to spread the partition assignments evenly across consumer.

### What is the proxy component?
It’s a component that was introduced recently. Essentially it’s a stateless proxy that speaks that Pulsar binary protocol. The motivation is to avoid (or overcome the impossibility) of direct connection between clients and brokers.

--- 

## Usage and Configuration
### Can I manually change the number of bundles after creating namespaces?
Yes, you can split a given bundle manually.

### Is the producer kafka wrapper thread-safe?
The producer wrapper should be thread-safe.

### Can I just remove a subscription?
Yes, you can use the cli tool `bin/pulsar-admin persistent unsubscribe $TOPIC -s $SUBSCRIPTION`.

### How are subscription modes set? Can I create new subscriptions over the WebSocket API?
Yes, you can set most of the producer/consumer configuration option in websocket, by passing them as HTTP query parameters like:
`ws://localhost:8080/ws/consumer/persistent/sample/standalone/ns1/my-topic/my-sub?subscriptionType=Shared`

see [the doc](http://pulsar.apache.org/docs/latest/clients/WebSocket/#RunningtheWebSocketservice-1fhsvp).

### Is there any sort of order of operations or best practices on the upgrade procedure for a geo-replicated Pulsar cluster?
In general, updating the Pulsar brokers is an easy operation, since the brokers don't have local state. The typical rollout is a rolling upgrade, either doing 1 broker at a time or some percentage of them in parallel.

There are not complicated requirements to upgrade geo-replicated clusters, since we take particular care in ensuring backward and forward compatibility.

Both the client and the brokers are reporting their own protocol version and they're able to disable newer features if the other side doesn't support them yet.

Additionally, when making metadata breaking format changes (if the need arises), we make sure to spread the changes along at least 2 releases.

This is to always allow the possibility to downgrade a running cluster to a previous version, in case any server problem is identified in production.

So, one release will understand the new format while the next one will actually start using it.

### Since Pulsar has configurable retention per namespace, can I set a "forever" value, ie., always keep all data in the namespaces?
So, retention applies to "consumed" messages. Ones, for which the consumer has already acknowledged the processing. By default, retention is 0, so it means data is deleted as soon as all consumers acknowledge. You can set retention to delay the retention.

That also means, that data is kept forever, by default, if the consumers are not acknowledging.

There is no currently "infinite" retention, other than setting to very high value.

### How can a consumer "replay" a topic from the beginning, ie., where can I set an offset for the consumer?
1. Use admin API (or CLI tool):
    - Reset to a specific point in time (3h ago)
    - Reset to a message id
2. You can use the client API `seek`.

### When create a consumer, does this affect other consumers ?
The key is that you should use different subscriptions for each consumer. Each subscription is completely independent from others.

### The default when creating a consumer, is it to "tail" from "now" on the topic, or from the "last acknowledged" or something else?
So when you spin up a consumer, it will try to subscribe to the topic, if the subscription doesn't exist, a new one will be created, and it will be positioned at the end of the topic ("now"). 

Once you reconnect, the subscription will still be there and it will be positioned on the last acknowledged messages from the previous session.

### I want some produce lock, i.e., to pessimistically or optimistically lock a specified topic so only one producer can write at a time and all further producers know they have to reprocess data before trying again to write a topic.
To ensure only one producer is connected, you just need to use the same "producerName", the broker will ensure that no 2 producers with same name are publishing on a given topic.

### I tested the performance using PerformanceProducer between two server node with 10,000Mbits NIC(and I tested tcp throughput can be larger than 1GB/s). I saw that the max msg throughput is around 1000,000 msg/s when using little msg_size(such as 64/128Bytes), when I increased the msg_size to 1028 or larger , then the msg/s will decreased sharply to 150,000msg/s, and both has max throughput around   1600Mbit/s, which is far from 1GB/s.  And I'm curious that the throughput between producer and broker why can't excess 1600Mbit/s ?  It seems that the Producer executor only use one thread, is this the reason？Then I start two producer client jvm, the throughput increased not much, just about little beyond 1600Mbit/s. Any other reasons?
Most probably, when increasing the payload size, you're reaching the disk max write rate on a single bookie.

There are few tricks that can be used to increase throughput (other than just partitioning)

1. Enable striping in BK, by setting ensemble to bigger than write quorum. E.g. e=5 w=2 a=2. Write 2 copies of each message but stripe them across 5 bookies

2. If there are already multiple topics/partitions, you can try to configure the bookies with multiple journals (e.g. 4). This should increase the throughput when the journal is on SSDs, since the controller has multiple IO queues and can efficiently sustain multiple threads each doing sequential writes

- Option (1) you just configure it on a given pulsar namespace, look at "namespaces set-persistence" command

- Option (2) needs to be configured in bookies

### Is there any work on a Mesos Framework for Pulsar/Bookkeeper this point? Would this be useful?
We don’t have anything ready available for Mesos/DCOS though there should be nothing preventing it

It would surely be useful.


### Is there an HDFS like interface?
Not for Pulsar.There was some work in BK / DistributedLog community to have it but not at the messaging layer.

### Where can I find information about `receiveAsync` parameters? In particular, is there a timeout as in `receive`?
There’s no other info about `receiveAsync()`. The method doesn’t take any parameters. Currently there’s no timeout on it. You can always set a timeout on the `CompletableFuture` itself, but the problem is how to cancel the future and avoid “getting” the message.

What’s your use case for timeout on the `receiveAsync()`? Could that be achieved more easily by using the `MessageListener`?

### Why do we choose to use bookkeeper to store consumer offset instead of zookeeper? I mean what's the benefits?
ZooKeeper is a “consensus” system that while it exposes a key/value interface is not meant to support a large volume of writes per second.

ZK is not an “horizontally scalable” system, because every node receive every transaction and keeps the whole data set. Effectively, ZK is based on a single “log” that is replicated consistently across the participants. 

The max throughput we have observed on a well configured ZK on good hardware was around ~10K writes/s. If you want to do more than that, you would have to shard it.. 

To store consumers cursor positions, we need to write potentially a large number of updates per second. Typically we persist the cursor every 1 second, though the rate is configurable and if you want to reduce the amount of potential duplicates, you can increase the persistent frequency.

With BookKeeper it’s very efficient to have a large throughput across a huge number of different “logs”. In our case, we use 1 log per cursor, and it becomes feasible to persist every single cursor update.

### I'm facing some issue using `.receiveAsync` that it seems to be related with `UnAckedMessageTracker` and `PartitionedConsumerImpl`. We are consuming messages with `receiveAsync`, doing instant `acknowledgeAsync` when message is received, after that the process will delay the next execution of itself. In such scenario we are consuming a lot more messages (repeated) than the num of messages produced. We are using Partitioned topics with setAckTimeout 30 seconds and I believe this issue could be related with `PartitionedConsumerImpl` because the same test in a non-partitioned topic does not generate any repeated message.
PartitionedConsumer is composed of a set of regular consumers, one per partition. To have a single `receive()` abstraction, messages from all partitions are then pushed into a shared queue. 

The thing is that the unacked message tracker works at the partition level.So when the timeout happens, it’s able to request redelivery for the messages and clear them from the queue when that happens,
but if the messages were already pushed into the shared queue, the “clearing” part will not happen.

- the only quick workaround that I can think of is to increase the “ack-timeout” to a level in which timeout doesn’t occur in processing
- another option would be to reduce the receiver queue size, so that less messages are sitting in the queue

### Can I use bookkeeper newer v3 wire protocol in Pulsar? How can I enable it?
The answer is currently not, because we force the broker to use v2 protocol and that's not configurable at the moment.

### Is "kubernetes/generic/proxy.yaml" meant to be used whenever we want to expose a Pulsar broker outside the Kubernetes cluster?
Yes, the “proxy” is an additional component to deploy a stateless proxy frontend that can be exposed through a load balancer and that doesn’t require direct connectivity to the actual brokers. No need to use it from within Kubernetes cluster. Also in some cases it’s simpler to have expose the brokers through `NodePort` or `ClusterIp` for other outside producer/consumers.

### Is there a way of having both authentication and the Pulsar dashboard working at same time?
The key is that with authorization, the stats collector needs to access the APIs that require the credentials. That’s not a problem for stats collected through Prometheus but it is for the “Pulsar dashboard” which is where the per-topic stats are shown. I think that should be quite easy to fix.

### How can I know when I've reached the end of the stream during replay?
There is no direct way because messages can still be published in the topic, and relying on the `readNext(timeout)` is not precise because the client might be temporarily disconnected from broker in that moment.

One option is to use `publishTimestamp` of messages. When you start replaying you can check current "now", then you replay util you hit a message with timestamp >= now.

Another option is to "terminate" the topic. Once a topic is "terminated", no more message can be published on the topic, a reader/consumer can check the `hasReachedEndOfTopic()` condition to know when that happened.

A final option is to check the topic stats. This is a tiny bit involved, because it requires the admin client (or using REST) to get the stats for the topic and checking the "backlog". If the backlog is 0, it means we've hit the end.

### How can I prevent an inactive topic to be deleted under any circumstance? I want to set no time or space limit for a certain namespace.
There’s not currently an option for “infinite” (though it sounds a good idea! maybe we could use `-1` for that). The only option now is to use INT_MAX for `retentionTimeInMinutes` and LONG_MAX for `retentionSizeInMB`. It’s not “infinite” but 4085 years of retention should probably be enough!

### Is there a profiling option in Pulsar, so that we can breakdown the time costed in every stage? For instance, message A stay in queue 1ms, bk writing time 2ms(interval between sending to bk and receiving ack from bk) and so on.
There are latency stats at different stages. In the client (eg: reported every 1min in info logs). 
In the broker: accessible through the broker metrics, and finally in bookies where there are several different latency metrics. 

In broker there’s just the write latency on BK, because there is no other queuing involved in the write path.

### How can I have multiple readers that each get all the messages from a topic from the beginning concurrently? I.e., no round-robin, no exclusivity
you can create reader with `MessageId.earliest`


### Does broker validate if a property exists or not when producer/consumer connects ?
yes, broker performs auth&auth while creating producer/consumer and this information presents under namespace policies.. so, if auth is enabled then broker does validation

### From what I’ve seen so far, it seems that I’d instead want to do a partitioned topic when I want a firehose/mix of data, and shuffle that firehose in to specific topics per entity when I’d have more discrete consumers. Is that accurate?
Precisely, you can use either approach, and even combine them, depending on what is more convenient for the use case. The general traits to choose one or the other are: 

- Partitions -> Maintain a single “logical” topic but scale throughput to multiple machines. Also, ability to consume in order for a “partition” of the keys. In general, consumers are assigned a partition (and thus a subset of keys) without specifying anything.

- Multiple topics -> When each topic represent some concrete existing “concept” in the application and it is “finite” (eg: using a topic per each user when the number of users is unbound and can be in the 100s of millions it’s not a good idea), within 10s or 100s of thousands. Having multiple topics makes it easier for a consumer to consume a specific portion of messages.

### For subscribing to a large number of topics like that, would i need to call `subscribe` for each one individually, or is there some sort of wildcard capability?
Currently you can only subscribe individually, (though you can automate it by getting the list of topics and going through it), but we’re working on the wildcard subscribe and we’re targeting that for next release.

### Hi, is the difference between a consumer and a reader documented somewhere?
Main difference: a reader can be used when manually managing the offset/messageId, rather than relying on Pulsar to keep track of it with the acknowledgments
- consumer -> managed subscriptions with acks and auto retention
- reader -> always specify start message id on creation


### Hey, question on routing mode for partitioned topics. What is the default configuration and what is used in the Kafka adaptor?
The default is to use the hash of the key on a message. If the message has no key, the producer will use a “default” partition (picks 1 random partition and use it for all the messages it publishes). 

This is to maintain the same ordering guarantee when no partitions are there: per-producer ordering.

The same applies when using the Kafka wrapper.

### I'm setting up bookies on AWS d2.4xlarge instances (16 cores, 122G memory, 12x2TB raid-0 hd). Do you have any recommendation for memory configuration for this kind of setup? For configurations like java heap, direct memory and dbStorage_writeCacheMaxSizeMb, dbStorage_readAheadCacheMaxSizeMb, dbStorage_rocksDB_blockCacheSize. BTW, I'm going to use journalSyncData=false since we cannot recover machines when they shutdown. So no fsync is required for every message.
Since the VM has lot of RAM you can increase a lot from the defaults and leave the rest page cache. For JVM heap I'd say ~24g. WriteCacheMaxSize and ReadAheadCacheMaxSize are both coming from JVM direct memory.  I'd say to start with 16g @ 16g. For rocksdb block cache, which is allocated in JNI so it's completely out of JVM configuration, ideally you want to cache most of the indexes. I'd say 4gb should be enough to index all the data in the 24Tb storage space.

### When there are multiple consumers for a topic, the broker reads once from bookies and send them to all consumers with some buffer? or go get from bookies all the time for each consumers ?
In general, all dispatching is done directly by broker memory. We only read from bookies when consumer are falling behind.

### My bookies ledgers are running out of disk space? How can I find out what went wrong
TBD
- Expiry/TTL
- Backlog Quotas
- Retention Settings
- Bookie configuration e.g. Garbage Collection

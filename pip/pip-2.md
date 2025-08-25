# PIP-2: Non Persistent topic

* **Status**: Implemented
 * **Author**: [Rajan Dhabalia](https://github.com/rdhabalia)
 * **Pull Request**: [#538](https://github.com/apache/incubator-pulsar/pull/538)

## Introduction
 
Introducing non-persistent topic to support applications that only want to consume real time published messages and do not need persistent guarantee that can also reduce message-publish latency by removing overhead of persisting messages.
 
In pulsar, topic is a key entity that used to connect producers and consumers. Broker will create separate entity for persistent and non-persistent topic. As name suggests, non-persist topic does not persist messages into any durable storage disk unlike persistent topic where messages are durably persisted on multiple disks. Therefore, as soon as broker receives published message, it immediately delivers this message to all connected subscribers without caching or persisting them into any storage.
Broker identifies topic type based on topic-name prefix eg: `persistent` , `non-persistent` and creates appropriate topic entity to serve producers and consumers.

## Broker Changes
 
Non-persistent topics may consume higher network bandwidth of broker compare to persistent topic. Therefore, there should be a way in broker to control and balance out broker’s system-resources among all the topics.
 
### Per topic throttling

Sometimes, continuous stream of published messages from one topic may over utilize broker’s network-bandwidth or CPU, which may impact performance of other topics owned by that broker. Therefore, broker should be able to throttle number of in flight messages per connection So, if producer tries to publish messages higher than configured rate, then broker silently drops those new incoming messages by sending success ack to the publisher and not delivering them to the subscribers.
However, if message has been dropped by a broker then broker can notify producer by sending special message-id (`ledgerId=-1, entryId=-1`) or flag (`msgDropped`)  in `sendReceipt` ack command. Client library can handle msg-drop indication in two ways:
1. Ignore message-drop and just do logging about msg-dropped or
2. It can notify producer by publish failure. But this behavior violates the message ordering because client-library will not retry this failed message and subsequent messages may get published successfully without getting dropped at broker.

Therefore, broker can send special message-id when it drops the message and client library can follow first option, where it ignores the message-drop and does not notify producer with publish failure.
 
Number of in-flight messages per connection can be configured at [broker-config](https://github.com/apache/incubator-pulsar/blob/master/conf/broker.conf):
```properties
# Max concurrent non-persistent message can be processed per connection
maxConcurrentNonPersistentMessagePerConnection=1000
```

### Message drop at broker
 
* **On message publish**
Broker allows only fixed number of inflight messages per connection and it throttles all other messages by dropping and not delivering them to any subscribers. This publish message-drop can be monitored using `admin-stats` api.
 
* **On message dispatching**
Broker delivers message to consumer only if the consumer has enough permit to consume message and consumer TCP channel is writable, otherwise broker drops the message without delivering it to the consumer and this message-drop rate can we monitored using `admin-stats` api. **NOTE: Therefore, consumer’s receiver queue size (to accommodate enough permits) and TCP-receiver window size (to keep channel writable) should be configured properly to avoid message drop while dispatching it.**


### Broker isolation to serve only non-persistent topic

Sometimes, there would be a need to configure few dedicated brokers in a cluster, to just serve non-persistent topics. With the help of below configurations, only brokers that have non-persistent topic mode enabled will load and serve non-persistent topics.
```properties
# It disables broker to load persistent topics
enablePersistentTopics=false
# It enables broker to load non-persistent topics
enableNonPersistentTopics=true
```

### Topic stats
Broker supports `stats` and `internal-stats` admin API to monitor non-persistent topic stats.

## Client lib changes:
It doesn’t require any client API or library changes, and application can continue using same existing API to publish and consume messages. However, application has to pass `non-persistent` prefix in the topic name to publish or subscribe for non-persistent topic.
Eg:
`non-persistent://my-property/us-west/my-namespace/my-topic`
 
### Consumer API

```java
PulsarClient client = PulsarClient.create("pulsar://localhost:6650");

Consumer consumer = client.subscribe(
           "non-persistent://sample/standalone/ns1/my-topic",
           "my-subscribtion-name");
```

### Producer API

```java
PulsarClient client = PulsarClient.create("pulsar://localhost:6650");

Producer producer = client.createProducer(
           "non-persistent://sample/standalone/ns1/my-topic");
```

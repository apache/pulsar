# PIP-26: Delayed Message Delivery
* Status: Proposed
* Author: [Penghui Li](https://github.com/codelipenghui), [Jia Zhai](https://github.com/jiazhai)
* Pull Request: [#2375](https://github.com/apache/pulsar/issues/2375), [#4062](https://github.com/apache/pulsar/pull/4062)
* Mailing List Discussion: https://lists.apache.org/thread.html/977a8042581819769809f298bd2a2e9fd6b78598792c98069eb691e5@%3Cdev.pulsar.apache.org%3E
* Release: N/A

## Motivation
Scheduled and delayed message delivery is a very common feature to support in a message system. Basically individual message can have a header which will be set by publisher and based on the header value the broker should hold on delivering messages until the configured delay or the scheduled time is met. 

## Usage
The delayed message delivery feature is enabled per message at producer side.

Delayed messages publish example in client side:

```java
// message to be delivered at the configured delay interval
producer.newMessage().deliverAfter(3L, TimeUnit.Minute).value("Hello Pulsar!").send();

// message to be delivered at the configure time.
producer.newMessage().deliverAt((new Date(2018, 10, 31, 23, 00, 00)).getTime())
```

To enable or disable delay message feature:

```shell
pulsar-admin namespaces

enable-delayed-message 	     Enable delayed message for all topics of the namespace
  Usage: enable-delayed-message [options] tenant/namespace
  
  	Options:
  	  -p --time-partition-granularity
  	  	Granularities of time will be partitioned, every time partition will be 
  	  	stored into legders and current time partition will be load in memory 
  	  	and organized in a TimeWheel.(eg: 30s, 5m, 1h, 3d, 2w)
  	  	Default: 5m
  	  -t --tick-duration
  	  	The duration between tick in TimeWheel. Calculate ticks per wheel
  	  	using time-partition-granularity / tick-duration before load time 
  	  	partition into a TimeWheel.(eg: 500ms, 1s, 5m)
  	    Default: 1s
  	    
disable-delayed-message 	 Disable delayed message for all topics of the namespace
  Usage: disable-delayed-message tenant/namespace
```

## Design

### Delayed Message Index

The “DelayedMessageIndex” will be implemented using a [TimeWheel approach](http://www.cs.columbia.edu/~nahum/w6998/papers/sosp87-timing-wheels.pdf). We will be maintaining a delayed index, indexing the delayed message by its time and actual message id.

The index is partitioned by the delayed time. Each time partition will be stored using one (or few) ledger(s). For example, if we are configuring  the index to be partitioned by 5 minutes, we will store the index data for every 5 minutes by its delayed time. The latest time partition will be loaded in memory and organized in a TimeWheel.

The TimeWheel is indexed by ticks. For example, if we configured the tick to be 1 second, we will be maintaining 300 ticks for 5 minutes’ index. A timer task is scheduled every tick, and it will pick the indexed message from the TimeWheel and dispatch them to the real consumers.

After completing dispatching the messages in current TimeWheel, it will load the TimeWheel from the next time partition.

Delayed message option ` time-partition-granularity ` and `tick-duration` properly be reset to adapt delay message throughput change.   ` time-partition-granularity `  can't be shrink. For example, exist config is time-partition-granularity = 5m and tick-duration = 1s, delay message index will store in 300 slot, If increase the time-partition-granularity to 10m, when load next time partition TimeWheel will init with 600 slot, Timewheel has enough slot to maintain already exist time partition(5m), but if decrease the time-partition-granularity to 2m, Timewheel can't load already exist time partition(5m) into 120 slot. So regardless the time-partition-granularity shrink first, It's can be improve by split time partition when load time partition. For time-partition-granularity, increase or decrease just affect precision of delay time.

Delay message feature conflict with TTL and Backlog Quota(consumer_backlog_eviction). So it's necessary write a doc to explain the conflict result. Next, if support ledger compact feature, it's can be improve by this feature, relocate un-ack messages to a new ledger, old ledger can be safe delete.

### Delayed Message Index Cursor

We will maintain a `cursor` for ensure the messages will be indexed correctly in the `DelayMessageIndex`, so we don’t miss dispatching any messages.

### Recovery

Since we are organizing the delayed message index using ledgers, and have a cursor to ensure messages are correctly indexed. So when a broker fails due to any reason, and the topic is recovered, we can figure out the latest time partition and load the index from the ledgers. 

## Changes

### Protocol Changes

In order to support `delayed` and `scheduled` messages, we need to add following fields in `MessageMetadata`.

```protobuf
message MessageMetadata {

    // the message will be delayed at delivery by `delayed_ms` milliseconds.
    optional int64 delayed_ms 	= 18;

}
```

### Broker Changes

The publish path will not be changed. All the messages will still be first appended to the managed ledger as normal. As the delivery related message attributes will be ignored for `failover` and `exclusive` subscriptions to ensure FIFO ordering.

The change happens at `MultipleConsumers` dispatcher. At the dispatching time, the dispatcher will check the delivery attributes at the message header. If a message is qualified for delivery (exceeding the configured delayed/scheduled time), the message will be dispatched as normal; otherwise it will be added back to the “delayed message index”. Those delayed messages will be delayed at dispatching from the “delayed message index”. 

### Client Changes

#### TypedMessageBuilder

```java
public interface TypedMessageBuilder<T> {
    
    /**
     * Build a message to be delivered at the configured delay interval : <tt>delayTime</tt>.
     * @return typed message builder.
     */
	  TypedMessageBuilder<T> delayedAt(long delayTime, TimeUnit timeUnit);
    
    /**
     * Build a message to be delivered at the configure time.
     *
     * @return typed message builder.
     */
    TypedMessageBuilder<T> scheduledAt(Date date);
}
```

#### Producer

Delay message can not send in bulk, because define the delay property in MessageMetadata.

#### Consumer

Both the `delayed` and `scheduled` messages will be appended to the topic. The `delayed` and `scheduled` message attributes are only applied to `shared` subscription. It means both exclusive and failover subscriptions will be ignoring these message delivery attributes to ensure FIFO delivery.

## Test Plan

Unit tests + Integration Tests

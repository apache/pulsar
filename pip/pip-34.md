# PIP-34: Add new subscribe type Key_shared

* **Status**: Implemented
 * **Author**: Jia Zhai
 * **Pull Request**: https://github.com/apache/pulsar/pull/4079
 * **Mailing List discussion**:
 * **Release**: 2.4.0

## Motivation
At present, there are 3 kinds of subscription modes: exclusive, shared, and failover. Shared mode subscription is used a lot, because consumers could effectively parallel consume messages of same partition.  But using shared mode will not keep the message order of same key, It would be good to leverage share mode, while also keeping the ordering of messages.

A failover type subscription with muti-partitions could partly solve the issue. but the consumer number is tied with partition number, we could not add consumer to quick up message dispatch.

This Proposal trying to introduce a new subscribe type Key_shared to extend shared type. By Key_shred, one partition could have several consumers to parallel consume messages, while all messages with the same key will be dispatched to only one consumer.

## Proposal and changes

The main idea is to bring a hash routing layer in new Key_Shared dispatcher. Each consumer in the Key_Shared subscription serves its own ranges of hash value. And when a message needs to be dispatched to a consumer, dispatcher first get the Key of message, do the hash for the key, and send this message to the consumer that serves this hash value.

The main work is on the hash layer and the new dispatcher.

### Hash layer
As in the mail discussing, Any hash mechanism that can map a key to a
consumer should work here.  We will make the hashing mechanism pluggable in this proposal.


The hash value of message key determines the target consumer. The hash layer has the following requirements:
1. Each consumer serves a fixed range of hash value.
1. The whole range of hash value could be covered by all the consumers.
1. Once a consumer is removed, the left consumers could still serve the whole range.

Here is an example hash method: In the dispatcher, broker could collect the dispatch rate for each consumer. 
When a new consumer is added, we could choose the busiest consumer and split its hash range, and share a half of the hash range to the new consumer.
When a consumer is closed, we could assign its hash range to adjacent consumer, which has less dispatch rate.

Here is a picture show the rough idea:
![picture-1](https://gist.githubusercontent.com/jiazhai/3172c8eadabc41612dece24fce60fc7f/raw/f35f55a93fddbf047a4dd75c690f489e2a0abe30/1.png)

There are 3 kinds of blocks, each block represents one consumer. vertical axis represents the hash range value, while the horizontal axis represents time.
1. at time 0, this subscribe is created, and the first consumer - C1 is added, all the hash range(0--1) is served by consumer C1.
1. at time T1, a new consumer - C2 added in, and hash range(0--0.5) is still served by C1, while the other half of hash range(0.5--1) is served by new consumer C2.
1. at time T2, a new consumer - C3 added in, and if C2 has bigger dispatcher rate than C1, the split and share the hash range of C2. hash range (0.5--0.75) still served by C2, while C3 serve hash range(0.75--1).
1. at time T3, C1 is closed, since C1 is adjacent to C2, the hash range of it will be assigned to C2. and C2 will serve(0--0.75)
1. at time T4, C2 is closed, and its hash range is assigned to C3. C3 will serve the whole range(0--1).

### change in PulsarApi.proto
Add a new sub type in CommandSubscribe.SubType. 
Add a new field in MessageMetadata, which served for this feature only. By using a new Key, it could avoid impacting other features. e.g. user want to use original key for compact/partition-routing, while want to use a new key for the key ordering.
 
```
message CommandSubscribe {
	enum SubType {
		Exclusive = 0;
		Shared    = 1;
		Failover  = 2;
		Key_Shared = 3; // add new type here < ==
	}

message MessageMetadata {
	...
	optional string ordering_key = 18;
```

### add a new dispatcher in broker side.

PersistentStickyKeyDispatcherMultipleConsumers.
The main methods will include:
```
void addConsumer(Consumer consumer) throws BrokerServiceException {
// add consumer 
// and update the hash range of related consumer.
}

void removeConsumer(Consumer consumer) throws BrokerServiceException {
// remove consumer
// and update the hash range of related consumer.
}

Consumer getConsumer(String key) {
// return the consumer that serves this hash key
}

// once complete read entries from BookKeeper, 
// dispatch messages to consumer according to the key value.
void readEntriesComplete(List<Entry> entries, Object ctx) {
// 1. fetch Key out of Entry.
// 1. dispatch message to target consumer.
// 1. if consumer not have available permit, add message to messagestoReplay.
} 
```

## Some future work
Batch message: since message is dispatched by key in broker. If we want to support batch message, we should add a key-based batch.
Consumer priority: since currently it is dispatch by key, the priority is not used. If we want to use this, we could add it in the future.

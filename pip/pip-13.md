# PIP-13: Subscribe to topics represented by regular expressions

* **Status**: Implemented
 * **Author**: [Jia Zhai](https://github.com/zhaijack)
 * **Pull Request**:
   - [#1103](https://github.com/apache/incubator-pulsar/pull/1103)
   - [#1165](https://github.com/apache/incubator-pulsar/pull/1165)
   - [#1175](https://github.com/apache/incubator-pulsar/pull/1175)
 * **Mailing List discussion**: 


## Motivation
The consumer needs to handle subscription to topics represented by regular expressions. The scope is `namespace` in first stage, all topics/patten should be targeted in same namespace, This will make easy authentication and authorization control.

At last, we should add and implementation a serials of new methods in `PulsarClient.java`
```java
Consumer subscribe(Collection<String> topics, String subscription);
Consumer subscribe(Pattern topicsPattern, String subscription);
```

The goals the should be achieved are these below, we could achieve it one by one:
- support subscription to multiple topics in the same namespace (no guarantee on ordering between topics)
- support regex based subscription
- auto-discover topic addition/deletion

## Design

### support subscription to multiple topics
This will need a new implementation of `ConsumerBase` which wrapper over multiple single-topic-consumers, let’s name it as `TopicsConsumerImpl`. 
When user call new method
`Consumer subscribe(Collection<String> topics, String subscription);`
It will iteratively new a `ConsumerImpl` for each topic, and return a `TopicsConsumerImpl`. The main work is:
 
1. This `TopicsConsumerImpl` class should provide implementation of abstract methods in `ConsumerBase`, Should also provide some specific methods such as:
```java
// maintain a map for all the <Topic, Consumer>, after we subscribe all the topics.
private final ConcurrentMap<String, ConsumerImpl> consumers = new ConcurrentHashMap<>();
// get topics
Set<String> getTopics();
// get consumers
List<ConsumerImpl> getConsumers();

// subscribe a topic
void subscribeTopic(String topic);
// unSubscribe a topic
void unSubscribeTopic(String topic);
```

2. While Message receive/ack, the message identify is needed. In the implementation, we need handle Message identify(MessageId) differently for some of the abstract methods in `ConsumerBase`, because we have to add `MessageId` with additional `String topic` or `consumer id`, Or we may need to change `MessageIdData` in `PulsarApi.proto`.



### support regex based subscription.
As mentioned before, the scope is `namespace`. The main work is:

1. In above `TopicsConsumerImpl` class, need to keep the `Pattern`, which was passed in from api for subscription.
2. leverage currently pulsar admin API of `getList` to get a list of Topics.
In `interface PersistentTopics `:
```java
List<String> getList(String namespace) throws PulsarAdminException;
List<String> getPartitionedTopicList(String namespace) throws PulsarAdminException;
```

3. The process of new method `Consumer subscribe(String namespace, Pattern topicsPattern, String subscription)` should be like this:
- call method `List<String> getList(String namespace)` to get all the topics;
- Use `topicsPattern` to filter out the matched sub-topics-list.
- construct the `TopicsConsumerImpl` with the the sub-topics-list.

### auto-discover topic addition/deletion
The main work is:
1. provide a listener, which based on topics changes, to do subscribe and unsubscribe on individual topic when target topic been changed(remove/add).
```java
Interface TopicsChangeListener {
	// unsubscribe and delete ConsumerImpl in the `consumers` map in `TopicsConsumerImpl` based on added topics.
	void onTopicsRemoved(Collection<String> topics);
	// subscribe and create a list of new ConsumerImpl, added them to the `consumers` map in `TopicsConsumerImpl`.
	void onTopicsAdded(Collection<String> topics);
}
```
Add a method `void registerListener(TopicsChangeListener listener)` to `TopicsConsumerImpl`

2. Based on above work, using a timer, periodically call `List<String> getList(String namespace)`. And comparing the filtered fresh sub-topics-list with current topics holden in `TopicsConsumerImpl`, try to get 2 lists: `newAddedTopicsList` and  `removedTopicsList`.
3. If the 2 lists not empty, call `TopicsChangeListener.onTopicsAdded(newAddedTopicsList)`, and `TopicsChangeListener.onTopicsRemoved(removedTopicsList)` to do subscribe and unsubscribe, and update `consumers` map in `TopicsConsumerImpl`.

# Changes
The changes will be mostly on the surface and on client side:
1. add and implementation a serials of new methods in `org.apache.pulsar.client.api.PulsarClient.java`
```java
Consumer subscribe(Collection<String> topics, String subscription);
Consumer subscribe(Pattern topicsPattern, String subscription);
```
2. add and implenentation of new `Consumer`, which is `TopicsConsumerImpl` , returned by above `subscribe` method

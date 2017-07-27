Normal topics can be served only by a single {% popover broker %}, which limits the topic's maximum throughput. *Partitioned topics* are a special type of topic that be handled by multiple brokers, which allows for much higher throughput.

Behind the scenes, a partitioned topic is actually implemented as N internal topics, where N is the number of partitions. When publishing messages to a partitioned topic, each message is routed to one of several brokers. The distribution of partitions across brokers is handled automatically by Pulsar.

The diagram below illustrates this:

![Partitioned Topic](/img/pulsar_partitioned_topic.jpg)

Here, the topic **T1** has five partitions (**P0** through **P4**) split across three brokers. Because there are more partitions than brokers, two brokers handle two partitions a piece, while the third handles only one (again, Pulsar handles this distribution of partitions automatically).

Messages for this topic are broadcast to two {% popover consumers %}. The [routing mode](#routing-modes) determines both which broker handles each partition, while the [subscription mode](../../getting-started/ConceptsAndArchitecture#subscription-modes) determines which messages go to which consumers.

Decisions about routing and subscription modes can be made separately in most cases. Throughput concerns should guide partitioning/routing decisions while subscription decisions should be guided by application semantics.

There is no difference between partitioned topics and normal topics in terms of how subscription modes work, as partitioning only determines what happens between when a message is published by a {% popover producer %} and processed and {% popover acknowledged %} by a {% popover consumer %}.

Partitioned topics need to be explicitly created via the [admin API](../../admin/AdminInterface). The number of partitions can be specified when creating the topic.

#### Routing modes

When publishing to partitioned topics, you must specify a *routing mode*. The routing mode determines which partition---that is, which internal topic---each message should be published to.

There are three routing modes available by default:

Mode | Description | Ordering guarantee
:----|:------------|:------------------
Key hash | If a key property has been specified on the message, the partitioned producer will hash the key and assign it to a particular partition. | Per-key-bucket ordering
Single default partition | If no key is provided, each producer's message will be routed to a dedicated partition, initially random selected | Per-producer ordering
Round robin distribution | If no key is provided, all messages will be routed to different partitions in round-robin fashion to achieve maximum throughput. | None

In addition to these default modes, you can also create a custom routing mode if you're using the [Java client](../../clients/Java) by implementing the {% javadoc MessageRouter client com.yahoo.pulsar.client.api.MessageRouter %} interface.

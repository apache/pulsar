Normal topics can be served only by a single {% popover broker %}, which limits the topic's maximum throughput. *Partitioned topics* are a special type of topic that can span multiple brokers and thus allow for much higher throughput.

Behind the scenes, a partitioned topic is actually implemented as N internal topics, where N is the number of partitions. There is no difference between partitioned topics and other normal topics in terms of how subscription modes work, as partitioning only determines what happens between when a message is published by a {% popover producer %} and processed and {% popover acknowledged %} by a {% popover consumer %}.

Partitioned topics need to be explicitly created via the [admin API](../../admin/AdminInterface). The number of partitions can be specified when creating the topic.

The diagram below illustrates this:

![Partitioned Topic]({{ site.baseurl }}img/pulsar_partitioned_topic.jpg)

Here, the topic **T1** has five partitions (**P0** through **P4**) split across three brokers broadcasting to two consumers. The [routing mode](#routing-modes) determines both which broker handles each partition which consumer each message is delivered to.

#### Routing modes

With partitioned topics, the *routing mode* determines two things:

1. Which partitions---that is, which internal topic---each message should be published to
1. Which consumer the message should be ultimately delivered to


There are three routing modes available by default:

Mode | Description | Ordering guarantee
:----|:------------|:------------------
Key hash | If a key property has been specified on the message, the partitioned producer will hash the key and assign it to a particular partition. | Per-key-bucket ordering
Single default partition | If no message is provided, each producer's message will be routed to a dedicated partition, initially random selected | Per-producer ordering
Round robin distribution | If no message is provided, all messages will be routed to different partitions in round-robin fashion to achieve maximum throughput. | None

In addition to these default modes, you can also create a custom routing mode if you're using the [Java client](../../applications/JavaClient) by implementing the {% javadoc MessageRouter client com.yahoo.pulsar.client.api.MessageRouter %} interface.

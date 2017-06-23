Normal topics can be served only by a single {% popover broker %}, which limits the topic's maximum throughput. *Partitioned topics* are a special type of topic that can span multiple brokers and thus allow for much higher throughput.

Behind the scenes, a partitioned topic is actually implemented as N internal topics, where N is the number of partitions. There is no difference between partitioned topics and other normal topics in terms of how subscription modes work.

Partitioned topics need to be explicitly created via the [admin API](../../admin/AdminInterface). The number of partitions can be specified when creating the topic.

The diagram below illustrates this:

![Partitioned Topic]({{ site.baseurl }}img/pulsar_partitioned_topic.jpg)

With partitioned topics, the *routing mode* determines which partition---that is, which internal topic---a message will be published to. There are three routing modes available by default:

Mode | Description | Ordering guarantee
:----|:------------|:------------------
Key hash | If a key property has been specified on the message, the partitioned producer will hash the key and assign it to a particular partition. | Per-key-bucket ordering
Single default partition | If no message is provided, each producer's message will be routed to a dedicated partition, initially random selected | Per-producer ordering
Round robin distribution | If no message is provided, all messages will be routed to different partitions in round-robin fashion to achieve maximum throughput. | None

In addition to these default modes, you can also create a custom routing mode if you're using the [Java client](../../applications/JavaClient) by implementing the {% javadoc MessageRouter client com.yahoo.pulsar.client.api.MessageRouter %} interface.

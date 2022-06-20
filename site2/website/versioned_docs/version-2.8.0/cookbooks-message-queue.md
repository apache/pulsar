---
id: cookbooks-message-queue
title: Using Pulsar as a message queue
sidebar_label: "Message queue"
original_id: cookbooks-message-queue
---

Message queues are essential components of many large-scale data architectures. If every single work object that passes through your system absolutely *must* be processed in spite of the slowness or downright failure of this or that system component, there's a good chance that you'll need a message queue to step in and ensure that unprocessed data is retained---with correct ordering---until the required actions are taken.

Pulsar is a great choice for a message queue because:

* it was built with [persistent message storage](concepts-architecture-overview.md#persistent-storage) in mind
* it offers automatic load balancing across [consumers](reference-terminology.md#consumer) for messages on a topic (or custom load balancing if you wish)

> You can use the same Pulsar installation to act as a real-time message bus and as a message queue if you wish (or just one or the other). You can set aside some topics for real-time purposes and other topics for message queue purposes (or use specific namespaces for either purpose if you wish).


# Client configuration changes

To use a Pulsar [topic](reference-terminology.md#topic) as a message queue, you should distribute the receiver load on that topic across several consumers (the optimal number of consumers will depend on the load). Each consumer must:

* Establish a [shared subscription](concepts-messaging.md#shared) and use the same subscription name as the other consumers (otherwise the subscription is not shared and the consumers can't act as a processing ensemble)
* If you'd like to have tight control over message dispatching across consumers, set the consumers' **receiver queue** size very low (potentially even to 0 if necessary). Each Pulsar [consumer](reference-terminology.md#consumer) has a receiver queue that determines how many messages the consumer will attempt to fetch at a time. A receiver queue of 1000 (the default), for example, means that the consumer will attempt to process 1000 messages from the topic's backlog upon connection. Setting the receiver queue to zero essentially means ensuring that each consumer is only doing one thing at a time.

   The downside to restricting the receiver queue size of consumers is that that limits the potential throughput of those consumers and cannot be used with [partitioned topics](reference-terminology.md#partitioned-topic). Whether the performance/control trade-off is worthwhile will depend on your use case.

## Java clients

Here's an example Java consumer configuration that uses a shared subscription:

```java

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;

String SERVICE_URL = "pulsar://localhost:6650";
String TOPIC = "persistent://public/default/mq-topic-1";
String subscription = "sub-1";

PulsarClient client = PulsarClient.builder()
        .serviceUrl(SERVICE_URL)
        .build();

Consumer consumer = client.newConsumer()
        .topic(TOPIC)
        .subscriptionName(subscription)
        .subscriptionType(SubscriptionType.Shared)
        // If you'd like to restrict the receiver queue size
        .receiverQueueSize(10)
        .subscribe();

```

## Python clients

Here's an example Python consumer configuration that uses a shared subscription:

```python

from pulsar import Client, ConsumerType

SERVICE_URL = "pulsar://localhost:6650"
TOPIC = "persistent://public/default/mq-topic-1"
SUBSCRIPTION = "sub-1"

client = Client(SERVICE_URL)
consumer = client.subscribe(
    TOPIC,
    SUBSCRIPTION,
    # If you'd like to restrict the receiver queue size
    receiver_queue_size=10,
    consumer_type=ConsumerType.Shared)

```

## C++ clients

Here's an example C++ consumer configuration that uses a shared subscription:

```cpp

#include <pulsar/Client.h>

std::string serviceUrl = "pulsar://localhost:6650";
std::string topic = "persistent://public/defaultmq-topic-1";
std::string subscription = "sub-1";

Client client(serviceUrl);

ConsumerConfiguration consumerConfig;
consumerConfig.setConsumerType(ConsumerType.ConsumerShared);
// If you'd like to restrict the receiver queue size
consumerConfig.setReceiverQueueSize(10);

Consumer consumer;

Result result = client.subscribe(topic, subscription, consumerConfig, consumer);

```

## Go clients

Here is an example of a Go consumer configuration that uses a shared subscription:

```go

import "github.com/apache/pulsar-client-go/pulsar"

client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})
if err != nil {
    log.Fatal(err)
}
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:             "persistent://public/default/mq-topic-1",
    SubscriptionName:  "sub-1",
    Type:              pulsar.Shared,
    ReceiverQueueSize: 10, // If you'd like to restrict the receiver queue size
})
if err != nil {
    log.Fatal(err)
}

```


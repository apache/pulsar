---
title: Using Pulsar as a message queue
lead: Although Pulsar is typically known as a real-time messaging system, it's also an excellent choice for a queuing system
tags: [clients, java, python, message queue, cookbook]
---

Message queues are essential components of many large-scale data architectures. If every single work object that passes through your system absolutely *must* be processed in spite of the slowness or downright failure of this or that system component, there's a good chance that you'll need a message queue to step in and ensure that unprocessed data is retained---with correct ordering---until the required actions are taken.

Pulsar is a great choice for a message queue because:

* it was built with [persistent message storage](../../getting-started/ConceptsAndArchitecture#persistent-storage) in mind
* it offers automatic load balancing across {% popover consumers %} for messages on a topic (or custom load balancing if you wish)

{% include admonition.html type="success" content="You can use the same Pulsar installation to act as a real-time message bus and as a message queue if you wish (or just one or the other). You can set aside some topics for real-time purposes and other topics for message queue purposes (or use specific namespaces for either purpose if you wish)." %}

## Client configuration changes

To use a Pulsar {% popover topic %} as a message queue, you should distribute the receiver load on that topic across several {% popover consumers %}. Each consumer must:

* Have a [shared subscription](../../getting-started/ConceptsAndArchitecture#shared) and use the same subscription name as the other consumers (otherwise the subscription is not shared)
* Have the receiver queue size set to zero (or very low). Each Pulsar {% popover consumer %} has a **receiver queue** that determines how many messages the consumer will attempt to fetch at a time. A receiver queue of 1000 (the default), for example, means that the consumer will attempt to process 1000 messages from the topic's backlog upon connection. Setting the receiver queue to zero essentially means ensuring that each consumer is only doing one thing at a time.

{% include admonition.html type="info" content="The default receiver queue size is 1000." %}

### Java clients {#java}

Here's an example Java consumer configuration that sets the receiver queue size to zero and uses a shared subscription:

```java
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;

String SERVICE_URL = "pulsar://localhost:6650";
String TOPIC = "persistent://sample/standalone/ns1/mq-topic-1";
String subscription = "sub-1";

PulsarClient client = PulsarClient.builder()
        .serviceUrl(SERVICE_URL)
        .build();

Consumer consumer = client.newConsumer()
        .topic(TOPIC)
        .subscriptionName(subscription)
        .subscriptionType(SubscriptionType.Shared)
        .receiverQueueSize(0)
        .subscribe();
```

### Python clients {#python}

Here's an example Python consumer configuration that sets the receiver queue size to zero and uses a shared subscription:

```python
from pulsar import Client, ConsumerType

SERVICE_URL = "pulsar://localhost:6650"
TOPIC = "persistent://sample/standalone/ns1/mq-topic-1"
SUBSCRIPTION = "sub-1"

client = Client(SERVICE_URL)
consumer = client.subscribe(
    TOPIC,
    SUBSCRIPTION,
    receiver_queue_size=0,
    consumer_type=ConsumerType.Shared)
```

### C++ clients {#cpp}

Here's an example C++ consumer configuration that sets the receiver queue size to zero and uses a shared subscription:

```cpp
#include <pulsar/Client.h>

std::string serviceUrl = "pulsar://localhost:6650";
std::string topic = "persistent://sample/standalone/ns1/mq-topic-1";
std::string subscription = "sub-1";

Client client(serviceUrl);

ConsumerConfiguration consumerConfig;
consumerConfig.setReceiverQueueSize(0);
consumerConfig.setConsumerType(ConsumerType.ConsumerShared)

Consumer consumer;

Result result = client.subscribe("persistent://sample/standalone/ns1/my-topic", consumerConfig, consumer);
```
---
title: Using Pulsar as a message queue
lead: Although Pulsar is typically known as a real-time messaging system, it's also an excellent choice for a queuing system
tags: [clients, java, python, message queue, cookbook]
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

Message queues are essential components of many large-scale data architectures. If every single work object that passes through your system absolutely *must* be processed in spite of the slowness or downright failure of this or that system component, there's a good chance that you'll need a message queue to step in and ensure that unprocessed data is retained---with correct ordering---until the required actions are taken.

Pulsar is a great choice for a message queue because:

* it was built with [persistent message storage](../../getting-started/ConceptsAndArchitecture#persistent-storage) in mind
* it offers automatic load balancing across {% popover consumers %} for messages on a topic (or custom load balancing if you wish)

{% include admonition.html type="success" content="You can use the same Pulsar installation to act as a real-time message bus and as a message queue if you wish (or just one or the other). You can set aside some topics for real-time purposes and other topics for message queue purposes (or use specific namespaces for either purpose if you wish)." %}

## Client configuration changes

To use a Pulsar {% popover topic %} as a message queue, you should distribute the receiver load on that topic across several {% popover consumers %} (the optimal number of consumers will depend on the load). Each consumer must:

* Establish a [shared subscription](../../getting-started/ConceptsAndArchitecture#shared) and use the same subscription name as the other consumers (otherwise the subscription is not shared and the consumers can't act as a processing ensemble)
* If you'd like to have tight control over message dispatching across consumers, set the consumers' **receiver queue** size very low (potentially even to 0 if necessary). Each Pulsar {% popover consumer %} has a receiver queue that determines how many messages the consumer will attempt to fetch at a time. A receiver queue of 1000 (the default), for example, means that the consumer will attempt to process 1000 messages from the topic's backlog upon connection. Setting the receiver queue to zero essentially means ensuring that each consumer is only doing one thing at a time.

   The downside to restricting the receiver queue size of consumers is that that limits the potential throughput of those consumers and cannot be used with {% popover partitioned topics %}. Whether the performance/control trade-off is worthwhile will depend on your use case.

### Java clients {#java}

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

### Python clients {#python}

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

### C++ clients {#cpp}

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

Result result = client.subscribe("persistent://public/default/my-topic", subscription, consumerConfig, consumer);
```
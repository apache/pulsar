---
title: Message deduplication
tags: [admin, deduplication, cookbook]
---

**Message deduplication** is a feature of Pulsar that, when enabled, ensures that each message produced on Pulsar {% popover topics %} is persisted to disk *only once*, even if the message is produced more than once. Message deduplication essentially unburdens Pulsar applications of the responsibility of ensuring deduplication and handles it automatically on the server side instead.

Using message deduplication in Pulsar involves making some [configuration changes](#configuration) to your Pulsar brokers as well as some minor changes to the behavior of Pulsar [clients](#clients).

{% include admonition.html type="info" content="For a more thorough theoretical explanation of message deduplication, see the [Concepts and Architecture](../../getting-started/ConceptsAndArchitecture#message-deduplication) document." %}

## Configuration for message deduplication {#configuration}

You can configure message deduplication in Pulsar using the [`broker.conf`](../../reference/Configuration#broker) configuration file. The following deduplication-related parameters are available:

Parameter | Description | Default
:---------|:------------|:-------
`brokerDeduplicationEnabled` | Sets the default behavior for message deduplication in the broker. If enabled, the broker will reject messages that were already stored in the topic. This setting can be overridden on a per-namespace basis. | `false`
`brokerDeduplicationMaxNumberOfProducers` | The maximum number of producers for which information will be stored for deduplication purposes. | `10000`
`brokerDeduplicationEntriesInterval` | The number of entries after which a deduplication informational snapshot is taken. A larger interval will lead to fewer snapshots being taken, though this would also lengthen the topic recovery time (the time required for entries published after the snapshot to be replayed). | `1000`
`brokerDeduplicationProducerInactivityTimeoutMinutes` | The time of inactivity (in minutes) after which the broker will discard deduplication information related to a disconnected producer. | `360` (6 hours)

### Setting the broker-level default {#default}

To enable message deduplication in a broker, set the `brokerDeduplicationEnabled` parameter to `true` and re-start the broker.

### Enabling message deduplication {#enabling}

You can enable message deduplication on specific namespaces, regardless of the the [default](#default) for the broker, using the 

```bash
$ bin/pulsar-admin
```

### Disabling message deduplication {#disabling}

If message deduplication is [enabled](#enabling) on a Pulsar broker, all {% popover namespaces %} will have deduplication enabled by default. You can override this, however, on a per-namespace basis. This

[`set-deduplication`](../../CliTools#pulsar-admin-namespaces-set-deduplication)

## Message deduplication and Pulsar clients {#clients}

If you enable message deduplication in your Pulsar {% popover brokers %}, you won't need to make any major changes to your Pulsar clients. There are, however, two settings that you need to provide for your client {% popover producers %}:

1. The producer must be given a name
1. The message send timeout needs to be set to infinity (i.e. no timeout)

Instructions for [Java](#java), [Python](#python), and [C++](#cpp) clients can be found below.

### Java clients {#java}

To enable message deduplication on a [Java producer](../../clients/Java#producers), set the producer name using the `producerName` setter and set the timeout to 0 using the `sendTimeout` setter. Here's an example:

```java
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import java.util.concurrent.TimeUnit;

PulsarClient pulsarClient = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
Producer producer = pulsarClient.newProducer()
        .producerName("producer-1")
        .topic("persistent://sample/standalone/ns1/topic-1")
        .sendTimeout(0, TimeUnit.SECONDS)
        .create();
```

### Python clients {#python}

To enable message deduplication on a [Python producer](../../clients/Python#producers), set the producer name using `producer_name` and the timeout to 0 using `send_timeout_millis`. Here's an example:

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")
producer = client.create_producer(
    "persistent://sample/standalone/ns1/topic-1",
    producer_name="producer-1",
    send_timeout_millis=0)
```

## C++ clients (#cpp)

To enable message deduplication on a [C++ producer](../../clients/Cpp#producer), set the producer name using `producer_name` and the timeout to 0 using `send_timeout_millis`. Here's an example:

```cpp
#include <pulsar/Client.h>

std::string serviceUrl = "pulsar://localhost:6650";
std::string topic = "persistent://prop/unit/ns1/topic-1";
std::string producerName = "producer-1";

Client client(serviceUrl);

ProducerConfiguration producerConfig;
producerConfig.setSendTimeout(0);
producerConfig.setProducerName(producerName);

Producer producer;

Result result = client.createProducer("persistent://sample/standalone/ns1/my-topic", producerConfig, producer);
```
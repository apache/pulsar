---
id: version-2.1.0-incubating-cookbooks-deduplication
title: Message deduplication
sidebar_label: Message deduplication
original_id: cookbooks-deduplication
---

**Message deduplication** is a feature of Pulsar that, when enabled, ensures that each message produced on Pulsar topics is persisted to disk *only once*, even if the message is produced more than once. Message deduplication essentially unburdens Pulsar applications of the responsibility of ensuring deduplication and instead handles it automatically on the server side.

Using message deduplication in Pulsar involves making some [configuration changes](#configuration) to your Pulsar brokers as well as some minor changes to the behavior of Pulsar [clients](#clients).

> For a more thorough theoretical explanation of message deduplication, see the [Concepts and Architecture](concepts-messaging.md#message-deduplication) document.


## How it works

Message deduplication can be enabled and disabled on a per-namespace basis. By default, it is *disabled* on all namespaces and can enabled in the following ways:

* Using the [`pulsar-admin namespaces`](#enabling) interface
* As a broker-level [default](#default) for all namespaces

## Configuration for message deduplication

You can configure message deduplication in Pulsar using the [`broker.conf`](reference-configuration.md#broker) configuration file. The following deduplication-related parameters are available:

Parameter | Description | Default
:---------|:------------|:-------
`brokerDeduplicationEnabled` | Sets the default behavior for message deduplication in the Pulsar [broker](reference-terminology.md#broker). If set to `true`, message deduplication will be enabled by default on all namespaces; if set to `false` (the default), deduplication will have to be [enabled](#enabling) and [disabled](#disabling) on a per-namespace basis. | `false`
`brokerDeduplicationMaxNumberOfProducers` | The maximum number of producers for which information will be stored for deduplication purposes. | `10000`
`brokerDeduplicationEntriesInterval` | The number of entries after which a deduplication informational snapshot is taken. A larger interval will lead to fewer snapshots being taken, though this would also lengthen the topic recovery time (the time required for entries published after the snapshot to be replayed). | `1000`
`brokerDeduplicationProducerInactivityTimeoutMinutes` | The time of inactivity (in minutes) after which the broker will discard deduplication information related to a disconnected producer. | `360` (6 hours)

### Setting the broker-level default {#default}

By default, message deduplication is *disabled* on all Pulsar namespaces. To enable it by default on all namespaces, set the `brokerDeduplicationEnabled` parameter to `true` and re-start the broker.

Regardless of the value of `brokerDeduplicationEnabled`, [enabling](#enabling) and [disabling](#disabling) via the CLI will override the broker-level default.

### Enabling message deduplication {#enabling}

You can enable message deduplication on specific namespaces, regardless of the the [default](#default) for the broker, using the [`pulsar-admin namespace set-deduplication`](reference-pulsar-admin.md#namespace-set-deduplication) command. You can use the `--enable`/`-e` flag and specify the namespace. Here's an example with <tenant>/<namespace>:

```bash
$ bin/pulsar-admin namespaces set-deduplication \
  public/default \
  --enable # or just -e
```

### Disabling message deduplication {#disabling}

You can disable message deduplication on a specific namespace using the same method shown [above](#enabling), except using the `--disable`/`-d` flag instead. Here's an example with <tenant>/<namespace>:

```bash
$ bin/pulsar-admin namespaces set-deduplication \
  public/default \
  --disable # or just -d
```

## Message deduplication and Pulsar clients {#clients}

If you enable message deduplication in your Pulsar brokers, you won't need to make any major changes to your Pulsar clients. There are, however, two settings that you need to provide for your client producers:

1. The producer must be given a name
1. The message send timeout needs to be set to infinity (i.e. no timeout)

Instructions for [Java](#java), [Python](#python), and [C++](#cpp) clients can be found below.

### Java clients {#java}

To enable message deduplication on a [Java producer](client-libraries-java.md#producers), set the producer name using the `producerName` setter and set the timeout to 0 using the `sendTimeout` setter. Here's an example:

```java
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import java.util.concurrent.TimeUnit;

PulsarClient pulsarClient = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
Producer producer = pulsarClient.newProducer()
        .producerName("producer-1")
        .topic("persistent://public/default/topic-1")
        .sendTimeout(0, TimeUnit.SECONDS)
        .create();
```

### Python clients {#python}

To enable message deduplication on a [Python producer](client-libraries-python.md#producers), set the producer name using `producer_name` and the timeout to 0 using `send_timeout_millis`. Here's an example:

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")
producer = client.create_producer(
    "persistent://public/default/topic-1",
    producer_name="producer-1",
    send_timeout_millis=0)
```

### C++ clients {#cpp}

To enable message deduplication on a [C++ producer](client-libraries-cpp.md#producer), set the producer name using `producer_name` and the timeout to 0 using `send_timeout_millis`. Here's an example:

```cpp
#include <pulsar/Client.h>

std::string serviceUrl = "pulsar://localhost:6650";
std::string topic = "persistent://some-tenant/ns1/topic-1";
std::string producerName = "producer-1";

Client client(serviceUrl);

ProducerConfiguration producerConfig;
producerConfig.setSendTimeout(0);
producerConfig.setProducerName(producerName);

Producer producer;

Result result = client.createProducer(topic, producerConfig, producer);
```


---
id: version-2.8.2-cookbooks-deduplication
title: Message deduplication
sidebar_label: Message deduplication
original_id: cookbooks-deduplication
---

When **Message deduplication** is enabled, it ensures that each message produced on Pulsar topics is persisted to disk *only once*, even if the message is produced more than once. Message deduplication is handled automatically on the server side. 

To use message deduplication in Pulsar, you need to configure your Pulsar brokers and clients.

## How it works

You can enable or disable message deduplication at the namespace level or the topic level. By default, it is disabled on all namespaces or topics. You can enable it in the following ways:

* Enable deduplication for all namespaces/topics at the broker-level.
* Enable deduplication for a specific namespace with the `pulsar-admin namespaces` interface.
* Enable deduplication for a specific topic with the `pulsar-admin topics` interface.

## Configure message deduplication

You can configure message deduplication in Pulsar using the [`broker.conf`](reference-configuration.md#broker) configuration file. The following deduplication-related parameters are available.

Parameter | Description | Default
:---------|:------------|:-------
`brokerDeduplicationEnabled` | Sets the default behavior for message deduplication in the Pulsar broker. If it is set to `true`, message deduplication is enabled on all namespaces/topics. If it is set to `false`, you have to enable or disable deduplication at the namespace level or the topic level. | `false`
`brokerDeduplicationMaxNumberOfProducers` | The maximum number of producers for which information is stored for deduplication purposes. | `10000`
`brokerDeduplicationEntriesInterval` | The number of entries after which a deduplication informational snapshot is taken. A larger interval leads to fewer snapshots being taken, though this lengthens the topic recovery time (the time required for entries published after the snapshot to be replayed). | `1000`
`brokerDeduplicationProducerInactivityTimeoutMinutes` | The time of inactivity (in minutes) after which the broker discards deduplication information related to a disconnected producer. | `360` (6 hours)

### Set default value at the broker-level

By default, message deduplication is *disabled* on all Pulsar namespaces/topics. To enable it on all namespaces/topics, set the `brokerDeduplicationEnabled` parameter to `true` and re-start the broker.

Even if you set the value for `brokerDeduplicationEnabled`, enabling or disabling via Pulsar admin CLI overrides the default settings at the broker-level.

### Enable message deduplication

Though message deduplication is disabled by default at the broker level, you can enable message deduplication for a specific namespace or topic using the [`pulsar-admin namespaces set-deduplication`](reference-pulsar-admin.md#namespace-set-deduplication) or the [`pulsar-admin topics set-deduplication`](reference-pulsar-admin.md#topic-set-deduplication) command. You can use the `--enable`/`-e` flag and specify the namespace/topic. 

The following example shows how to enable message deduplication at the namespace level.

```bash
$ bin/pulsar-admin namespaces set-deduplication \
  public/default \
  --enable # or just -e
```

### Disable message deduplication

Even if you enable message deduplication at the broker level, you can disable message deduplication for a specific namespace or topic using the [`pulsar-admin namespace set-deduplication`](reference-pulsar-admin.md#namespace-set-deduplication) or the [`pulsar-admin topics set-deduplication`](reference-pulsar-admin.md#topic-set-deduplication) command. Use the `--disable`/`-d` flag and specify the namespace/topic.

The following example shows how to disable message deduplication at the namespace level.

```bash
$ bin/pulsar-admin namespaces set-deduplication \
  public/default \
  --disable # or just -d
```

## Pulsar clients

If you enable message deduplication in Pulsar brokers, you need complete the following tasks for your client producers:

1. Specify a name for the producer.
1. Set the message timeout to `0` (namely, no timeout).

The instructions for Java, Python, and C++ clients are different.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java clients-->

To enable message deduplication on a [Java producer](client-libraries-java.md#producers), set the producer name using the `producerName` setter, and set the timeout to `0` using the `sendTimeout` setter. 

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

<!--Python clients-->

To enable message deduplication on a [Python producer](client-libraries-python.md#producers), set the producer name using `producer_name`, and set the timeout to `0` using `send_timeout_millis`. 

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")
producer = client.create_producer(
    "persistent://public/default/topic-1",
    producer_name="producer-1",
    send_timeout_millis=0)
```
<!--C++ clients-->

To enable message deduplication on a [C++ producer](client-libraries-cpp.md#producer), set the producer name using `producer_name`, and set the timeout to `0` using `send_timeout_millis`. 

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
<!--END_DOCUSAURUS_CODE_TABS-->
---
id: version-2.7.2-concepts-multiple-advertised-listeners
title: Multiple advertised listeners
sidebar_label: Multiple advertised listeners
original_id: concepts-multiple-advertised-listeners
---

When a Pulsar cluster is deployed in the production environment, it may require to expose multiple advertised addresses for the broker. For example, when you deploy a Pulsar cluster in Kubernetes and want other clients, which are not in the same Kubernetes cluster, to connect to the Pulsar cluster, you need to assign a broker URL to external clients. But clients in the same Kubernetes cluster can still connect to the Pulsar cluster through the internal network of Kubernetes.

## Advertised listeners

To ensure clients in both internal and external networks can connect to a Pulsar cluster, Pulsar introduces `advertisedListeners` and `internalListenerName` configuration options into the [broker configuration file](reference-configuration.md#broker) to ensure that the broker supports exposing multiple advertised listeners and support the separation of internal and external network traffic.

- The `advertisedListeners` is used to specify multiple advertised listeners. The broker uses the listener as the broker identifier in the load manager and the bundle owner data. The `advertisedListeners` is formatted as `<listener_name>:pulsar://<host>:<port>, <listener_name>:pulsar+ssl://<host>:<port>`. You can set up the `advertisedListeners` like
`advertisedListeners=internal:pulsar://192.168.1.11:6660,internal:pulsar+ssl://192.168.1.11:6651`.

- The `internalListenerName` is used to specify the internal service URL that the broker uses. You can specify the `internalListenerName` by choosing one of the `advertisedListeners`. The broker uses the listener name of the first advertised listener as the `internalListenerName` if the `internalListenerName` is absent.

After setting up the `advertisedListeners`, clients can choose one of the listeners as the service URL to create a connection to the broker as long as the network is accessible. However, if the client creates producers or consumer on a topic, the client must send a lookup requests to the broker for getting the owner broker, then connect to the owner broker to publish messages or consume messages. Therefore, You must allow the client to get the corresponding service URL with the same advertised listener name as the one used by the client. This helps keep client-side simple and secure.

## Use multiple advertised listeners

This example shows how a Pulsar client uses multiple advertised listeners.

1. Configure multiple advertised listeners in the broker configuration file.

```shell
advertisedListeners={listenerName}:pulsar://xxxx:6650,
{listenerName}:pulsar+ssl://xxxx:6651
```

2. Specify the listener name for the client.

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://xxxx:6650")
    .listenerName("external")
    .build();
```

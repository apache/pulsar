# PIP-61: Advertised multiple addresses

* Current State: Under Discussion
* Author: Shunli Gao, Penghui Li, Sijie Guo, Jia Zhai
* Pull request: 
* Mailing List discussion: 
* Release: 2.6.0

### Motivation
Currently, the broker supports only one advertised address which is configured in the broker configuration file(broker.conf). But when deployed in a production Pulsar cluster, it may require to expose multiple advertised addresses for the broker. 

If you are deploying a Pulsar cluster that can be accessed through both the public network and private network, it is best that clients in the LAN can connect to the Pulsar cluster through the private network. And if clients are not in the same LAN with the Pulsar cluster, they should connect to the Pulsar cluster by the public network. This is good for both service quality and cost.

If you are deploying a Pulsar cluster in k8s and want other clients which are not in the same k8s cluster with the Pulsar cluster can connect to the Pulsar cluster, you need to assign a broker URL to external clients. But clients in the same k8s cluster with the Pulsar cluster can still connect to the Pulsar cluster through the internal network of k8s. Of course, you can also achieve this by using Pulsar Proxy.

Transactional streaming also can take advantage of multiple advertised addresses for the broker. The transaction coordinator creates connections to the broker(transaction buffer) through the internal service URL and the client connects to the broker through the external service URL.
So this proposal is to support the use cases similar to the above. In this proposal, the main purpose is to let the broker support expose multiple advertised listeners and support the separation of internal and external network traffic.

### Approach
This approach introduces two new configurations `advertisedListeners` and `internalListenerName` in the broker.conf. The `advertisedListeners` is used to specify multiple advertised listeners and the `internalListenerName` is used to specify the internal service URL that the broker uses. This will discuss why we need `advertisedListeners` for the broker later.

The `advertisedListeners` is formatted as `<listener_name>:pulsar://<host>:<port>, <listener_name>:pulsar+ssl://<host>:<port>`. Currently, `advertisedAddress` and `brokerServicePort` to specify the service URL that expose to the client. User can’t setup both `advertisedAddress` and `advertisedListeners` at the same time, can’t set duplicate listener name and can’t assign same `host:port` to different listeners. Users can set up the `advertisedListeners` like following example:

```
advertisedListeners=internal:pulsar://192.168.1.11:6660,internal:pulsar+ssl://192.168.1.11:6651
```

After setting up the `advertisedListeners`, clients can choose one of the listeners as the service URL to create a connection to the broker as long as the network is accessible. But if creates producers or consumer on a topic by the client, the client must send a lookup requests to the broker for getting the owner broker, then connect to the owner broker to publish messages or consume messages. Therefore, we must allow the client to get the corresponding service URL with the same advertised listener name as the client uses.  We select an approach of only returning the corresponding service URL.

**1. Only return the corresponding service URL**

In the approach, when the client sends a lookup request to the broker, the broker only returns one service URL that with the same listener name with the client uses. The purpose of this approach is keeping client-side simple and not expose extra listeners to the client, this is better for security. 

**2. Return all advertised listeners(rejected)**

In the approach, the broker returns all owner broker listeners to the client. When the client gets the response of the lookup request, the client needs to select the right one to connect to the owner broker. Same as `brokerServiceUrl` that we handled currently.

The `internalListenerName` is used by the broker internal. The broker uses the listener as the broker identifier in the load manager that the listener with the same name as `internalListenerName`. Currently, the load manager uses `advertisedAddress` and `brokerServicePort` as the identifier of the broker and the bundle ownership data also uses `advertisedAddress` and `brokerServicePort` as the owner data. Since `advertisedListeners` introduced, they all need `internalListenerName` to identify a broker.

Pulsar also supports HTTP protocol lookup service, so if the user uses HTTP protocol lookup service, currently we do not concern it.

Users can specify the `internalListenerName` by choosing one of the `advertisedListeners`. The broker uses the listener name of the first advertised listener as the `internalListenerName` if the `internalListenerName` is absent. 

### Changes

1. Add two configurations in the broker.conf

```
# Used to specify multiple advertised listeners for the broker. 
# The value must format as <listener_name>:pulsar://<host>:<port>,
# multiple listeners should separate with commas.
# Do not use this configuration with advertisedAddress and brokerServicePort.
# The Default value is absent means use advertisedAddress and brokerServicePort.
advertisedListeners=

# Used to specify the internal listener name for the broker.
# The listener name must contain in the advertisedListeners. 
# The Default value is absent, the broker uses the first listener as the internal listener.
internalListenerName=
```

2. Use internal listener as the broker identifier in the load manager
3. Use internal listener as the broker identifier in the bundle owner data
4. Lookup changes

The pulsar broker returns the right listener for a given topic according to the advertised listener name which the client uses. So we need to add a new field to the CommandLookupTopic:

```   
message CommandLookupTopic {
     optional string advertised_listener_name = 7;
}
```

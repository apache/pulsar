# PIP-1: Pulsar Proxy

* **Status**: Implemented
 * **Author**: [Matteo Merli](https://github.com/merlimat)
 * **Pull Request**: [#548](https://github.com/apache/incubator-pulsar/pull/548)
 * **Mailing List discussion**: https://lists.apache.org/thread.html/66b120e0ab5a54edac23e490dcf128900605fd9216321dcc315050bf@%3Cdev.pulsar.apache.org%3E

## Motivation

Introduce a new optional component in Pulsar to expose a stateless proxy
that uses the current Pulsar binary protocol.

The pulsar-proxy can be helpful in situations where the direct connectivity
between clients and brokers is either not possible or not desirable for the
administrative overhead that might introduce.

For example, in a cloud environment and when running within a Kubernetes
cluster, is easy to connect to a broker from inside the cluster itself.

On the contrary, if a client is outside the Kubernetes cluster, it does not
have direct access the Broker addresses (which most commonly would be
tied to the Pod IP). For the client to have direct connectivity, we would
have to tie the broker to the Node IP (the VM/Host instead of the public
container IP), and then ensure that these IP are visible from outside
the cluster.

By using a proxy, all lookup and data connections are flowing through one
of the proxy instances and being the proxy stateless nature, it can be
exposed in a variety of modes (eg: VIP, DNS, Kubernetes cluster IP or
node-port and cloud-specific load balancers).

This allows for a much simple way to expose the Pulsar service and to
control the access the service.

## Design

The main design goal is to introduce a very simple proxy layer that
is stateless and that needs minimal interaction and with the client
and broker.

The client does not need to know upfront or have a specific configuration
to use the proxy. If the serviceUrl is pointing to a pulsar proxy set of nodes,
the proxy will notify the client (in the topic lookup phase) that it
needs to connect through the proxy.

The lookup response is still containing the target broker, along with a
flag instructing the client to open the connection on the original
`serviceUrl` hostname, rather than the broker.

The client is still aware about which producer/consumer is associated to
a particular broker. This allows to simplify a lot the proxy implementation.

Once a client connects to the proxy and asks to be forwarded to a
specific target broker, the proxy will follow this process:

 1. Verify the target broker is indeed a valid broker
 2. Verify authentication of the client and extract the `principal`
 3. Open a connection to the broker and perform initial handshake
    (`Connect -> Connected`), forwarding the client authentication role.
 4. Once the handshake with broker is complete, reply `Connected` to
    client
 5. Remove protobuf deserializer from both connections and
    blindly forward byte buffers from one side to the other
 6. When one connection gets closed, close also the other one


 The advantage of this model is that the proxy downgrade itself
 to a TCP proxy after the initial handshake and it doesn't have
 to deal with keeping track.

 In addition to this, the proxy needs to be able to respond to
 both lookup and partitions metadata without incurring in
 redirection to the clients.

 If the downstream broker, reply with a redirection, the proxy will
 have to handle that internally and only reply to client once it
 has the final response.

### TLS Encryption and authentication

The proxy support TLS for transport level encryption and as an
authentication plugin just like the broker are already doing.

If a client enables TLS in its configuration, all communications
between the client and the proxy will be encrypted.

The use of TLS between proxy and brokers is configured separately.
It is therefore possible to use the proxy as a TLS frontend, handling
the TLS termination and then forwarding the traffic to brokers in
clear text. Also it's possible for the proxy to use TLS and then
re-encrypt the data when talking with the server.

## Changes introduced

In addition to the new proxy component, these changes are necessary
in client and broker.

### Broker

The broker needs to validate the proxy authentication credentials
and understand what was the original `principal` role of the client.

This is done by introducing a new field on the `CommandConnect`:
```java
// Original principal that was verified by
// a Pulsar proxy. In this case the auth info above
// will the the auth of the proxy itself
optional string original_principal = 7;
```

When this field is set, the broker authorization check will do
the following:
 1. Verify the proxy belongs to *super-user* role
 2. Use the `original_principal` for the authorization check

### Client library

Client library needs to be aware that a particular connection needs
to be made through the service url hostname rather than the
particular broker serving the topic.

When doing a lookup request, there is an additional flag on the
response:

```java
// If it's true, indicates to the client that it must
// always connect through the service url after the
// lookup has been completed.
optional bool proxy_through_service_url = 8 [default = false];
```

The client will use the same existing connection pool mechanism,
and it will open multiple connections to the proxy, one (or more, if
configured so) for each destination broker.

## Rejected alternatives

A different approach would be to have an explicit proxy, configurable
in the client.

With this model, a user could just set the proxy url in the `ClientConfiguration`
object.

A similar option would be to have the transparent proxy that always
replies to lookup request by telling the client that the topic is
being served by the proxy itself.

In the first case, major changes would be required in the client library to
skip the lookup phase.

In both cases, the proxy implementation would be very complicated, because
for one incoming connection from a client, the proxy would have to
demultiplex the protobuf commands from different producers/consumers and
forward them in (possibly) separated connections to the target brokers.
Additionally, the proxy would have to deserialize all the protobuf commands.

The other concern is about the proxy keeping track of the state of the
resources created by the client. For example the status of the creation
of a producer can be difficult to mediate between the client and broker
when the client timeout is involved.

By maintaining a 1-1 matching between a `client->proxy` and a `proxy->broker`
connection, we can just degrade to TCP level proxy and avoid all these
issues.

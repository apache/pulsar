# PIP-95: Smart Listener Selection with Multiple Bind Addresses

* * **Author**: Eron Wright
* **Pull Request**: 
* **Mailing List discussion**: ([discussion](https://mail-archives.apache.org/mod_mbox/pulsar-dev/202109.mbox/%3CCAGkx0%3DRxNB6p4szhwELZ5EMt1O9_6KjBjYfsnzd-4E0%2BVcH%2BEQ%40mail.gmail.com%3E)) ([vote](https://mail-archives.apache.org/mod_mbox/pulsar-dev/202109.mbox/%3cCAGkx0=TLCiWjLsgJh=NhCEEh68cKS=WtBbd_K3a_q1-N3b2dkQ@mail.gmail.com%3e))
* **Release**: 

### Motivation

The Pulsar broker has the concept of advertised listeners representing broker endpoints that are discoverable by, and accessible to, Pulsar clients.  In addition, one listener may be designated as the _internal_ listener, to be used for broker-to-broker communication.  Listeners may be understood as named routes to the broker.

Each broker stores its listener data into ZooKeeper to support topic lookup requests.  A lookup request may contain a listener name parameter to obtain a specific endpoint.  For example, a lookup request for the listener named `external` may return an endpoint of `pulsar+ssl://broker-1.cluster.example.dev:6651` (an external address). If a listener name is not specified, the response contains the endpoint for the internal listener.

This PIP seeks to improve the experience when a listener name is not specified, to select an appropriate endpoint automatically without requiring explicit configuration on the client.  The proposed approach is to use information that the client naturally has, that is the Pulsar service endpoint.  The broker shall select a listener based on which service endpoint was used to make the lookup request.  For example, a client that makes a lookup request via an ingress gateway would use that same gateway for the subsequent broker connection. 

A secondary goal is to improve the interoperability of KOP by implementing smart endpoint selection for ordinary Kafka clients.  This shall be based on the semantics of [KIP-103](https://cwiki.apache.org/confluence/display/KAFKA/KIP-103%3A+Separation+of+Internal+and+External+traffic) and is consistent with this proposal.  Similarly for other protocol handlers.

### Public Interfaces

#### Configuration: Bind Addresses
It is proposed that a new configuration property be defined to specify a map of listener name to server bind address.  The broker shall bind a server socket for each address, and use the associated listener name (by default) for any lookup request that comes through that address.  The property shall be named `bindAddresses` and have the form:

```
<listener_name>:<scheme>://<host>:<port>,[...]
```

Where `listener_name` is a configured listener via the `advertisedListeners` configuration property.

Where `scheme` selects a protocol handler, e.g. `pulsar` or `pulsar+ssl` for the Pulsar protocol.

Where `host` has a local interface address to bind to, such as `0.0.0.0` to bind to all interfaces.

Where `port` is a unique server port number, such as `6652`.

See the compatibility section for further information. 

#### Admin API: Lookup Request: Listener Name Header
It is proposed that a new header-based parameter be added to the Topic Lookup operation on the Admin API (`/v2/topic/{topic-domain}/{tenant}/{namespace}/{topic}`).  The header name shall be `X-Pulsar-ListenerName` and the value shall correspond to the name of a configured listener.

The operation shall return `400 Bad Request` if the listener name doesn't match the configuration.

### Proposed Changes

The main technical challenge is to know which advertised listener a lookup request came from, and the proposed solution is two-fold:
1. for the Pulsar binary protocol, use _a unique bind address_ for each listener.
2. for the Pulsar HTTP-based admin protocol, use an HTTP header to specify a listener name.

#### Multiple Bind Addresses
The broker shall be configurable to open a server socket on numerous bind addresses, and to associate each socket with a listener name.  When a lookup request arrives on a particular socket, the broker shall use the associated listener by default.  Note that an explicit listener name in the request parameters would take precedence.

For example, port `6652` may be associated with the `external` listener to establish the default response for a lookup request received on that port.

The scheme information shall be used to determine whether to terminate TLS on the server socket.  Note that all TLS sockets would use the same TLS configuration (a per-address configuration is out-of-scope).

#### Listener Name Header
The broker shall look for the `X-Pulsar-ListenerName` header as an additional parameter to the Topic Lookup operation.  The value shall be used when a listener name is not supplied via the similar `listenerName` query parameter.

The rationale for supporting a header-based alternative to the query parameter is to allow an HTTP-aware gateway to inject the header value as a hint for requests that comes thru the gateway.  Istio, for example, is able to inject headers but is not able to set query parameters ([docs](https://istio.io/latest/docs/reference/config/networking/virtual-service/#Headers)).

#### Relax Validation of Listener Addresses
The validation logic for advertised listeners shall be relaxed to allow certain configurations that are currently rejected.  The root issue is that the broker erroneously assumes that TLS is always terminated at the broker, as opposed to at the gateway.  This precludes a configuration where a gateway terminates TLS and then forwards the request as plaintext.

#### TLS-Only Configurations
The validation and lookup logic shall be updated to allow for pure TLS configurations, that is where an advertised listener consists only of a TLS endpoint (no plaintext endpoint).  Currently, the broker insists that all listeners have a plaintext endpoint.  This makes little sense, e.g. for an Internet-facing gateway endpoint where TLS is a hard requirement.

The behavior of the Pulsar client is to use the same scheme for the service endpoint and for the broker endpoint.  For example, if a client uses `pulsar+ssl` to perform a lookup request, it then uses `pulsar+ssl` to connect to the broker to produce messages.  No change is anticipated.

### Compatibility, Deprecation, and Migration Plan

For compatibility, the primary bind addresses shall not be associated with any particular listener.  The behavior of lookup requests to those addresses shall be unchanged.  The primary bind addresses are configured using `bindAddress`, `brokerServicePort`, and `brokerServicePortTls` as normal.

It is assumed that any bind addresses that are configured with `bindAddresses` are in addition to the primary bind addresses and do not conflict.  If one wishes to purely use `bindAddresses`, one must _unset_ the `brokerServicePort` property (note that it has a default value of `6650`).

### Test Plan

#### Unit Tests
New unit tests will test the parsing and validation logic for the `bindAddresses` configuration property.

#### Adhoc Tests
Pulsar will be tested on an adhoc basis in various combinations, primarily using Pulsar standalone mode.  Specific cases:

- migration scenarios involving `bindAddress`, `brokerServicePort`, `brokerServicePortTls`, `webServicePort`, and `webServicePortTls`.
- TLS and/or non-TLS channels
- end-to-end testing with multiple listeners

### Rejected Alternatives
None.

### Diagrams

![image](https://user-images.githubusercontent.com/1775518/138172487-cd0f374e-2590-481a-b76d-d6007de66a9f.png)

![image](https://user-images.githubusercontent.com/1775518/138172615-2c7bd433-c8ee-40d3-9bab-b85f7b04fb38.png)

![image](https://user-images.githubusercontent.com/1775518/138172689-39b4b4de-5710-42d1-8fb4-d754db9887b4.png)

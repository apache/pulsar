# PIP-59: gPRC Protocol Handler

* Status: Drafting
* Author: Christophe Bornet
* Pull Request:
* Mailing List discussion:
* Release:

# Motivation

Implemented since v2.5.0, [PIP 41](https://github.com/apache/pulsar/wiki/PIP-41%3A-Pluggable-Protocol-Handler) allows implementing custom protocols natively inside a Pulsar Broker.
This PIP proposes to implement a protocol based on gRPC as an alternative to the Pulsar binary protocol.
Contrary to Kafka-on-Pulsar or MQTT-on-Pulsar, this would only be a replacement of the lower transport/session protocol while keeping Pulsar Producer/Consumer logic.
This will allow clients in languages that have a gRPC implementation but not yet a Pulsar driver to communicate with Pulsar without going through a proxy that limits the performance.

## Pros

* gRPC being a higher level protocol than TCP it handles natively some of the features that are needed in the binary protocol:
  * Framing
  * Session establishment through bidirectional streaming endpoints
  * Keep-alive, reconnect
  * Flow control
  * Metadata/headers transmission
  * Errors
  * Authentication
* Benefit of the gRPC eco-system (proxies, routers, load balancers, ...)
* Simpler writing of drivers in languages that have a gRPC client implementation

## Cons

* gRPC sits on HTTP2 so there will probably be a small overhead compared to the pure TCP binary protocol.
* The internals of the gRPC Java server implementation are often not exposed (on purpose) which means fewer performance optimizations possible.

Performance of the gRPC protocol compared to the binary one should be assessed once developped.

# Changes

## Protocol handler

A protocol handler will be created with the name `grpc`.
When this protocol handler is started it will launch a gRPC server on `grpcServicePort`and a TLS protected one on `grpcServicePortTls` if these configuration properties are set.

The gRPC service loaded by the server will have the form:
```protobuf
service PulsarGrpcService {
    rpc produce(stream Send) returns (stream SendResult) {}
    rpc consume(stream Ack) returns (stream Message) {}
}
```
We can note that contrary to the binary protocol, producers and consumers are handled by distinct RPCs. However they will still share the same HTTP2 connection on which they are multiplexed.

## RPC Metadata/headers

A gRPC `ServerInterceptor` will be used to populate the gRPC Context with values sent in the RPC metadata.
For instance for the produce RPC, the metadata will be the topic name, the producer name, the schema, etc...
The `SocketAddress` (IP /port) of the client is also put in context.
The RPC implementation can then read these data from the context.

## Producers/consumers

At the start of the RPC call, the produce (resp. consume) RPCs will create a producer (resp. consumer) using the parameters passed in the metadata.
This producer will be used as long as the bidirectional stream is alive and will be closed once it completes.
The current `ServerCnx`, `Producer` and `Consumer` classes have a direct dependency on the Netty channel and the binary protocol.
So they must be abstracted to interface or abstract class with distinct implementations for the Pulsar binary protocol and for the gRPC protocol.
By making `Producer` and `Consumer` abstract, most of the producer/consumer logic is reused.
Only the receiving/sending of messages is done in the implementation.
Later other protocol handlers (eg. for RSocket) that would be only changing of the transport will just have to implement those classes.

## Rate limiting

The binary protocol does rate limiting on the publishing by stopping the automatic read of the Netty channel.
For gRPC, it's possible to do something similar by disabling the automatic flow control and stop requesting new data when the rate limit is reached.

## Authentication

If authentication is enabled, a gRPC `ServerInterceptor` will extract authentication data from metadata headers and use the `AuthenticationService` to perform authentication.
If authentication fails, the call will be ended with status `UNAUTHENTICATED`.
If authentication is successful, the corresponding `role` will be added to gRPC's Context so that it can be used by the call implementation.

For 2-way authentication (eg. SASL), if the initial authentication is successful, the challenge data will be set in response headers and the call ended.
The client can then get the challenge, perform authentication on its side, and perform a new call with the challenge response in the call metadata.
The server extracts the response from the metadata and if valid, generates a signed token which contains the authentication role and an expiration date.
Then the call is ended with the token passed as metadata.
The client can then attach the token to its call to perform authentication.
The server will verify the signature of the token and if valid, will put the role in gRPC Context.

## Topic lookup / advertisement
The handler will advertise the host and ports (plaintext and TLS) of the gRPC servers in the [protocol data](https://github.com/apache/pulsar/wiki/PIP-41%3A-Pluggable-Protocol-Handler#protocol-data). This data can then be retrieved during lookup by reading from Zookeeper the load report of the broker owning the topic.

---
title: The Pulsar Go client
tags: [client, go, golang]
---

The Pulsar Go client can be used to create Pulsar [producers](#producers), [consumers](#consumers), and [readers](#readers) in Go (aka Golang).

{% include admonition.html type="info" title="API docs available as well"
   content="For API docs, consult the [Godoc](https://godoc.org/github.com/apache/incubator-pulsar/pulsar-client-go/pulsar)." %}

## Installation

You can install the `pulsar` library locally using `go get`:

```bash
$ go get -u github.com/apache/incubator-pulsar/pulsar-client-go/pulsar
```

Once installed locally, you can import it into your project:

```go
import "github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
```

## Connection URLs

{% include explanations/client-url.md %}

## Client configuration

You can configure your Pulsar client using a `ClientOptions` object. Here's an example:

```go
import (
    "runtime"

    "github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
)

func main() {
        cores := runtime.NumCPU()

        opts := pulsar.ClientOptions{
                URL: "pulsar://localhost:6650",
                OperationTimeoutSeconds: 5,
                MessageListenerThreads: runtime.NumCPU(),
        }

        client := pulsar.NewClient(opts)
}
```

The following configurable parameters are available

Parameter | Description | Default
:---------|:------------|:-------
`URL` | The connection URL for the Pulsar cluster |
`IOThreads` | The number of threads to use for handling connections to Pulsar {% popover brokers %} | 1
`OperationTimeoutSeconds` | The timeout for some Go client operations (creating producers, subscribing to and unsubscribing from {% popover topics %}). Retries will occur until this threshold is reached, at which point the operation will fail. | 30
`MessageListenerThreads` | The number of threads used by message listeners ([consumers](#consumers)) | 1
`ConcurrentLookupRequests` | The number of concurrent lookup requests that can be sent on each broker connection. Setting a maximum helps to keep from overloading brokers. You should set values over the default of 5000 only if the client needs to produce and/or subscribe to thousands of Pulsar topics. | 5000
`Logger` | A custom logger implementation for the client (as a function that takes a log level, filepath, line number, and message). All info, warn, and error messages will be routed to this function.
`EnableTLS` | Whether [TLS](#tls) encryption is enabled for the client | `false`
`TLSTrustCertsFilePath` | The filepath for the trusted TLS certificate |
`TLSAllowInsecureConnection` | Whether the client accepts untrusted TLS certificates from the broker | `false`
`StatsIntervalInSeconds` | The interval at which (in seconds) TODO | 60

## Producers

## Consumers

## TLS encryption {#tls}

In order to use [TLS encryption](../../admin/Authz#), you'll need to configure your client to do so:

* Set `EnableTLS` to `true`
* Set `TLSTrustCertsFilePath` to the path to the TLS certs used by your client and the broker

Here's an example:

```go
opts := pulsar.ClientOptions{
        URL: "pulsar://my-cluster.com:6650",
        EnableTLS: true,
        TLSTrustCertsFilePath: "/path/to/certs/my-cert.csr",
}
```
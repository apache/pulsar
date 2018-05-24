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
        "log"
        "runtime"

        "github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
)

func main() {
        cores := runtime.NumCPU()

        clientOpts := pulsar.ClientOptions{
                URL: "pulsar://localhost:6650",
                OperationTimeoutSeconds: 5,
                MessageListenerThreads: runtime.NumCPU(),
        }

        client, err := pulsar.NewClient(clientOpts)

        if err != nil {
                log.Fatalf("Could not instantiate Pulsar client: %v", err)
        }
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

Pulsar {% popover producers %} publish messages to Pulsar {% popover topics %}. You can [configure](#producer-configuration) Go producers using a `ProducerOptions` object. Here's an example:

```go
producerOpts := pulsar.ProducerOptions{
        Topic: "my-topic",
}

producer, err := client.CreateProducer(producerOpts)

if err != nil {
        log.Fatalf("Could not instantiate Pulsar producer: %v", err)
}

defer producer.Close()

msg := pulsar.ProducerMessage{
        Payload: []byte("Hello, Pulsar"),
}

if err := producer.Send(msg); err != nil {
        log.Fatalf("Producer could not send message: %v", err)
}
```

{% include admonition.html type="warning" title="Blocking operation"
   content="When you create a new Pulsar producer, the operation will block until either a producer is successfully created or an error is thrown." %}

### Producer operations

Pulsar Go producers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`Topic()` | Fetches the producer's {% popover topic %} | `string`
`Name()` | Fetchs the producer's name | `string`
`Send(context.Context, ProducerMessage) error` | Publishes a [message](#messages) to the producer's topic. This call will block until the message is successfully acknowledged by the Pulsar broker, or an error will be thrown if the timeout set using the `SendTimeout` in the producer's [configuration](#producer-configuration) is exceeded. | `error`
`SendAsync(context.Context, ProducerMessage, func(ProducerMessage, error))` | Publishes a [message](#messages) to the producer's topic asynchronously. The third argument is a callback function that specifies what happens either when the message is acknowledged or an error is thrown. |
`Close()` | Closes the producer and releases all resources allocated to it. If `Close()` is called then no more messages will be accepted from the publisher. This method will block until all pending publish requests have been persisted by Pulsar. If an error is thrown, no pending writes will be retried. | `error`

Here's a more involved example usage of a producer:

```go
import (
        "context"
        "fmt"

        "github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
)

func main() {
        // Instantiate a Pulsar client
        client, err := pulsar.NewClient(pulsar.ClientOptions{
                URL: "pulsar://localhost:6650",
        })

        if err != nil { log.Fatal(err) }

        // Use the client to instantiate a producer
        producer, err := client.CreateProducer(pulsar.ProducerOptions{
                Topic: "my-topic",
        })

        if err != nil { log.Fatal(err) }

        ctx := context.Background()

        // Send 10 messages synchronously and 10 messages asynchronously
        for i := 0; i < 10; i++ {
                // Create a message
                msg := pulsar.ProducerMessage{
                        Payload: []byte(fmt.Sprintf("message-%d", i)),
                }

                // Attempt to send the message
                if err := producer.Send(ctx, msg); err != nil {
                        log.Fatal(err)
                }

                // Create a different message to send asynchronously
                asyncMsg := pulsar.ProducerMessage{
                        Payload: []byte(fmt.Sprintf("async-message-%d", i)),
                }

                // Attempt to send the message asynchronously and handle the response
                producer.SendAsync(ctx, asyncMsg, func(msg pulsar.ProducerMessage, err error) {
                        if err != nil { log.Fatal(err) }

                        fmt.Printf("Message %s succesfully published", msg.ID())
                })
        }
}
```

### Producer configuration

Parameter | Description | Default
:---------|:------------|:-------
`Topic` | The Pulsar {% popover topic %} to which the producer will publish messages |
`Name` | A name for the producer. If you don't explicitly assign a name, Pulsar will automatically generate a globally unique name that you can access later using the `Name()` method.  If you choose to explicitly assign a name, it will need to be unique across *all* Pulsar clusters, otherwise the creation operation will throw an error. |
`SendTimeout` | When publishing a message to a topic, the producer will wait for an acknowledgment from the responsible Pulsar {% popover broker %}. If a message is not acknowledged within the threshold set by this parameter, an error will be thrown. If you set `SendTimeout` to -1, the timeout will be set to infinity (and thus removed). Removing the send timeout is recommended when using Pulsar's [message de-duplication](../../cookbooks/message-deduplication) feature. | 30 seconds
`MaxPendingMessages` | |
`MaxPendingMessagesAcrossPartitions` | |
`BlockIfQueueFull` | |
`MessageRoutingMode` | |
`HashingScheme` | |
`CompressionType` | The message data compression type used by the producer. The available options are [`LZ4`](https://github.com/lz4/lz4) and [`ZLIB`](https://zlib.net/). | No compression
`MessageRouter` | By default, Pulsar uses a round-robin routing scheme for [partitioned topics](../../cookbooks/PartitionedTopics). The `MessageRouter` parameter enables you to specify custom routing logic via a function that takes the Pulsar message and topic metadata as an argument and returns an integer (where the ), i.e. a function signature of `func(Message, TopicMetadata) int`. |

## Consumers

Pulsar {% popover consumers %} subscribe to one or more Pulsar {% popover topics %} and listen for incoming messages produced on that topic/those topics. You can [configure](#consumer-configuration) Go consumers using a `ConsumerOptions` object. Here's a basic example that uses channels:

```go
msgChannel := make(chan pulsar.ConsumerMessage)

consumerOpts := pulsar.ConsumerOptions{
        Topic:            "my-topic",
        SubscriptionName: "my-subscription-1",
        Type:             pulsar.Exclusive,
        MessageChannel:   msgChannel,
}

consumer, err := client.Subscribe(consumerOpts)

if err != nil {
        log.Fatalf("Could not establish subscription: %v", err)
}

defer consumer.Close()

for cm := range channel {
        msg := cm.Message

        fmt.Printf("Message ID: %s", msg.ID())
        fmt.Printf("Message value: %s", string(msg.Payload()))

        consumer.Ack(msg)
}
```

{% include admonition.html type="warning" title="Blocking operation"
   content="When you create a new Pulsar consumer, the operation will block until either a producer is successfully created or an error is thrown." %}

### Consumer operations

Pulsar Go consumers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`Topic()` | Fetches the consumer's {% popover topic %} | `string`
`Subscription()` | Fetches the consumer's subscription name | `string`
`Unsubcribe()` | Unsubscribes the consumer from the assigned topic. Throws an error if the unsubscribe operation is somehow unsuccessful. | `error`
`Receive(context.Context)` | Receives a single message from the topic. This method blocks until a message is available. | `(Message, error)`
`Ack(Message)` | {% popover Acknowledges %} a message to the Pulsar {% popover broker %} | `error`
`AckID(MessageID)` | {% popover Acknowledges %} a message to the Pulsar {% popover broker %} by message ID | `error`
`AckCumulative(Message)` | {% popover Acknowledges %} *all* the messages in the stream, up to and including the specified message. The `AckCumulative` method will block until the ack has been sent to the broker. After that, the messages will *not* be redelivered to the consumer. Cumulative acking can only be used with a [shared](../../getting-started/ConceptsAndArchitecture#shared) subscription type.
`Close()` | Closes the consumer, disabling its ability to receive messages from the broker | `error`
`RedeliverUnackedMessages()` | Redelivers *all* unacknowledged messages on the topic. In [failover](../../getting-started/ConceptsAndArchitecture#failover) mode, this request is ignored if the consumer isn't active on the specified topic; in [shared](../../getting-started/ConceptsAndArchitecture#shared) mode, redelivered messages are distributed across all consumers connected to the topic. **Note**: this is a *non-blocking* operation that doesn't throw an error. |

#### Receive example

Here's an example usage of a Go consumer that uses the `Receive()` method to process incoming messages:

```go
import (
        "context"
        "log"

        "github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
)

func main() {
        // Instantiate a Pulsar client
        client, err := pulsar.NewClient(pulsar.ClientOptions{
                URL: "pulsar://localhost:6650",
        })

        if err != nil { log.Fatal(err) }

        // Use the client object to instantiate a consumer
        consumer, err := client.Subscribe(pulsar.ConsumerOptions{
                Topic:            "my-golang-topic",
                SubscriptionName: "sub-1",
                SubscriptionType: pulsar.Exclusive,
        })

        if err != nil { log.Fatal(err) }

        defer consumer.Close()

        ctx := context.Background()

        // Listen indefinitely on the topic
        for {
                msg, err := consumer.Receive(ctx)

                if err != nil { log.Fatal(err) }

                msgBytes := msg.Payload()

                // Do something with the message

                consumer.Ack(msg)
        }
}
```

#### Channel example

Here's a more involved example usage of a Go consumer that uses Go [channels](https://gobyexample.com/channels) to process incoming messages:

```go
import (
        "context"
        "fmt"
        "log"
        "strings"

        "github.com/apache/incubator-pulsar/pulsar-client-go/pulsar"
)

func main() {
        // Instantiate a Pulsar client
        client, err := pulsar.NewClient(pulsar.ClientOptions{
                URL: "pulsar://localhost:6650",
        })

        if err != nil { log.Fatal(err) }

        // Create 5 consumers with a shared subscription
        for i := 0; i < 5; i++ {
                consumerName := fmt.Sprintf("consumer-%d", i)

                // Establish a per-consumer channel to listen for received messages
                rcvChannel := make(chan pulsar.ConsumerMessage)

                consumer, err := client.Subscribe(pulsar.ConsumerOptions{
                        Topic:            "my-golang-topic",
                        SubscriptionName: "shared-subscription-1",
                        SubscriptionType: pulsar.Shared,
                        MessageChannel:   rcvChannel,
                })

                if err != nil { log.Fatal(err) }

                defer consumer.Close()

                // Listen on the message receiver channel
                for cm := range rcvChannel {
                        msg := cm.Message

                        // Unsubscribe if the message contains a specified string
                        msgStr := string(msg.Payload())

                        if strings.Contains(msgStr, "hocus") {
                                log.Fatal(consumer.Unsubscribe())
                        }

                        // Acknowledge the message
                        consumer.Ack(msg)
                }
        }
}
```

### Consumer configuration

Parameter | Description | Default
:---------|:------------|:-------
`Topic` | The Pulsar {% popover topic %} on which the consumer will establish a subscription and listen for messages |
`SubscriptionName` | The subscription name for this consumer |
`Name` | The name of the consumer |
`AckTimeout` | | 0
`SubscriptionType` | Available options are `Exclusive`, `Shared`, and `Failover` | `Exclusive`
`MessageChannel` | The Go channel used by the consumer. Messages that arrive from the Pulsar topic(s) will be passed to this channel. |
`ReceiverQueueSize` | | 1000
`MaxTotalReceiverQueueSizeAcrossPartitions` | | 50000

## Readers

Pulsar {% popover readers %} publish messages to Pulsar {% popover topics %}. You can [configure](#producer-configuration) Go producers using a `ProducerOptions` object. Here's an example:

{% include admonition.html type="warning" title="Blocking operation"
   content="When you create a new Pulsar reader, the operation will block until either a producer is successfully created or an error is thrown." %}

## Messages

The Pulsar Go client provides a `ProducerMessage` interface that you can use to construct messages to producer on Pulsar topics. Here's an example message:

```go
msg := pulsar.ProducerMessage{
        Payload: []byte("Here is some message data"),
        Key: "message-key",
        Properties: map[string]string{
            "foo": "bar",
        },
        EventTime: time.Now(),
        ReplicationClusters: []string{"cluster1", "cluster3"},
}

if err := producer.send(msg); err != nil {
        log.Fatalf("Could not publish message due to: %v", err)
}
```

Parameter | Description
:---------|:-----------
`Payload` | The actual data payload of the message
`Key` | The optional key associated with the message (particularly useful for things like topic compaction)
`Properties` | A key-value map (both keys and values must be strings) for any application-specific metadata attached to the message
`EventTime` | The timestamp associated with the message
`ReplicationClusters` | The clusters to which this message will be replicated. Pulsar brokers handle message replication automatically; you should only change this setting if you want to override the broker default.

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
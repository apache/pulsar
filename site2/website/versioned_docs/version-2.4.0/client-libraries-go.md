---
id: version-2.4.0-client-libraries-go
title: The Pulsar Go client
sidebar_label: Go
original_id: client-libraries-go
---

The Pulsar Go client can be used to create Pulsar [producers](#producers), [consumers](#consumers), and [readers](#readers) in Go (aka Golang).

> **API docs available as well**  
> For standard API docs, consult the [Godoc](https://godoc.org/github.com/apache/pulsar/pulsar-client-go/pulsar).


## Installation

### Requirements

Pulsar Go client library is based on the C++ client library. Follow
the instructions for [C++ library](client-libraries-cpp.md) for installing the binaries
through [RPM](client-libraries-cpp.md#rpm), [Deb](client-libraries-cpp.md#deb) or [Homebrew packages](client-libraries-cpp.md#macos).

### Installing go package

> **Compatibility Warning**  
> The version number of the Go client **must match** the version number of the Pulsar C++ client library.

You can install the `pulsar` library locally using `go get`.  Note that `go get` doesn't support fetching a specific tag - it will always pull in master's version of the Go client.  You'll need a C++ client library that matches master.

```bash
$ go get -u github.com/apache/pulsar/pulsar-client-go/pulsar
```

Or you can use [dep](https://github.com/golang/dep) for managing the dependencies.

```bash
$ dep ensure -add github.com/apache/pulsar/pulsar-client-go/pulsar@v{{pulsar:version}}
```

Once installed locally, you can import it into your project:

```go
import "github.com/apache/pulsar/pulsar-client-go/pulsar"
```

## Connection URLs

To connect to Pulsar using client libraries, you need to specify a [Pulsar protocol](developing-binary-protocol.md) URL.

Pulsar protocol URLs are assigned to specific clusters, use the `pulsar` scheme and have a default port of 6650. Here's an example for `localhost`:

```http
pulsar://localhost:6650
```

A URL for a production Pulsar cluster may look something like this:

```http
pulsar://pulsar.us-west.example.com:6650
```

If you're using [TLS](security-tls-authentication.md) authentication, the URL will look like something like this:

```http
pulsar+ssl://pulsar.us-west.example.com:6651
```

## Creating a client

In order to interact with Pulsar, you'll first need a `Client` object. You can create a client object using the `NewClient` function, passing in a `ClientOptions` object (more on configuration [below](#client-configuration)). Here's an example:


```go
import (
    "log"
    "runtime"

    "github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {
    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL: "pulsar://localhost:6650",
        OperationTimeoutSeconds: 5,
        MessageListenerThreads: runtime.NumCPU(),
    })

    if err != nil {
        log.Fatalf("Could not instantiate Pulsar client: %v", err)
    }
}
```

### Client operations

Pulsar Go client has the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`CreateProducer(ProducerOptions)` | Create the producer instance. (This method will be blocked until the producer is created successfully.) | `(Producer, error)`
`CreateProducerWithSchema(ProducerOptions, Schema)` | Create a producer instance with schema. | `(Producer, error)`
`Subscribe(ConsumerOptions)` | Create a `Consumer` by subscribing to a topic. | `(Consumer, error)`
`SubscribeWithSchema(ConsumerOptions, Schema)` | Create a `Consumer` with schema by subscribing to a topic. | `(Consumer, error)`
`CreateReader(ReaderOptions)` | Create a Reader instance. | `(Reader, error)`
`CreateReaderWithSchema(ReaderOptions, Schema)` | Create a Reader instance with schema. | `(Reader, error)`
`TopicPartitions(topic string)` | Fetch the list of partitions for a given topic. | `([]string, error)`
`Close()` | Close the Pulsar Go client and release associated resources. | `error`

The following configurable parameters are available for Pulsar clients:

Parameter | Description | Default
:---------|:------------|:-------
`URL` | The connection URL for the Pulsar cluster. See [above](#urls) for more info |
`IOThreads` | The number of threads to use for handling connections to Pulsar [brokers](reference-terminology.md#broker) | 1
`OperationTimeoutSeconds` | The timeout for some Go client operations (creating producers, subscribing to and unsubscribing from [topics](reference-terminology.md#topic)). Retries will occur until this threshold is reached, at which point the operation will fail. | 30
`MessageListenerThreads` | The number of threads used by message listeners ([consumers](#consumers) and [readers](#readers)) | 1
`ConcurrentLookupRequests` | The number of concurrent lookup requests that can be sent on each broker connection. Setting a maximum helps to keep from overloading brokers. You should set values over the default of 5000 only if the client needs to produce and/or subscribe to thousands of Pulsar topics. | 5000
`Logger` | A custom logger implementation for the client (as a function that takes a log level, file path, line number, and message). All info, warn, and error messages will be routed to this function. | `nil`
`TLSTrustCertsFilePath` | The file path for the trusted TLS certificate |
`TLSAllowInsecureConnection` | Whether the client accepts untrusted TLS certificates from the broker | `false`
`Authentication` | Configure the authentication provider. (default: no authentication). Example: `Authentication: NewAuthenticationTLS("my-cert.pem", "my-key.pem")` | `nil`
`StatsIntervalInSeconds` | The interval (in seconds) at which client stats are published | 60

## Producers

Pulsar producers publish messages to Pulsar topics. You can [configure](#producer-configuration) Go producers using a `ProducerOptions` object. Here's an example:

```go
producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic: "my-topic",
})

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

> **Blocking operation**  
> When you create a new Pulsar producer, the operation will block (waiting on a go channel) until either a producer is successfully created or an error is thrown.


### Producer operations

Pulsar Go producers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`Topic()` | Fetches the producer's [topic](reference-terminology.md#topic)| `string`
`Name()` | Fetches the producer's name | `string`
`Send(context.Context, ProducerMessage) error` | Publishes a [message](#messages) to the producer's topic. This call will block until the message is successfully acknowledged by the Pulsar broker, or an error will be thrown if the timeout set using the `SendTimeout` in the producer's [configuration](#producer-configuration) is exceeded. | `error`
`SendAsync(context.Context, ProducerMessage, func(ProducerMessage, error))` | Publishes a [message](#messages) to the producer's topic asynchronously. The third argument is a callback function that specifies what happens either when the message is acknowledged or an error is thrown. |
`Close()` | Closes the producer and releases all resources allocated to it. If `Close()` is called then no more messages will be accepted from the publisher. This method will block until all pending publish requests have been persisted by Pulsar. If an error is thrown, no pending writes will be retried. | `error`

Here's a more involved example usage of a producer:

```go
import (
    "context"
    "fmt"
    "log"

    "github.com/apache/pulsar/pulsar-client-go/pulsar"
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

            fmt.Printf("the %s successfully published", string(msg.Payload))
        })
    }
}
```

### Producer configuration

Parameter | Description | Default
:---------|:------------|:-------
`Topic` | The Pulsar [topic](reference-terminology.md#topic) to which the producer will publish messages |
`Name` | A name for the producer. If you don't explicitly assign a name, Pulsar will automatically generate a globally unique name that you can access later using the `Name()` method.  If you choose to explicitly assign a name, it will need to be unique across *all* Pulsar clusters, otherwise the creation operation will throw an error. |
`SendTimeout` | When publishing a message to a topic, the producer will wait for an acknowledgment from the responsible Pulsar [broker](reference-terminology.md#broker). If a message is not acknowledged within the threshold set by this parameter, an error will be thrown. If you set `SendTimeout` to -1, the timeout will be set to infinity (and thus removed). Removing the send timeout is recommended when using Pulsar's [message de-duplication](cookbooks-deduplication.md) feature. | 30 seconds
`MaxPendingMessages` | The maximum size of the queue holding pending messages (i.e. messages waiting to receive an acknowledgment from the [broker](reference-terminology.md#broker)). By default, when the queue is full all calls to the `Send` and `SendAsync` methods will fail *unless* `BlockIfQueueFull` is set to `true`. |
`MaxPendingMessagesAcrossPartitions` | |
`BlockIfQueueFull` | If set to `true`, the producer's `Send` and `SendAsync` methods will block when the outgoing message queue is full rather than failing and throwing an error (the size of that queue is dictated by the `MaxPendingMessages` parameter); if set to `false` (the default), `Send` and `SendAsync` operations will fail and throw a `ProducerQueueIsFullError` when the queue is full. | `false`
`MessageRoutingMode` | The message routing logic (for producers on [partitioned topics](concepts-architecture-overview.md#partitioned-topics)). This logic is applied only when no key is set on messages. The available options are: round robin (`pulsar.RoundRobinDistribution`, the default), publishing all messages to a single partition (`pulsar.UseSinglePartition`), or a custom partitioning scheme (`pulsar.CustomPartition`). | `pulsar.RoundRobinDistribution`
`HashingScheme` | The hashing function that determines the partition on which a particular message is published (partitioned topics only). The available options are: `pulsar.JavaStringHash` (the equivalent of `String.hashCode()` in Java), `pulsar.Murmur3_32Hash` (applies the [Murmur3](https://en.wikipedia.org/wiki/MurmurHash) hashing function), or `pulsar.BoostHash` (applies the hashing function from C++'s [Boost](https://www.boost.org/doc/libs/1_62_0/doc/html/hash.html) library) | `pulsar.JavaStringHash`
`CompressionType` | The message data compression type used by the producer. The available options are [`LZ4`](https://github.com/lz4/lz4), [`ZLIB`](https://zlib.net/), [`ZSTD`](https://facebook.github.io/zstd/) and [`SNAPPY`](https://google.github.io/snappy/). | No compression
`MessageRouter` | By default, Pulsar uses a round-robin routing scheme for [partitioned topics](cookbooks-partitioned.md). The `MessageRouter` parameter enables you to specify custom routing logic via a function that takes the Pulsar message and topic metadata as an argument and returns an integer (where the ), i.e. a function signature of `func(Message, TopicMetadata) int`. |

## Consumers

Pulsar consumers subscribe to one or more Pulsar topics and listen for incoming messages produced on that topic/those topics. You can [configure](#consumer-configuration) Go consumers using a `ConsumerOptions` object. Here's a basic example that uses channels:

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

for cm := range msgChannel {
    msg := cm.Message

    fmt.Printf("Message ID: %s", msg.ID())
    fmt.Printf("Message value: %s", string(msg.Payload()))

    consumer.Ack(msg)
}
```

> **Blocking operation**  
> When you create a new Pulsar consumer, the operation will block (on a go channel) until either a producer is successfully created or an error is thrown.


### Consumer operations

Pulsar Go consumers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`Topic()` | Returns the consumer's [topic](reference-terminology.md#topic) | `string`
`Subscription()` | Returns the consumer's subscription name | `string`
`Unsubcribe()` | Unsubscribes the consumer from the assigned topic. Throws an error if the unsubscribe operation is somehow unsuccessful. | `error`
`Receive(context.Context)` | Receives a single message from the topic. This method blocks until a message is available. | `(Message, error)`
`Ack(Message)` | [Acknowledges](reference-terminology.md#acknowledgment-ack) a message to the Pulsar [broker](reference-terminology.md#broker) | `error`
`AckID(MessageID)` | [Acknowledges](reference-terminology.md#acknowledgment-ack) a message to the Pulsar [broker](reference-terminology.md#broker) by message ID | `error`
`AckCumulative(Message)` | [Acknowledges](reference-terminology.md#acknowledgment-ack) *all* the messages in the stream, up to and including the specified message. The `AckCumulative` method will block until the ack has been sent to the broker. After that, the messages will *not* be redelivered to the consumer. Cumulative acking can only be used with a [shared](concepts-messaging.md#shared) subscription type. | `error`
`Nack(Message)` | Acknowledge the failure to process a single message. | `error`
`NackID(MessageID)` | Acknowledge the failure to process a single message. | `error`
`Close()` | Closes the consumer, disabling its ability to receive messages from the broker | `error`
`RedeliverUnackedMessages()` | Redelivers *all* unacknowledged messages on the topic. In [failover](concepts-messaging.md#failover) mode, this request is ignored if the consumer isn't active on the specified topic; in [shared](concepts-messaging.md#shared) mode, redelivered messages are distributed across all consumers connected to the topic. **Note**: this is a *non-blocking* operation that doesn't throw an error. |
`Schema()` | Set the message schema definition | `Schema`

#### Receive example

Here's an example usage of a Go consumer that uses the `Receive()` method to process incoming messages:

```go
import (
    "context"
    "log"

    "github.com/apache/pulsar/pulsar-client-go/pulsar"
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

        // Do something with the message
        err = processMessage(msg)

        if err == nil {
            // Message processed successfully
            consumer.Ack(msg)
        } else {
            // Failed to process messages
            consumer.Nack(msg)
        }
    }
}
```

#### Schema example

This example shows how to create a producer and consumer with schema.

```go
var exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
    		"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
jsonSchema := NewJsonSchema(exampleSchemaDef, nil)

// create producer
producer, err := client.CreateProducerWithSchema(ProducerOptions{
	Topic: "jsonTopic",
}, jsonSchema)
err = producer.Send(context.Background(), ProducerMessage{
	Value: &testJson{
		ID:   100,
		Name: "pulsar",
	},
})
if err != nil {
	log.Fatal(err)
}
defer producer.Close()

//create consumer
var s testJson

consumerJS := NewJsonSchema(exampleSchemaDef, nil)
consumer, err := client.SubscribeWithSchema(ConsumerOptions{
	Topic:            "jsonTopic",
	SubscriptionName: "sub-2",
}, consumerJS)
if err != nil {
	log.Fatal(err)
}
msg, err := consumer.Receive(context.Background())
if err != nil {
	log.Fatal(err)
}
err = msg.GetValue(&s)
if err != nil {
	log.Fatal(err)
}
fmt.Println(s.ID) // output: 100
fmt.Println(s.Name) // output: pulsar

defer consumer.Close()
```

### Consumer configuration

Parameter | Description | Default
:---------|:------------|:-------
`Topic` | The Pulsar [topic](reference-terminology.md#topic) on which the consumer will establish a subscription and listen for messages |
`SubscriptionName` | The subscription name for this consumer |
`Name` | The name of the consumer |
`AckTimeout` | | 0
`NackRedeliveryDelay` | The delay after which to redeliver the messages that failed to be processed. Default is 1min. (See `Consumer.Nack()`) | 1 minute
`SubscriptionType` | Available options are `Exclusive`, `Shared`, `Key_Shared` and `Failover` | `Exclusive`
`MessageChannel` | The Go channel used by the consumer. Messages that arrive from the Pulsar topic(s) will be passed to this channel. |
`ReceiverQueueSize` | Sets the size of the consumer's receiver queue, i.e. the number of messages that can be accumulated by the consumer before the application calls `Receive`. A value higher than the default of 1000 could increase consumer throughput, though at the expense of more memory utilization. | 1000
`MaxTotalReceiverQueueSizeAcrossPartitions` |Set the max total receiver queue size across partitions. This setting will be used to reduce the receiver queue size for individual partitions if the total exceeds this value | 50000
`Name` | Set the consumer name. | `string`
`ReadCompacted` | If enabled, the consumer will read messages from the compacted topic rather than reading the full message backlog of the topic.| `bool`
`Schema` | Message schema definition| 

## Readers

Pulsar readers process messages from Pulsar topics. Readers are different from consumers because with readers you need to explicitly specify which message in the stream you want to begin with (consumers, on the other hand, automatically begin with the most recent unacked message). You can [configure](#reader-configuration) Go readers using a `ReaderOptions` object. Here's an example:

```go
reader, err := client.CreateReader(pulsar.ReaderOptions{
    Topic: "my-golang-topic",
    StartMessageId: pulsar.LatestMessage,
})
```

> **Blocking operation**  
> When you create a new Pulsar reader, the operation will block (on a go channel) until either a reader is successfully created or an error is thrown.


### Reader operations

Pulsar Go readers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`Topic()` | Returns the reader's [topic](reference-terminology.md#topic) | `string`
`Next(context.Context)` | Receives the next message on the topic (analogous to the `Receive` method for [consumers](#consumer-operations)). This method blocks until a message is available. | `(Message, error)`
`Close()` | Closes the reader, disabling its ability to receive messages from the broker | `error`

#### "Next" example

Here's an example usage of a Go reader that uses the `Next()` method to process incoming messages:

```go
import (
    "context"
    "log"

    "github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {
    // Instantiate a Pulsar client
    client, err := pulsar.NewClient(pulsar.ClientOptions{
            URL: "pulsar://localhost:6650",
    })

    if err != nil { log.Fatalf("Could not create client: %v", err) }

    // Use the client to instantiate a reader
    reader, err := client.CreateReader(pulsar.ReaderOptions{
        Topic:          "my-golang-topic",
        StartMessageID: pulsar.EarliestMessage,
    })

    if err != nil { log.Fatalf("Could not create reader: %v", err) }

    defer reader.Close()

    ctx := context.Background()

    // Listen on the topic for incoming messages
    for {
        msg, err := reader.Next(ctx)
        if err != nil { log.Fatalf("Error reading from topic: %v", err) }

        // Process the message
    }
}
```

In the example above, the reader begins reading from the earliest available message (specified by `pulsar.EarliestMessage`). The reader can also begin reading from the latest message (`pulsar.LatestMessage`) or some other message ID specified by bytes using the `DeserializeMessageID` function, which takes a byte array and returns a `MessageID` object. Here's an example:

```go
lastSavedId := // Read last saved message id from external store as byte[]

reader, err := client.CreateReader(pulsar.ReaderOptions{
    Topic:          "my-golang-topic",
    StartMessageID: DeserializeMessageID(lastSavedId),
})
```

### Reader configuration

Parameter | Description | Default
:---------|:------------|:-------
`Topic` | The Pulsar [topic](reference-terminology.md#topic) on which the reader will establish a subscription and listen for messages |
`Name` | The name of the reader |
`StartMessageID` | The initial reader position, i.e. the message at which the reader begins processing messages. The options are `pulsar.EarliestMessage` (the earliest available message on the topic), `pulsar.LatestMessage` (the latest available message on the topic), or a `MessageID` object for a position that isn't earliest or latest. |
`MessageChannel` | The Go channel used by the reader. Messages that arrive from the Pulsar topic(s) will be passed to this channel. |
`ReceiverQueueSize` | Sets the size of the reader's receiver queue, i.e. the number of messages that can be accumulated by the reader before the application calls `Next`. A value higher than the default of 1000 could increase reader throughput, though at the expense of more memory utilization. | 1000
`SubscriptionRolePrefix` | The subscription role prefix. | `reader`

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

The following methods parameters are available for `ProducerMessage` objects:

Parameter | Description
:---------|:-----------
`Payload` | The actual data payload of the message
`Key` | The optional key associated with the message (particularly useful for things like topic compaction)
`Properties` | A key-value map (both keys and values must be strings) for any application-specific metadata attached to the message
`EventTime` | The timestamp associated with the message
`ReplicationClusters` | The clusters to which this message will be replicated. Pulsar brokers handle message replication automatically; you should only change this setting if you want to override the broker default.

## TLS encryption and authentication

In order to use [TLS encryption](security-tls-transport.md), you'll need to configure your client to do so:

 * Use `pulsar+ssl` URL type
 * Set `TLSTrustCertsFilePath` to the path to the TLS certs used by your client and the Pulsar broker
 * Configure `Authentication` option

Here's an example:

```go
opts := pulsar.ClientOptions{
    URL: "pulsar+ssl://my-cluster.com:6651",
    TLSTrustCertsFilePath: "/path/to/certs/my-cert.csr",
    Authentication: NewAuthenticationTLS("my-cert.pem", "my-key.pem"),
}
```

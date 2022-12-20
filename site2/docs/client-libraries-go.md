---
id: client-libraries-go
title: Pulsar Go client
sidebar_label: "Go"
---

You can use a Pulsar [Go client](https://github.com/apache/pulsar-client-go) to create Pulsar [producers](#producers), [consumers](#consumers), and [readers](#readers) in Golang. For Pulsar features that Go clients support, see [Client Feature Matrix](https://docs.google.com/spreadsheets/d/1YHYTkIXR8-Ql103u-IMI18TXLlGStK8uJjDsOOA0T20/edit#gid=1784579914).

## Installation

You can install the `pulsar` library by using either `go get` or `go module`.

### Use `go get`

1. Download the library of Go client to your local environment:

   ```bash
   go get -u "github.com/apache/pulsar-client-go/pulsar"
   ```

2. Import it into your project:

   ```go
   import "github.com/apache/pulsar-client-go/pulsar"
   ```

### Use `go module`

1. Create a directory named `test_dir` and change your working directory to it.

   ```bash
   mkdir test_dir && cd test_dir
   ```

2. Write a sample script (such as `test_example.go`) in the `test_dir` directory and write `package main` at the beginning of the file.

   ```bash
   go mod init test_dir 
   go mod tidy && go mod download
   go build test_example.go
   ./test_example
   ```

## Connection URLs

To connect to Pulsar using client libraries, you need to specify a [Pulsar protocol](developing-binary-protocol.md) URL.

You can assign Pulsar protocol URLs to specific clusters and use the `pulsar` scheme. The following is an example of `localhost` with the default port `6650`:

```http
pulsar://localhost:6650
```

If you have multiple brokers, separate `IP:port` by commas:

```http
pulsar://localhost:6550,localhost:6651,localhost:6652
```

If you use [TLS](security-tls-authentication.md) authentication, add `+ssl` in the scheme:

```http
pulsar+ssl://pulsar.us-west.example.com:6651
```

## API reference

API docs are available on the [Godoc](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar) page.

## Release notes

For the changelog of Pulsar Go clients, see [release notes](/release-notes/#go).

## Create a client

To interact with Pulsar, you need a [`Client`](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#Client) object first. You can create a client object using the [`NewClient`](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#NewClient) function, passing in a [`ClientOptions`](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#ClientOptions) object. Here's an example:

```go
import (
    "log"
    "time"

    "github.com/apache/pulsar-client-go/pulsar"
)

func main() {
    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL:               "pulsar://localhost:6650",
        OperationTimeout:  30 * time.Second,
        ConnectionTimeout: 30 * time.Second,
    })
    if err != nil {
        log.Fatalf("Could not instantiate Pulsar client: %v", err)
    }

    defer client.Close()
}
```

If you have multiple brokers, you can initiate a client object as below.

```go
import (
    "log"
    "time"
    "github.com/apache/pulsar-client-go/pulsar"
)

func main() {
    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL: "pulsar://localhost:6650,localhost:6651,localhost:6652",
        OperationTimeout:  30 * time.Second,
        ConnectionTimeout: 30 * time.Second,
    })
    if err != nil {
        log.Fatalf("Could not instantiate Pulsar client: %v", err)
    }

    defer client.Close()
}
```

All configurable parameters for `ClientOptions` are [here](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#ClientOptions).

## Producers

Pulsar producers publish messages to Pulsar topics. You can [configure](#producer-configuration) Go producers using a `ProducerOptions` object. Here's an example:

```go
producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic: "my-topic",
})

if err != nil {
    log.Fatal(err)
}

_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
    Payload: []byte("hello"),
})

defer producer.Close()

if err != nil {
    fmt.Println("Failed to publish message", err)
}
fmt.Println("Published message")
```

### Producer operations

All available methods of `Producer` interface are [here](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#Producer).

### Producer Example

#### How to use message router in producer

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})

if err != nil {
    log.Fatal(err)
}
defer client.Close()

producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic: "my-partitioned-topic",
    MessageRouter: func(msg *pulsar.ProducerMessage, tm pulsar.TopicMetadata) int {
        fmt.Println("Topic has", tm.NumPartitions(), "partitions. Routing message ", msg, " to partition 2.")
        // always push msg to partition 2
        return 2
    },
})

if err != nil {
    log.Fatal(err)
}
defer producer.Close()

for i := 0; i < 10; i++ {
    if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
        Payload: []byte(fmt.Sprintf("message-%d", i)),
    }); err != nil {
        log.Fatal(err)
    } else {
        log.Println("Published message: ", msgId)
    }
}

// subscribe a specific partition of a topic
// for demos only, not recommend to subscribe a specific partition
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    // pulsar partition is a special topic has the suffix '-partition-xx'
    Topic:            "my-partitioned-topic-partition-2",
    SubscriptionName: "my-sub",
    Type:             pulsar.Shared,
})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

for i := 0; i < 10; i++ {
    msg, err := consumer.Receive(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
    consumer.Ack(msg)
}
```

#### How to use chunking in producer

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
	URL: serviceURL,
})

if err != nil {
	log.Fatal(err)
}
defer client.Close()

// The message chunking feature is OFF by default.
// By default, a producer chunks the large message based on the max message size (`maxMessageSize`) configured at the broker side (for example, 5MB).
// Client can also configure the max chunked size using the producer configuration `ChunkMaxMessageSize`.
// Note: to enable chunking, you need to disable batching (`DisableBatching=true`) concurrently.
producer, err := client.CreateProducer(pulsar.ProducerOptions{
  Topic:               "my-topic",
  DisableBatching:     true,
  EnableChunking:      true,
})

if err != nil {
	log.Fatal(err)
}
defer producer.Close()
```

#### How to use schema interface in producer

```go
type testJSON struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}

var (
    exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
        "\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
)

client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

properties := make(map[string]string)
properties["pulsar"] = "hello"
jsonSchemaWithProperties := pulsar.NewJSONSchema(exampleSchemaDef, properties)
producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic:  "jsonTopic",
    Schema: jsonSchemaWithProperties,
})

if err != nil {
    log.Fatal(err)
}

_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
    Value: &testJSON{
        ID:   100,
        Name: "pulsar",
    },
})
if err != nil {
    log.Fatal(err)
}
producer.Close()
```

#### How to use delay relative in producer

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

topicName := "topic-1"
producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic:           topicName,
    DisableBatching: true,
})
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            topicName,
    SubscriptionName: "subName",
    Type:             pulsar.Shared,
})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

ID, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
    Payload:      []byte(fmt.Sprintf("test")),
    DeliverAfter: 3 * time.Second,
})
if err != nil {
    log.Fatal(err)
}
fmt.Println(ID)

ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
msg, err := consumer.Receive(ctx)
if err != nil {
    log.Fatal(err)
}
fmt.Println(msg.Payload())
cancel()

ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
msg, err = consumer.Receive(ctx)
if err != nil {
    log.Fatal(err)
}
fmt.Println(msg.Payload())
cancel()
```

#### How to use Prometheus metrics in producer

Pulsar Go client registers client metrics using Prometheus. This section demonstrates how to create a simple Pulsar producer application that exposes Prometheus metrics via HTTP.

1. Write a simple producer application.

```go
// Create a Pulsar client
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})
if err != nil {
    log.Fatal(err)
}

defer client.Close()

// Start a separate goroutine for Prometheus metrics
// In this case, Prometheus metrics can be accessed via http://localhost:2112/metrics
go func() {
    prometheusPort := 2112
    log.Printf("Starting Prometheus metrics at http://localhost:%v/metrics\n", prometheusPort)
    http.Handle("/metrics", promhttp.Handler())
    err = http.ListenAndServe(":"+strconv.Itoa(prometheusPort), nil)
    if err != nil {
        log.Fatal(err)
    }
}()

// Create a producer
producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic: "topic-1",
})
if err != nil {
    log.Fatal(err)
}

defer producer.Close()

ctx := context.Background()

// Write your business logic here
// In this case, you build a simple Web server. You can produce messages by requesting http://localhost:8082/produce
webPort := 8082
http.HandleFunc("/produce", func(w http.ResponseWriter, r *http.Request) {
    msgId, err := producer.Send(ctx, &pulsar.ProducerMessage{
        Payload: []byte(fmt.Sprintf("hello world")),
    })
    if err != nil {
        log.Fatal(err)
    } else {
        log.Printf("Published message: %v", msgId)
        fmt.Fprintf(w, "Published message: %v", msgId)
    }
})

err = http.ListenAndServe(":"+strconv.Itoa(webPort), nil)
if err != nil {
    log.Fatal(err)
}
```

2. To scrape metrics from applications, configure a local running Prometheus instance using a configuration file (`prometheus.yml`).

```yaml
scrape_configs:
- job_name: pulsar-client-go-metrics
  scrape_interval: 10s
  static_configs:
  - targets:
  - localhost:2112
```

Now you can query Pulsar client metrics on Prometheus.

### Producer configuration

All available options of `ProducerOptions` are [here](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#ProducerOptions).

## Consumers

Pulsar consumers subscribe to one or more Pulsar topics and listen for incoming messages produced on that topic/those topics. You can [configure](#consumer-configuration) Go consumers using a `ConsumerOptions` object. Here's a basic example that uses channels:

```go
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            "topic-1",
    SubscriptionName: "my-sub",
    Type:             pulsar.Shared,
})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

for i := 0; i < 10; i++ {
    // may block here
    msg, err := consumer.Receive(context.Background())
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
        msg.ID(), string(msg.Payload()))

    consumer.Ack(msg)
}

if err := consumer.Unsubscribe(); err != nil {
    log.Fatal(err)
}
```

### Consumer operations

All available methods of `Consumer` interface are [here](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#Consumer).

#### Create single-topic consumer

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
if err != nil {
    log.Fatal(err)
}

defer client.Close()

consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    // fill `Topic` field will create a single-topic consumer
    Topic:            "topic-1",
    SubscriptionName: "my-sub",
    Type:             pulsar.Shared,
})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()
```

#### Create regex-topic consumer

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})
defer client.Close()

topicsPattern := "persistent://public/default/topic.*"
opts := pulsar.ConsumerOptions{
    // fill `TopicsPattern` field will create a regex consumer
    TopicsPattern:    topicsPattern,
    SubscriptionName: "regex-sub",
}

consumer, err := client.Subscribe(opts)
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()
```

#### Create multi-topic consumer

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})
if err != nil {
    log.Fatal(err)
}

topics := []string{"topic-1", "topic-2"}
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    // fill `Topics` field will create a multi-topic consumer
    Topics:           topics,
    SubscriptionName: "multi-topic-sub",
})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()
```

#### Create consumer listener

```go
import (
    "fmt"
    "log"

    "github.com/apache/pulsar-client-go/pulsar"
)

func main() {
    client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
    if err != nil {
        log.Fatal(err)
    }

    defer client.Close()

    // we can listen this channel
    channel := make(chan pulsar.ConsumerMessage, 100)

    options := pulsar.ConsumerOptions{
        Topic:            "topic-1",
        SubscriptionName: "my-subscription",
        Type:             pulsar.Shared,
        // fill `MessageChannel` field will create a listener
        MessageChannel: channel,
    }

    consumer, err := client.Subscribe(options)
    if err != nil {
        log.Fatal(err)
    }

    defer consumer.Close()

    // Receive messages from channel. The channel returns a struct `ConsumerMessage` which contains message and the consumer from where
    // the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
    // shared across multiple consumers as well
    for cm := range channel {
        consumer := cm.Consumer
        msg := cm.Message
        fmt.Printf("Consumer %s received a message, msgId: %v, content: '%s'\n",
            consumer.Name(), msg.ID(), string(msg.Payload()))

        consumer.Ack(msg)
    }
}
```

#### Receive message with timeout

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

topic := "test-topic-with-no-messages"
ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
defer cancel()

// create consumer
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            topic,
    SubscriptionName: "my-sub1",
    Type:             pulsar.Shared,
})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

// receive message with a timeout
msg, err := consumer.Receive(ctx)
if err != nil {
    log.Fatal(err)
}
fmt.Println(msg.Payload())
```

#### Use schema in consumer

```go
type testJSON struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}

var (
    exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
        "\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
)

client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

var s testJSON

consumerJS := pulsar.NewJSONSchema(exampleSchemaDef, nil)
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:                       "jsonTopic",
    SubscriptionName:            "sub-1",
    Schema:                      consumerJS,
    SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
})
if err != nil {
    log.Fatal(err)
}

msg, err := consumer.Receive(context.Background())
if err != nil {
    log.Fatal(err)
}

err = msg.GetSchemaValue(&s)
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()
```

#### How to use Prometheus metrics in consumer

In this guide, This section demonstrates how to create a simple Pulsar consumer application that exposes Prometheus metrics via HTTP.
1. Write a simple consumer application.

```go
// Create a Pulsar client
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})
if err != nil {
    log.Fatal(err)
}

defer client.Close()

// Start a separate goroutine for Prometheus metrics
// In this case, Prometheus metrics can be accessed via http://localhost:2112/metrics
go func() {
    prometheusPort := 2112
    log.Printf("Starting Prometheus metrics at http://localhost:%v/metrics\n", prometheusPort)
    http.Handle("/metrics", promhttp.Handler())
    err = http.ListenAndServe(":"+strconv.Itoa(prometheusPort), nil)
    if err != nil {
        log.Fatal(err)
    }
}()

// Create a consumer
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            "topic-1",
    SubscriptionName: "sub-1",
    Type:             pulsar.Shared,
})
if err != nil {
    log.Fatal(err)
}

defer consumer.Close()

ctx := context.Background()

// Write your business logic here
// In this case, you build a simple Web server. You can consume messages by requesting http://localhost:8083/consume
webPort := 8083
http.HandleFunc("/consume", func(w http.ResponseWriter, r *http.Request) {
    msg, err := consumer.Receive(ctx)
    if err != nil {
        log.Fatal(err)
    } else {
        log.Printf("Received message msgId: %v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
        fmt.Fprintf(w, "Received message msgId: %v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
        consumer.Ack(msg)
    }
})

err = http.ListenAndServe(":"+strconv.Itoa(webPort), nil)
if err != nil {
    log.Fatal(err)
}
```

2. To scrape metrics from applications, configure a local running Prometheus instance using a configuration file (`prometheus.yml`).

```yaml
scrape_configs:
- job_name: pulsar-client-go-metrics
  scrape_interval: 10s
  static_configs:
  - targets:
  - localhost: 2112
```

Now you can query Pulsar client metrics on Prometheus.

### Consumer configuration

All available options of `ConsumerOptions` are [here](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#ConsumerOptions).

## Readers

Pulsar readers process messages from Pulsar topics. Readers are different from consumers because with readers you need to explicitly specify which message in the stream you want to begin with (consumers, on the other hand, automatically begin with the most recent unacked message). You can [configure](#reader-configuration) Go readers using a `ReaderOptions` object. Here's an example:

```go
reader, err := client.CreateReader(pulsar.ReaderOptions{
    Topic:          "topic-1",
    StartMessageID: pulsar.EarliestMessageID(),
})
if err != nil {
    log.Fatal(err)
}
defer reader.Close()
```

### Reader operations

All available methods of the `Reader` interface are [here](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#Reader).

### Reader example

#### How to use reader to read 'next' message

Here's an example usage of a Go reader that uses the `Next()` method to process incoming messages:

```go
import (
    "context"
    "fmt"
    "log"

    "github.com/apache/pulsar-client-go/pulsar"
)

func main() {
    client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
    if err != nil {
        log.Fatal(err)
    }

    defer client.Close()

    reader, err := client.CreateReader(pulsar.ReaderOptions{
        Topic:          "topic-1",
        StartMessageID: pulsar.EarliestMessageID(),
    })
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Close()

    for reader.HasNext() {
        msg, err := reader.Next(context.Background())
        if err != nil {
            log.Fatal(err)
        }

        fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
            msg.ID(), string(msg.Payload()))
    }
}
```

In the example above, the reader begins reading from the earliest available message (specified by `pulsar.EarliestMessage`). The reader can also begin reading from the latest message (`pulsar.LatestMessage`) or some other message ID specified by bytes using the `DeserializeMessageID` function, which takes a byte array and returns a `MessageID` object. Here's an example:

```go
lastSavedId := // Read last saved message id from external store as byte[]

reader, err := client.CreateReader(pulsar.ReaderOptions{
    Topic:          "my-golang-topic",
    StartMessageID: pulsar.DeserializeMessageID(lastSavedId),
})
```

#### Use reader to read specific message

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})

if err != nil {
    log.Fatal(err)
}
defer client.Close()

topic := "topic-1"
ctx := context.Background()

// create producer
producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic:           topic,
    DisableBatching: true,
})
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

// send 10 messages
msgIDs := [10]pulsar.MessageID{}
for i := 0; i < 10; i++ {
    msgID, _ := producer.Send(ctx, &pulsar.ProducerMessage{
        Payload: []byte(fmt.Sprintf("hello-%d", i)),
    })
    msgIDs[i] = msgID
}

// create reader on 5th message (not included)
reader, err := client.CreateReader(pulsar.ReaderOptions{
    Topic:                   topic,
    StartMessageID:          msgIDs[4],
    StartMessageIDInclusive: false,
})

if err != nil {
    log.Fatal(err)
}
defer reader.Close()

// receive the remaining 5 messages
for i := 5; i < 10; i++ {
    msg, err := reader.Next(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Read %d-th msg: %s\n", i, string(msg.Payload()))
}
// create reader on 5th message (included)
readerInclusive, err := client.CreateReader(pulsar.ReaderOptions{
    Topic:                   topic,
    StartMessageID:          msgIDs[4],
    StartMessageIDInclusive: true,
})

if err != nil {
    log.Fatal(err)
}
defer readerInclusive.Close()
```

### Reader configuration

All available options of `ReaderOptions` are [here](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#ReaderOptions).

## Messages

The Pulsar Go client provides a `ProducerMessage` interface that you can use to construct messages to producers on Pulsar topics. Here's an example message:

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

if _, err := producer.send(msg); err != nil {
    log.Fatalf("Could not publish message due to: %v", err)
}
```

All methods of `ProducerMessage` object are [here](https://pkg.go.dev/github.com/apache/pulsar-client-go/pulsar#ProducerMessage).

## TLS encryption and authentication

To use [TLS encryption](security-tls-transport.md), you need to configure your client to do so:

 * Use `pulsar+ssl` URL type
 * Set `TLSTrustCertsFilePath` to the path to the TLS certs used by your client and the Pulsar broker
 * Configure `Authentication` option

Here's an example:

```go
opts := pulsar.ClientOptions{
    URL: "pulsar+ssl://my-cluster.com:6651",
    TLSTrustCertsFilePath: "/path/to/certs/my-cert.csr",
    Authentication: pulsar.NewAuthenticationTLS("my-cert.pem", "my-key.pem"),
}
```

## OAuth2 authentication

To use [OAuth2 authentication](security-oauth2.md), you need to configure your client to perform the following operations.

This example shows how to configure OAuth2 authentication.

```go
oauth := pulsar.NewAuthenticationOAuth2(map[string]string{
    "type":       "client_credentials",
    "issuerUrl":  "https://dev-kt-aa9ne.us.auth0.com",
    "audience":   "https://dev-kt-aa9ne.us.auth0.com/api/v2/",
    "privateKey": "/path/to/privateKey",
    "clientId":   "0Xx...Yyxeny",
})
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL:              "pulsar://my-cluster:6650",
    Authentication:   oauth,
})
```


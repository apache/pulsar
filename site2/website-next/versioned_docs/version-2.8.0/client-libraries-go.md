---
id: client-libraries-go
title: Pulsar Go client
sidebar_label: Go
original_id: client-libraries-go
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


> Tips: Currently, the CGo client will be deprecated, if you want to know more about the CGo client, please refer to [CGo client docs](client-libraries-cgo)

You can use Pulsar [Go client](https://github.com/apache/pulsar-client-go) to create Pulsar [producers](#producers), [consumers](#consumers), and [readers](#readers) in Go (aka Golang).

> **API docs available as well**  
> For standard API docs, consult the [Godoc](https://godoc.org/github.com/apache/pulsar-client-go/pulsar).


## Installation

### Install go package

You can install the `pulsar` library locally using `go get`.  

```bash

$ go get -u "github.com/apache/pulsar-client-go/pulsar"

```

Once installed locally, you can import it into your project:

```go

import "github.com/apache/pulsar-client-go/pulsar"

```

## Connection URLs

To connect to Pulsar using client libraries, you need to specify a [Pulsar protocol](developing-binary-protocol) URL.

Pulsar protocol URLs are assigned to specific clusters, use the `pulsar` scheme and have a default port of 6650. Here's an example for `localhost`:

```http

pulsar://localhost:6650

```

If you have multiple brokers, you can set the URL as below.

```
pulsar://localhost:6550,localhost:6651,localhost:6652

```

A URL for a production Pulsar cluster may look something like this:

```http

pulsar://pulsar.us-west.example.com:6650

```

If you're using [TLS](security-tls-authentication) authentication, the URL will look like something like this:

```http

pulsar+ssl://pulsar.us-west.example.com:6651

```

## Create a client

In order to interact with Pulsar, you'll first need a `Client` object. You can create a client object using the `NewClient` function, passing in a `ClientOptions` object (more on configuration [below](#client-configuration)). Here's an example:

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

```
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

The following configurable parameters are available for Pulsar clients:

 Name | Description | Default
| :-------- | :---------- |:---------- |
| URL | Configure the service URL for the Pulsar service.<br /><br />If you have multiple brokers, you can set multiple Pulsar cluster addresses for a client. <br /><br />This parameter is **required**. |None |
| ConnectionTimeout | Timeout for the establishment of a TCP connection | 30s |
| OperationTimeout| Set the operation timeout. Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the operation will be marked as failed| 30s|
| Authentication | Configure the authentication provider. Example: `Authentication: NewAuthenticationTLS("my-cert.pem", "my-key.pem")` | no authentication |
| TLSTrustCertsFilePath | Set the path to the trusted TLS certificate file | |
| TLSAllowInsecureConnection | Configure whether the Pulsar client accept untrusted TLS certificate from broker | false |
| TLSValidateHostname | Configure whether the Pulsar client verify the validity of the host name from broker | false |
| ListenerName | Configure the net model for VPC users to connect to the Pulsar broker |  |
| MaxConnectionsPerBroker | Max number of connections to a single broker that is kept in the pool | 1 |
| CustomMetricsLabels | Add custom labels to all the metrics reported by this client instance |  |
| Logger | Configure the logger used by the client | logrus.StandardLogger |

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

Pulsar Go producers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`Topic()` | Fetches the producer's [topic](reference-terminology.md#topic)| `string`
`Name()` | Fetches the producer's name | `string`
`Send(context.Context, *ProducerMessage)` | Publishes a [message](#messages) to the producer's topic. This call will block until the message is successfully acknowledged by the Pulsar broker, or an error will be thrown if the timeout set using the `SendTimeout` in the producer's [configuration](#producer-configuration) is exceeded. | (MessageID, error)
`SendAsync(context.Context, *ProducerMessage, func(MessageID, *ProducerMessage, error))`| Send a message, this call will be blocking until is successfully acknowledged by the Pulsar broker. | 
`LastSequenceID()` | Get the last sequence id that was published by this producer. his represent either the automatically assigned or custom sequence id (set on the ProducerMessage) that was published and acknowledged by the broker. | int64
`Flush()`| Flush all the messages buffered in the client and wait until all messages have been successfully persisted. | error
`Close()` | Closes the producer and releases all resources allocated to it. If `Close()` is called then no more messages will be accepted from the publisher. This method will block until all pending publish requests have been persisted by Pulsar. If an error is thrown, no pending writes will be retried. | 

### Producer Example

#### How to use message router in producer

```go

client, err := NewClient(pulsar.ClientOptions{
	URL: serviceURL,
})

if err != nil {
	log.Fatal(err)
}
defer client.Close()

// Only subscribe on the specific partition
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
	Topic:            "my-partitioned-topic-partition-2",
	SubscriptionName: "my-sub",
})

if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

producer, err := client.CreateProducer(pulsar.ProducerOptions{
	Topic: "my-partitioned-topic",
	MessageRouter: func(msg *ProducerMessage, tm TopicMetadata) int {
		fmt.Println("Routing message ", msg, " -- Partitions: ", tm.NumPartitions())
		return 2
	},
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

```

```go

var (
	exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
)

```

```go

client, err := NewClient(pulsar.ClientOptions{
	URL: "pulsar://localhost:6650",
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

properties := make(map[string]string)
properties["pulsar"] = "hello"
jsonSchemaWithProperties := NewJSONSchema(exampleSchemaDef, properties)
producer, err := client.CreateProducer(ProducerOptions{
	Topic:  "jsonTopic",
	Schema: jsonSchemaWithProperties,
})
assert.Nil(t, err)

_, err = producer.Send(context.Background(), &ProducerMessage{
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

client, err := NewClient(pulsar.ClientOptions{
	URL: "pulsar://localhost:6650",
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

topicName := newTopicName()
producer, err := client.CreateProducer(pulsar.ProducerOptions{
	Topic: topicName,
})
if err != nil {
	log.Fatal(err)
}
defer producer.Close()

consumer, err := client.Subscribe(pulsar.ConsumerOptions{
	Topic:            topicName,
	SubscriptionName: "subName",
	Type:             Shared,
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

ctx, canc := context.WithTimeout(context.Background(), 1*time.Second)
msg, err := consumer.Receive(ctx)
if err != nil {
	log.Fatal(err)
}
fmt.Println(msg.Payload())
canc()

ctx, canc = context.WithTimeout(context.Background(), 5*time.Second)
msg, err = consumer.Receive(ctx)
if err != nil {
	log.Fatal(err)
}
fmt.Println(msg.Payload())
canc()

```

### Producer configuration

 Name | Description | Default
| :-------- | :---------- |:---------- |
| Topic | Topic specify the topic this consumer will subscribe to. This argument is required when constructing the reader. | |
| Name | Name specify a name for the producer. If not assigned, the system will generate a globally unique name which can be access with Producer.ProducerName(). | | 
| Properties | Properties attach a set of application defined properties to the producer This properties will be visible in the topic stats | |
| SendTimeout | SendTimeout set the timeout for a message that is not acknowledged by the server | 30s |
| DisableBlockIfQueueFull | DisableBlockIfQueueFull control whether Send and SendAsync block if producer's message queue is full | false |
| MaxPendingMessages| MaxPendingMessages set the max size of the queue holding the messages pending to receive an acknowledgment from the broker. | |
| HashingScheme | HashingScheme change the `HashingScheme` used to chose the partition on where to publish a particular message. | JavaStringHash |
| CompressionType | CompressionType set the compression type for the producer. | not compressed | 
| CompressionLevel | Define the desired compression level. Options: Default, Faster and Better | Default  | 
| MessageRouter | MessageRouter set a custom message routing policy by passing an implementation of MessageRouter | |
| DisableBatching | DisableBatching control whether automatic batching of messages is enabled for the producer. | false |
| BatchingMaxPublishDelay | BatchingMaxPublishDelay set the time period within which the messages sent will be batched | 1ms |
| BatchingMaxMessages | BatchingMaxMessages set the maximum number of messages permitted in a batch. | 1000 | 
| BatchingMaxSize | BatchingMaxSize sets the maximum number of bytes permitted in a batch. | 128KB | 
| Schema |  Schema set a custom schema type by passing an implementation of `Schema` | bytes[] | 
| Interceptors | A chain of interceptors. These interceptors are called at some points defined in the `ProducerInterceptor` interface. | None | 
| MaxReconnectToBroker | MaxReconnectToBroker set the maximum retry number of reconnectToBroker | ultimate | 
| BatcherBuilderType | BatcherBuilderType sets the batch builder type. This is used to create a batch container when batching is enabled. Options: DefaultBatchBuilder and KeyBasedBatchBuilder | DefaultBatchBuilder | 

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

Pulsar Go consumers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`Subscription()` | Returns the consumer's subscription name | `string`
`Unsubcribe()` | Unsubscribes the consumer from the assigned topic. Throws an error if the unsubscribe operation is somehow unsuccessful. | `error`
`Receive(context.Context)` | Receives a single message from the topic. This method blocks until a message is available. | `(Message, error)`
`Chan()` | Chan returns a channel from which to consume messages. | `<-chan ConsumerMessage`
`Ack(Message)` | [Acknowledges](reference-terminology.md#acknowledgment-ack) a message to the Pulsar [broker](reference-terminology.md#broker) | 
`AckID(MessageID)` | [Acknowledges](reference-terminology.md#acknowledgment-ack) a message to the Pulsar [broker](reference-terminology.md#broker) by message ID | 
`ReconsumeLater(msg Message, delay time.Duration)` | ReconsumeLater mark a message for redelivery after custom delay | 
`Nack(Message)` | Acknowledge the failure to process a single message. | 
`NackID(MessageID)` | Acknowledge the failure to process a single message. | 
`Seek(msgID MessageID)` | Reset the subscription associated with this consumer to a specific message id. The message id can either be a specific message or represent the first or last messages in the topic. | `error`
`SeekByTime(time time.Time)` | Reset the subscription associated with this consumer to a specific message publish time. | `error`
`Close()` | Closes the consumer, disabling its ability to receive messages from the broker | 
`Name()` | Name returns the name of consumer | `string`

### Receive example

#### How to use regex consumer

```go

client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})

defer client.Close()

p, err := client.CreateProducer(pulsar.ProducerOptions{
	Topic:           topicInRegex,
	DisableBatching: true,
})
if err != nil {
	log.Fatal(err)
}
defer p.Close()

topicsPattern := fmt.Sprintf("persistent://%s/foo.*", namespace)
opts := pulsar.ConsumerOptions{
	TopicsPattern:    topicsPattern,
	SubscriptionName: "regex-sub",
}
consumer, err := client.Subscribe(opts)
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

```

#### How to use multi topics Consumer

```go

func newTopicName() string {
	return fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
}


topic1 := "topic-1"
topic2 := "topic-2"

client, err := NewClient(pulsar.ClientOptions{
	URL: "pulsar://localhost:6650",
})
if err != nil {
	log.Fatal(err)
}
topics := []string{topic1, topic2}
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
	Topics:           topics,
	SubscriptionName: "multi-topic-sub",
})
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

```

#### How to use consumer listener

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

	channel := make(chan pulsar.ConsumerMessage, 100)

	options := pulsar.ConsumerOptions{
		Topic:            "topic-1",
		SubscriptionName: "my-subscription",
		Type:             pulsar.Shared,
	}

	options.MessageChannel = channel

	consumer, err := client.Subscribe(options)
	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	// Receive messages from channel. The channel returns a struct which contains message and the consumer from where
	// the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
	// shared across multiple consumers as well
	for cm := range channel {
		msg := cm.Message
		fmt.Printf("Received message  msgId: %v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		consumer.Ack(msg)
	}
}

```

#### How to use consumer receive timeout

```go

client, err := NewClient(pulsar.ClientOptions{
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
	Type:             Shared,
})
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

msg, err := consumer.Receive(ctx)
fmt.Println(msg.Payload())
if err != nil {
	log.Fatal(err)
}

```

#### How to use schema in consumer

```go

type testJSON struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

```

```go

var (
	exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
)

```

```go

client, err := NewClient(pulsar.ClientOptions{
	URL: "pulsar://localhost:6650",
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

var s testJSON

consumerJS := NewJSONSchema(exampleSchemaDef, nil)
consumer, err := client.Subscribe(ConsumerOptions{
	Topic:                       "jsonTopic",
	SubscriptionName:            "sub-1",
	Schema:                      consumerJS,
	SubscriptionInitialPosition: SubscriptionPositionEarliest,
})
assert.Nil(t, err)
msg, err := consumer.Receive(context.Background())
assert.Nil(t, err)
err = msg.GetSchemaValue(&s)
if err != nil {
	log.Fatal(err)
}

defer consumer.Close()

```

### Consumer configuration

 Name | Description | Default
| :-------- | :---------- |:---------- |
| Topic | Topic specify the topic this consumer will subscribe to. This argument is required when constructing the reader. | |
| Topics | Specify a list of topics this consumer will subscribe on. Either a topic, a list of topics or a topics pattern are required when subscribing| |
| TopicsPattern | Specify a regular expression to subscribe to multiple topics under the same namespace. Either a topic, a list of topics or a topics pattern are required when subscribing | |
| AutoDiscoveryPeriod | Specify the interval in which to poll for new partitions or new topics if using a TopicsPattern. | |
| SubscriptionName | Specify the subscription name for this consumer. This argument is required when subscribing | |
| Name | Set the consumer name | | 
| Properties | Properties attach a set of application defined properties to the producer This properties will be visible in the topic stats | |
| Type | Select the subscription type to be used when subscribing to the topic. | Exclusive |
| SubscriptionInitialPosition | InitialPosition at which the cursor will be set when subscribe | Latest |
| DLQ | Configuration for Dead Letter Queue consumer policy. | no DLQ | 
| MessageChannel | Sets a `MessageChannel` for the consumer. When a message is received, it will be pushed to the channel for consumption | | 
| ReceiverQueueSize | Sets the size of the consumer receive queue. | 1000| 
| NackRedeliveryDelay | The delay after which to redeliver the messages that failed to be processed | 1min |
| ReadCompacted | If enabled, the consumer will read messages from the compacted topic rather than reading the full message backlog of the topic | false |
| ReplicateSubscriptionState | Mark the subscription as replicated to keep it in sync across clusters | false |
| KeySharedPolicy | Configuration for Key Shared consumer policy. |  |
| RetryEnable | Auto retry send messages to default filled DLQPolicy topics | false |
| Interceptors | A chain of interceptors. These interceptors are called at some points defined in the `ConsumerInterceptor` interface. |  |
| MaxReconnectToBroker | MaxReconnectToBroker set the maximum retry number of reconnectToBroker. | ultimate |
| Schema | Schema set a custom schema type by passing an implementation of `Schema` | bytes[] |

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

Pulsar Go readers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`Topic()` | Returns the reader's [topic](reference-terminology.md#topic) | `string`
`Next(context.Context)` | Receives the next message on the topic (analogous to the `Receive` method for [consumers](#consumer-operations)). This method blocks until a message is available. | `(Message, error)`
`HasNext()` | Check if there is any message available to read from the current position| (bool, error)
`Close()` | Closes the reader, disabling its ability to receive messages from the broker | `error`
`Seek(MessageID)` | Reset the subscription associated with this reader to a specific message ID | `error`
`SeekByTime(time time.Time)` | Reset the subscription associated with this reader to a specific message publish time | `error`

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

#### How to use reader to read specific message

```go

client, err := NewClient(pulsar.ClientOptions{
	URL: lookupURL,
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
msgIDs := [10]MessageID{}
for i := 0; i < 10; i++ {
	msgID, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte(fmt.Sprintf("hello-%d", i)),
	})
	assert.NoError(t, err)
	assert.NotNil(t, msgID)
	msgIDs[i] = msgID
}

// create reader on 5th message (not included)
reader, err := client.CreateReader(pulsar.ReaderOptions{
	Topic:          topic,
	StartMessageID: msgIDs[4],
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

 Name | Description | Default
| :-------- | :---------- |:---------- |
| Topic | Topic specify the topic this consumer will subscribe to. This argument is required when constructing the reader. | |
| Name | Name set the reader name. | | 
| Properties | Attach a set of application defined properties to the reader. This properties will be visible in the topic stats | |
| StartMessageID | StartMessageID initial reader positioning is done by specifying a message id. | |
| StartMessageIDInclusive | If true, the reader will start at the `StartMessageID`, included. Default is `false` and the reader will start from the "next" message | false |
| MessageChannel | MessageChannel sets a `MessageChannel` for the consumer When a message is received, it will be pushed to the channel for consumption| |
| ReceiverQueueSize | ReceiverQueueSize sets the size of the consumer receive queue. | 1000 |
| SubscriptionRolePrefix| SubscriptionRolePrefix set the subscription role prefix. | “reader” | 
| ReadCompacted | If enabled, the reader will read messages from the compacted topic rather than reading the full message backlog of the topic.  ReadCompacted can only be enabled when reading from a persistent topic. | false|

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

if _, err := producer.send(msg); err != nil {
    log.Fatalf("Could not publish message due to: %v", err)
}

```

The following methods parameters are available for `ProducerMessage` objects:

Parameter | Description
:---------|:-----------
`Payload` | The actual data payload of the message
`Value` | Value and payload is mutually exclusive, `Value interface{}` for schema message.
`Key` | The optional key associated with the message (particularly useful for things like topic compaction)
`OrderingKey` | OrderingKey sets the ordering key of the message.
`Properties` | A key-value map (both keys and values must be strings) for any application-specific metadata attached to the message
`EventTime` | The timestamp associated with the message
`ReplicationClusters` | The clusters to which this message will be replicated. Pulsar brokers handle message replication automatically; you should only change this setting if you want to override the broker default.
`SequenceID` | Set the sequence id to assign to the current message
`DeliverAfter` | Request to deliver the message only after the specified relative delay
`DeliverAt` | Deliver the message only at or after the specified absolute timestamp

## TLS encryption and authentication

In order to use [TLS encryption](security-tls-transport), you'll need to configure your client to do so:

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

## OAuth2 authentication

To use [OAuth2 authentication](security-oauth2), you'll need to configure your client to perform the following operations.
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

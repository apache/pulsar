---
id: client-libraries-c#
title: Pulsar C# client
sidebar_label: C#
---

You can use the Pulsar C# client to create Pulsar producers and consumers in C#. All the methods in the producer, consumer, and reader of a C# client are thread-safe.

## Installation

## Client

This section describes some configuration examples for the Pulsar C# client.

### Create consumer

This section describes how to create a consumer.

The following table lists options available for creating a consumer.

| Option | Description | Required or not | Default |
|---- | ---- | ---- | ---- |
| ConsumerName | Set the consumer name. | Optional | N/A |
| InitialPosition | Set the initial position for the subscription. | Optional | `Latest'`|
| PriorityLevel | Set the priority level for the shared subscription consumer. | Optional |  0 |
| MessagePrefetchCount | Set the number of messages that are pre-fetched. | Optional | 1000 |
| ReadCompacted | Configure whether to read messages from the compacted topic. | Optional | `false` |
| SubscriptionName | Set the subscription name for the consumer. | Required | N/A |
| SubscriptionType  | Set the subscription type for the consumer. | Optional | `Exclusive` |
| Topic | Set the topic for this consumer. | Required | N/A |

- Create a consumer with builder.

    This example shows how to create a consumer using the builder.

    ```c#
    var consumer = client.NewConsumer()
                        .SubscriptionName("MySubscription")
                        .Topic("persistent://public/default/mytopic")
                        .Create();
    ```

- Create a consumer without builder.

    This example shows how to create a consumer without the builder.

    ```c#
    var options = new ConsumerOptions("MySubscription", "persistent://public/default/mytopic");
    var consumer = client.CreateConsumer(options);
    ```

### Create reader

This section describes how to create a reader.

The following table lists options available for creating a reader.

| Option | Description | Required or not | Default |
|---- | ---- | ---- | ---- |
| ReaderName | Set the reader name. | Optional | N/A |
| MessagePrefetchCount | Set the number of messages that are pre-fetched. | Optional |  1000 |
| ReadCompacted | Configure whether to read messages from the compacted topic. | Optional | `false` |
| StartMessageId | The initial reader position is set to the specified message ID. | Required | N/A |
| Topic | Set the topic for this reader. | Required | N/A |

- Create a reader with builder.

    This example shows how to create a reader using the builder.

    ```c#
    var reader = client.NewReader()
                    .StartMessageId(MessageId.Earliest)
                    .Topic("persistent://public/default/mytopic")
                    .Create();
    ```

- Create a reader without builder.

    This example shows how to create a reader without the builder.

    ```c#
    var options = new ReaderOptions(MessageId.Earliest, "persistent://public/default/mytopic");
    var reader = client.CreateReader(options);
    ```

## Producer

## Consumer

A consumer is a process that attaches to a topic through a subscription and then receives messages. This section describes some configuration examples about the consumer.

### Consume messages

This example shows how to consume messages from a topic.

```c#
await foreach (var message in consumer.Messages())
{
    Console.WriteLine("Received: " + Encoding.UTF8.GetString(message.Data.ToArray()));
}
```

### Acknowledge messages

Messages can be acknowledged individually or cumulatively. For details about message acknowledgement, see [acknowledgement](concepts-messaging.md#acknowledgement).

- Acknowledge messages individually.

```c#
await foreach (var message in consumer.Messages())
{
    Console.WriteLine("Received: " + Encoding.UTF8.GetString(message.Data.ToArray()));
}
```

- Acknowledge messages cumulatively.

    ```c#
    await consumer.AcknowledgeCumulative(message);
    ```

### Unsubscribe from topics

This example shows how a consumer unsubscribe from a topic.

```c#
await consumer.Unsubscribe();
```

#### Note

> A consumer cannot be used and is disposed once the consumer unsubscribes from a topic.

### Monitor consumer state

## Reader

A reader is actually just a consumer without a cursor. This means that Pulsar does not keep track of your progress and there is no need to acknowledge messages. This section describes some configuration examples about the reader.

### Receive messages

This example shows how a reader receives messages.

```c#
await foreach (var message in reader.Messages())
{
    Console.WriteLine("Received: " + Encoding.UTF8.GetString(message.Data.ToArray()));
}
```

### Monitor reader state

The following table lists state available for the reader.

| State | Description |
| ---- | ----|
| Closed | The reader or the Pulsar client has been disposed. |
| Connected | All is well. |
| Disconnected | The connection is lost and attempts are being made to reconnect.
| Faulted | An unrecoverable error has occurred. |
| ReachedEndOfTopic | No more messages are delivered. |

This example shows how to monitor the reader state.

```c#
private static async ValueTask Monitor(IReader reader, CancellationToken cancellationToken)
{
    var state = ReaderState.Disconnected;

    while (!cancellationToken.IsCancellationRequested)
    {
        state = await reader.StateChangedFrom(state, cancellationToken);

        var stateMessage = state switch
        {
            ReaderState.Connected => "The reader is connected",
            ReaderState.Disconnected => "The reader is disconnected",
            ReaderState.Closed => "The reader has closed",
            ReaderState.ReachedEndOfTopic => "The reader has reached end of topic",
            ReaderState.Faulted => "The reader has faulted",
            _ => $"The reader has an unknown state '{state}'"
        };

        Console.WriteLine(stateMessage);

        if (reader.IsFinalState(state))
            return;
    }
}
```
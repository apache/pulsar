# PIP-38: Batch Receiving Messages

- Status:
- Author: Penghui Li
- Discussion Thread:
- Issue:
- Pull Request: https://github.com/apache/pulsar/pull/4621

## Motivation

Batch processing is commonly used to improve throughput, support batch receiving in client can be better adapted to user's existing batch operations(batch insert data to database or other bulk APIs). At present, pulsar client provides the ability to receive a single message. If users want to take advantage of batch operating advantages, need to implement a message collector himself. 

For throughput optimization in the future will benefit from batch receiving , it can allow lazy deserialization and object creation, can also reduce `incomingMessages` enqueue and dequeue times 

So this proposal aims to provide a universal interface and mechanism for batch receiving messages.

## Requirements

Batch receiving should have the following capabilities:

- Multiple messages can be received at a time
- Users can set the max number or size of messages received in batches for consumers
- Provide a timeout mechanism to avoid waiting indefinitely

## Usage

Users can get multiple messages in the following way:

```java
Messages messages = consumer.batchReceive();
// execute user's batch operation
consumer.acknowledge(messages);
```

Users can set batch receive policy in the following way:

```java
Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s1")
                .batchReceivePolicy(BatchReceivePolicy.builder()
                        .maxNumberOfMessages(10)
                        .build())
                .subscribe();
```

Batch receive policy can meet multiple use cases:

**Fixed number of messages**

Consumer will be blocked until has enough number of messages available.

```java
BatchReceivePolicy.builder().maxNumMessages(10).build();
```

**Fixed bytes of messages**

Consumer will be blocked until has enough bytes of messages available.

```java
BatchReceivePolicy.builder().maxNumBytes(1024 * 1024 * 10).build();
```

> Note:
>
> The semantics here represent the max message bytes, can less than the max bytes in a messages batch.

**Fixed time period**

Get messages by a fixed time period.

```java
BatchReceivePolicy.builder().timeout(1, TimeUnit.SECONDS).build();
```

> Note:
>
> This way will not limit the number or bytes of a message batch, ensure have enough memory resources to maintain the messages batch in a single time period.

**Hybrid control**

Messages batch controlled by multiple conditions. Any one condition is met, messages batch will complete immediately.

```java
BatchReceivePolicy.builder()
    .maxNumMessages(10)
    .maxNumBytes(1024 * 1024 * 10)
    .timeout(10, TimeUnit.SECONDS)
    .build();
```

## Changes

**Add new Messages interface**

```java
/**
 * A container that holds the list {@link Message} for a topic.
 */
public interface Messages<T> extends Iterable<Message<T>> {
     /**
     * Get the list {@link Message}
     */
    List<Message<T>> getMessageList();

     /**
     * Get number of messages.
     */
    int size();
}
```

**Add new methods to Consumer API**

```java
/**
 * Batch receiving messages
 * <p>
 * This calls blocks until has enough messages or wait timeout, more details to see {@link BatchReceivePolicy}
 
 * @return messages
 * @since 2.5.0
 * @throws PulsarClientException
 */
Messages<T> batchReceive() throws PulsarClientException;

/**
 * Batch receiving messages
 * <p>
 * Retrieves messages when has enough messages or wait timeout and
 * completes {@link CompletableFuture} with received messages.
 * </p>
 * <p>
 * {@code batchReceiveAsync()} should be called subsequently once returned {@code CompletableFuture} gets complete with
 * received messages. Else it creates <i> backlog of receive requests </i> in the application.
 * </p>
 * @return messages
 * @since 2.5.0
 * @throws PulsarClientException
 */
CompletableFuture<Messages<T>> batchReceiveAsync();

/**
 * Acknowledge the consumption of {@link Messages}
 
 * @param messages messages
 * @throws PulsarClientException.AlreadyClosedException
 *              if the consumer was already closed
 */
void acknowledge(Messages<?> messages) throws PulsarClientException;

/**
 * Acknowledge the failure to process {@link Messages}
 * <p>
 * When messages is "negatively acked" it will be marked for redelivery after
 * some fixed delay. The delay is configurable when constructing the consumer
 * with {@link ConsumerBuilder#negativeAckRedeliveryDelay(long, TimeUnit)}.
 * <p>
 * This call is not blocking.
 
 * <p>
 * Example of usage:
 * <pre><code>
 * while (true) {
 *     Messages&lt;String&gt; msgs = consumer.batchReceive();
 
 *     try {
 *          // Process message...
 
 *          consumer.acknowledge(msgs);
 *     } catch (Throwable t) {
 *          log.warn("Failed to process message");
 *          consumer.negativeAcknowledge(msgs);
 *     }
 * }
 * </code></pre>
 
 * @param message
 *            The {@code Message} to be acknowledged
 */
void negativeAcknowledge(Messages<?> messages);
```

**Add BatchReceivePolicy**

```java
/**
 * Configuration for message batch receive {@link Consumer#batchReceive()} {@link Consumer#batchReceiveAsync()}.
 
 * Batch receive policy can limit the number and size of messages in a single batch, and can specify a timeout
 * for waiting for enough messages for this batch.
 
 * This batch receive will be completed as long as any one of the
 * conditions(has enough number of messages, has enough of size of messages, wait timeout) is met.
 
 * Examples:
 
 * 1.If set maxNumberOfMessages = 10, maxSizeOfMessages = 1MB and without timeout, it
 * means {@link Consumer#batchReceive()} will always wait until there is enough messages.
 
 * 2.If set maxNumberOfMessages = 0, maxSizeOfMessages = 0 and timeout = 100ms, it
 * means {@link Consumer#batchReceive()} will wait for 100ms whether or not there is enough messages.
 
 * Note:
 * Must specify messages limitation(maxNumberOfMessages, maxSizeOfMessages) or wait timeout.
 * Otherwise, {@link Messages} ingest {@link Message} will never end.
 
 * @since 2.5.0
 */
public class BatchReceivePolicy {
    /**
     * Default batch receive policy
     
     * Max number of messages: 100
     * Max size of messages: 10MB
     * Timeout: 100ms
     */
    public static final BatchReceivePolicy DEFAULT_POLICY = new BatchReceivePolicy(
        100, 1024 * 1024 * 10, 100, TimeUnit.MILLISECONDS);
  
    /**
     * Max number of message for a single batch receive, 0 or negative means no limit.
     */
    private int maxNumMessages;
  
    /**
     * Max size of message for a single batch receive, 0 or negative means no limit.
     */
    private int maxNumBytes;
  
  	/**
     * timeout for waiting for enough messages(enough number or enough size).
     */
    private int timeout;
  	private TimeUnit timeoutUnit;
}
```

## Some future work

Additionally it might be good to have a few more followup changes to optimize this further.

The current pulsar client breaks a message batch to individual messages and collect multiple message into a `Messages`. There is a lot of unuseful object conversations.

Ideally the pulsar client implementation should

1. Keep a queue of `Messages`. Each `Messages` is a message batch or multiple message batches.
2. On receiving individual message, it polls a `Messages` from the queue, and poll a message out of the `Messages`.

This can allow lazy deserialization and object creation, and it will increase the throughput using batch receive api because your cpu cycles can be reduced.

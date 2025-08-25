# PIP-96: Message payload processor for Pulsar client

**Status**: Approved

**Author**: Yunze Xu

**Pull Request**: https://github.com/apache/pulsar/pull/12088 

**Mailing List discussion**: 
https://lists.apache.org/thread.html/rb4df052fdea1d6791ecdebdd09bb4cbc27ffec307ad678afb1157c6c%40%3Cdev.pulsar.apache.org%3E

**Release**: 2.9.0

## Motivation

The initial motivation was from Kafka's protocol handler, i.e. KoP (https://github.com/streamnative/kop). KoP allows Kafka producer to configure entry format to `kafka`, which means the message from Kafka producer can be written to bookies directly without any conversion between Kafka's message format and Pulsar's message format. This improves the performance significantly. However, it also introduced the limit that Pulsar consumer cannot consume this topic because it cannot recognize the entry's format.

The existing `ConsumerInterceptor` cannot work for this case because it's based on a `Message<T>` object that is converted from an entry with valid Pulsar format. 

## Goal

This proposal tries to introduce a payload processor for Pulsar client, which performs a conversion for payload (without metadata parts) at client side. The processor is responsible to transfrom a raw buffer to some messages, then triggers some callbacks so that consumers can consume these messages or handle the error.

This proposal is mainly for protocol handlers because they can access `PersistentTopic` and write bytes to bookies directly. In a rare case, if users want to write something to the topic's ledger directly by BookKeeper client, the payload processor can also handle the case.

## API Changes

Since we cannot add any dependency to the pulsar-client-api module to avoid cyclic dependency while we need to access `BrokerEntryMetadata`, `MessageMetadata` and `ByteBuf`, this proposal adds following two interfaces.

```java
/**
 * The context of an entry, which usually represents a message of a batch if batching is enabled.
 */
public interface EntryContext {

    /**
     * Get a value associated with the given key.
     *
     * @param key
     * @return the value associated with the key or null if the key or value doesn't exist
     */
    String getProperty(String key);

    /**
     * Get the number of messages.
     *
     * Since the message could be batched, a message could have multiple internal single messages.
     *
     * @return the number of internal single messages or 1 if the message is not batched.
     */
    int getNumMessages();

    /**
     * Check whether the entry is a batch.
     *
     * @return true if the entry is a batch.
     */
    boolean isBatch();

    /**
     * Get the internal single message with a specific index from a payload if the entry is a batch.
     *
     * @param index the batch index
     * @param numMessages the number of messages in the batch
     * @param payload the message payload
     * @param containMetadata whether the payload contains the single message metadata
     * @param schema the schema of the batch
     * @param <T>
     * @return the single message
     * @implNote The `index` and `numMessages` parameters are used to create the message id with batch index.
     *   If `containMetadata` is true, parse the single message metadata from the payload first. The fields of single
     *   message metadata will overwrite the same fields of the entry's metadata.
     */
    <T> Message<T> getMessageAt(int index,
                                int numMessages,
                                MessagePayload payload,
                                boolean containMetadata,
                                Schema<T> schema);

    /**
     * Convert the given payload to a single message if the entry is non-batched.
     *
     * @param payload the message payload
     * @param schema the schema of the message
     * @param <T>
     * @return the converted single message
     */
    <T> Message<T> asSingleMessage(MessagePayload payload, Schema<T> schema);
}
```

```java
/**
 * The abstraction of a message's payload.
 */
public interface MessagePayload {

    /**
     * Copy the bytes of the payload into the byte array.
     *
     * @return the byte array that is filled with the readable bytes of the payload, it should not be null
     */
    byte[] copiedBuffer();

    /**
     * Release the resources if necessary.
     *
     * NOTE: For a MessagePayload object that is created from {@link MessagePayloadFactory#DEFAULT}, this method must be
     * called to avoid memory leak.
     */
    default void release() {
        // No ops
    }
}
```

- `EntryContext` is the abstraction of metadata and some resources of `ConsumerImpl`. `getProperty()`  is used to check the format tag. `getMessageAt()` and `asSingleMessage()` are used to create messages baed on a `MessagePayload`.

  > It should be noted that `getNumMessages()` and `isBatch()` only retrieve metadata from the `MessageMetadata` (see `PulsarApi.proto`). They should only be used for the default Pulsar format. Because in other Entry format, the actual metadata could be recorded inside the payload.

- `MessagePayload` is the abstraction of a payload buffer. The only usage for user is being passed to `EntryContext`'s methods to create `Message<T>`, which uses a Netty `ByteBuf` as the storage. Since users might pass a custom defined `MessagePayload`, it provides a `copiedBuffer()` method to copy bytes to a Netty `ByteBuf`.

  > We add this class mainly because **pulsar-client-api** module should not rely on Netty. If users still want to operate on the internal `ByteBuf`, see the **Implementation** section of this proposal.

In addition, a factory class is added for user to use the built-in `MessagePayload` implementation.

```java
public interface MessagePayloadFactory {

    MessagePayloadFactory DEFAULT = DefaultImplementation.getDefaultImplementation().newDefaultMessagePayloadFactory();

    /**
     * Create a payload whose underlying buffer refers to a byte array.
     *
     * @param bytes the byte array
     * @return the created MessagePayload object
     */
    MessagePayload wrap(byte[] bytes);

    /**
     * Create a payload whose underlying buffer refers to a NIO buffer.
     *
     * @param buffer the NIO buffer
     * @return the created MessagePayload object
     */
    MessagePayload wrap(ByteBuffer buffer);
}
```

The built-in `MessagePayload` implementation is backed by a Netty `ByteBuf`. So when it's converted to `ByteBuf`, no bytes copy will happen.

The core interface `PayloadProcessor` is based on the these two interfaces:

```java
/**
 * The processor to process a message payload.
 *
 * It's responsible to convert the raw buffer to some messages, then trigger some callbacks so that consumer can consume
 * these messages and handle the exception if it existed.
 *
 * The most important part is to decode the raw buffer. After that, we can call {@link EntryContext#getMessageAt} or
 * {@link EntryContext#asSingleMessage} to construct {@link Message} for consumer to consume. Since we need to pass the
 * {@link MessagePayload} object to these methods, we can use {@link MessagePayloadFactory#DEFAULT} to create it or just
 * reuse the payload argument.
 */
public interface PayloadProcessor {

    /**
     * Process the message payload.
     *
     * @param payload the payload whose underlying buffer is a Netty ByteBuf
     * @param context the message context that contains the message format information and methods to create a message
     * @param schema the message's schema
     * @param messageConsumer the callback to consume each message
     * @param <T>
     * @throws Exception
     */
    <T> void process(MessagePayload payload,
                     EntryContext context,
                     Schema<T> schema,
                     Consumer<Message<T>> messageConsumer) throws Exception;

    // The default processor for Pulsar format payload. It should be noted getNumMessages() and isBatch() methods of
    // EntryContext only work for Pulsar format. For other formats, the message metadata might be stored in the payload.
    PayloadProcessor DEFAULT = new PayloadProcessor() {

        @Override
        public <T> void process(MessagePayload payload,
                                EntryContext context,
                                Schema<T> schema,
                                Consumer<Message<T>> messageConsumer) {
            if (context.isBatch()) {
                final int numMessages = context.getNumMessages();
                for (int i = 0; i < numMessages; i++) {
                    messageConsumer.accept(context.getMessageAt(i, numMessages, payload, true, schema));
                }
            } else {
                messageConsumer.accept(context.asSingleMessage(payload, schema));
            }
        }
    };
}
```

Finally, we can configure our processor in `ConsumerBuilder`:

```java
    /**
     * If it's configured with a non-null value, the consumer will use the processor to process the payload, including
     * decoding it to messages and triggering the listener.
     *
     * Default: null
     */
    ConsumerBuilder<T> payloadProcessor(PayloadProcessor payloadProcessor);
```

## Implementation

For `EntryContext` and `MessagePayload` in **pulsar-client-api** module, there're the corresponding implementation classes `EntryContextImpl` and `MessagePayloadImpl`.

- `EntryContextImpl` holds references to metadata and consumer so that it can use `ConsumerImpl`'s methods to create a non-batched message or a single message of a batch using a `MessagePayload`.
- `MessagePayloadImpl` is just a wrapper of Netty `ByteBuf`.

We provided a util method `MessagePayloadUtils#convertToByteBuf` to convert `MessagePayload` to `ByteBuf`. It's simple so I just show the code.

```java
    public static ByteBuf convertToByteBuf(final MessagePayload payload) {
        if (payload instanceof MessagePayloadImpl) {
            return ((MessagePayloadImpl) payload).getByteBuf().retain();
        } else {
            return Unpooled.wrappedBuffer(payload.copiedBuffer());
        }
    }
```

Therefore, we recommend users to use `MessagePayloadFactory.DEFAULT` to create a `MessagePayloadImpl` to avoid coping bytes. Combining with `convertToByteBuf`, users could not use `MessagePayloadImpl` directly.
1. Use `MessagePayloadFactory.DEFAULT` to create a `MessagePayload` as the argument of `EntryContext`'s method.
2. Use `convertToByteBuf` to retrieve a Netty `ByteBuf` for read operations.

> I tried to wrap `ByteBuf`'s methods into `MessagePayload`. But it made code too huge. Otherwise it's still not flexible as `ByteBuf`'s original methods.

`ConsumerImpl` is refactored just to remove the repeated code and provide `newSingleMessage`/`newMessage` methods for `EntryContextImpl` to reuse.

By default, if no payload processor is configured, `ConsumerImpl` will still go through the original process. But if a processor is configured, the original process will be skipped and the process will process the payload instead.

The following tests should be added:

1. Unit tests for `MessagePayloadUtils#convertToByteBuf`.
2. Tests for `PayloadProcessor#DEFAULT`. It should cover the different scenarios, like one topic vs multiple topics, batching vs non-batching, batch size 1 vs N.
3. A custom process that decodes a specific format of message into messages that Pulsar consumer can recognize. The raw bytes are published by `PersistentTopic` directly.

## Reject Alternatives

There was [a proposal](https://github.com/apache/pulsar/issues/11962) that was discarded before. It tried to perform the conversion at the broker side before dispatching entries. However, the dispatcher run in a single thread when dispatching entries. The conversion cost might be affect all other topics.

Therefore, we should do it at the client side. It could bring overhead for Pulsar consumer but doesn't affect other topics.

Also, this proposal tried to load converter before, but it will be hard to maintain the compatibility.

This proposal has used the payload converter that converts a payload to `Iterable<Message<T>>` before, but the implementation is complex and has some flaws.

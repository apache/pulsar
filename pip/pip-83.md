# PIP-83: Pulsar client: Message consumption with pooled buffer

* **Status**: Implemented
* **Authors**: Matteo Merli, Rajan Dhabalia
* **Pull Request**: https://github.com/apache/pulsar/pull/10184
* **Mailing List discussion**:
* **Release**:

## Motivation

Pulsar provides a high-performance client library that keeps messages in a pooled buffer while sending and receiving messages from the broker. On the publish path, the java client already supports message-publish with zero-copy.  It allows the application to create message payload in pooled buffer (direct memory) and publish the same message to the broker from the same allocated pooled buffer. 

On the consuming side,  Pulsar java client library receives messages from the broker , copies payload to an unpooled buffer and hands over copied messages to the application. Later on, allocated unpooled buffer memory for that message will be claimed back by the garbage collector in the application.

The primary reason for the current behavior was to provide a “safe” API, in which an application misuse of the API would not cause a memory leak.

At the same time, a very high allocation rate can be causing a lot of work for the JVM GC and that will have an impact on the consumer performance. So, sometimes consumer applications need a mechanism to access messages directly from a pooled buffer with minimal object allocation, to avoid GC impact in application performance. However, one of the caveats of allowing applications to access messages backed by pooled buffers will be managing the message life cycle and making sure the application deallocates pooled buffers of the message after it is done with the message processing. 

Pulsar client library should provide support for an API to allow the application to access message payload from pooled buffers.  The library has to also provide an associated release API,  to release and deallocate pooled buffers used by the message.

## Client-side changes

### Consumer configuration
Pulsar consumer configuration will provide a configuration `poolMessages` to create a consumer which avoids copying payload to unpooled buffer. The  application is then expected to access the message and explicitly release the message back to the pool.

### Consumer API
A message payload backed by a pooled buffer can be accessed by ByteBuffer. A consumer with ByteBuffer schema-type can be used by application to get message payload as ByteBuffer. Therefore, Consumer API will not require any change,  and the existing API can be used to access message payload from the pooled buffer.


```

interface ConsumerBuilder {
  /// ….

  /**
  * Enable pooling of messages and the underlying data buffers.
  * When pooling is enabled, the application is responsible for
  * calling Message.release() after the handling of every 
  * received message.
  * If “release()” is not called on a received message, there will be a memory leak. 
  * If an application attempts to use and already “released” 
  * message, it might experience undefined behavior (eg: memory corruption, deserialization error, etc.).
  * */
  ConsumerBuilder poolMessages(boolean poolMessages);
}
```

**Consumer API**

```
Consumer<ByteBuffer> consumer = client.newConsumer(Schema.BYTEBUFFER)                       
                                 .poolMessages(true)
                                 .subscriptionName(subscriptionName)topic(topicName)
                                 .subscribe();

ByteBuffer payload = consumer.receive().getValue();
```

### Message API
Once the application consumes the message and accesses the message-payload which is backed by a pooled buffer, the application should be able to release the message and deallocate the buffer. Therefore, the Message will have a Release API that should be used by the application to release the message and deallocate the memory.

```

interface Message {
  /// ….

  /**
  * Release a message back to the pool. 
  * This is required only if the consumer was created with the option to pool messages, otherwise it will have no effect.
  * */
   void release();
}

```

`MessageImpl` can be also recyclable to optimize message object allocation.

`Message::getData` and `Message::getValue` api must be still thread-safe and enabling `poolMessages` should not change the api behavior.

## Broker changes
This feature requires only client-side changes and it doesn’t require any broker changes.

## Application usage

```
Consumer<ByteBuffer> consumer = client.newConsumer(Schema.BYTEBUFFER)                       
                 .poolMessages(true)
                 .subscriptionName(subscriptionName)topic(topicName)
                 .subscribe();

Message<ByteBuffer> msg = consumer.receive();
try {
    ByteBuffer value = msg.getValue();
    processMessage(value);
} finally {
    msg.release();
}
```

## Notes

### Close method
An alternative to the proposed “release()” would be to make `Message` to extend `Closeable` Java interface. The downside of this option is that it would trigger warnings in existing consumers code indicating that applications are not properly closing the messages, while in the great majority of use cases that would not be necessary.

### Reference Counting
The current proposal only includes a “release()” method but doesn’t add a full reference counting mechanism to the Pulsar Message interface. Effectively, a message can only have a ref count of 1 and will be freed once release is called once.

This is done to keep the API as simple as possible. In future, if a concrete need arises, we could expand that into supporting proper ref-count with a `retain()` method.

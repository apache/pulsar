# PIP-23: Message Tracing By Interceptors

- **Status**: Done
- **Author**: [Penghui Li](https://github.com/codelipenghui), [Jia Zhai](https://github.com/zhaijack)
- **Pull Request**: [#2471](https://github.com/apache/incubator-pulsar/pull/2471)
- **Mailing List discussion**: 
- **Release**: 2.2.0

## Requirement & Overview

This is related to issue #2280. 
And for Pulsar itself, That will be useful also to track performance issues and understand more of the latency breakdowns. Tracing is the easiest way to do: "show me a request with high-latency and show where the latency (for this particular request) was coming from.

## GOALS

A good approach to that, is to integrate a tracing standard, either OpenTracing or OpenCensus, at both bookkeeper and pulsar. so their applications can integrate with OpenTracing or OpenCensus as well,  then application's traceId, spanId can be passed all way from producer to broker to bookie and to consumer. Then those traces can be collected and fed into an ElasticSearch cluster for query.

## IMPLEMENTATION

Integration with a tracing system typically requires tight integration with applications. Especially if Pulsar is used in a data pipeline, Pulsar should be able to provide mechanism to be able to propagate trace context end-to-end. That says Pulsar doesn’t have to integrate with any tracing framework directly, but it has to provide mechanism for applications to do so. This can be done by:

- Use `properties` in message header for propagating trace context.
- Provide an interface to intercept message pipeline in Pulsar, which allows applications customize their own tracing behaviors and meet their requirements.

This PIP introduces **Interceptor** to examine (and potentially modify) messages at key places during the lifecycle of a Pulsar message. The interceptors include `ProducerInterceptor` for intercepting messages at Producer side, and `ConsumerInterceptor` for intercepting messages at Consumer side.

### Producer Interceptor

The ProducerInterceptor intercept messages before sending them and on receiving acknowledgement from brokers. The interfaces for a ProducerInterceptor are defined as below:

```java
package org.apache.pulsar.client.api;

/**
 * A plugin interface that allows you to intercept (and possibly mutate) the 
 * messages received by the producer before they are published to the Pulsar   
 * brokers.
 * <p>
 * Exceptions thrown by ProducerInterceptor methods will be caught, logged, but  
 * not propagated further.
 * <p>
 * ProducerInterceptor callbacks may be called from multiple threads. Interceptor  
 * implementation must ensure thread-safety, if needed.
 */
public interface ProducerInterceptor<T> extends AutoCloseable {

    /**
     * Close the interceptor.
     */
    void close();

    /**
     * This is called from {@link Producer#send(Object)} and {@link  
     * Producer#sendAsync(Object)} methods, before
     * send the message to the brokers. This method is allowed to modify the 
     * record, in which case, the new record
     * will be returned.
     * <p>
     * Any exception thrown by this method will be caught by the caller and 
     * logged, but not propagated further.
     * <p>
     * Since the producer may run multiple interceptors, a particular 
     * interceptor's {@link #beforeSend(Message)} callback will be called in the   
     * order specified by 
     * {@link ProducerBuilder#intercept(ProducerInterceptor[])}.
     * <p>
     * The first interceptor in the list gets the message passed from the client, 
     * the following interceptor will be passed the message returned by the   
     * previous interceptor, and so on. Since interceptors are allowed to modify
     * messages, interceptors may potentially get the message already modified by   
     * other interceptors. However, building a pipeline of mutable interceptors   
     * that depend on the output of the previous interceptor is discouraged, 
     * because of potential side-effects caused by interceptors potentially   
     * failing to modify the message and throwing an exception. If one of the 
     * interceptors in the list throws an exception from   
     * {@link#beforeSend(Message)}, the exception is caught, logged, and the next 
     * interceptor is called with the message returned by the last successful   
     * interceptor in the list, or otherwise the client.
     *
     * @param message message to send
     * @return the intercepted message
     */
    Message<T> beforeSend(Message<T> message);

    /**
     * This method is called when the message sent to the broker has been 
     * acknowledged, or when sending the message fails. 
     * This method is generally called just before the user callback is 
     * called, and in additional cases when an exception on the producer side.
     * <p>
     * Any exception thrown by this method will be ignored by the caller.
     * <p>
     * This method will generally execute in the background I/O thread, so the   
     * implementation should be reasonably fast. Otherwise, sending of messages    
     * from other threads could be delayed.
     *
     * @param message the message that application sends
     * @param msgId the message id that assigned by the broker; null if send failed.
     * @param cause the exception on sending messages, null indicates send has succeed.
     */
    void onSendAcknowledgement(Message<T> message, MessageId msgId, Throwable cause);

}
```

### Consumer Interceptor

The ConsumerInterceptor intercept messages before delivering to consumers and before sending acknowledgements to brokers. The interfaces for a ConsumerInterceptor are defined as below:

```java
package org.apache.pulsar.client.api;

/**
 * A plugin interface that allows you to intercept (and possibly mutate)
 * messages received by the consumer.
 *
 * <p>A primary use case is to hook into consumer applications for custom
 * monitoring, logging, etc.
 *
 * <p>Exceptions thrown by interceptor methods will be caught, logged, but
 * not propagated further.
 */
public interface ConsumerInterceptor<T> extends AutoCloseable {

    /**
     * Close the interceptor.
     */
    void close();

    /**
     * This is called just before the message is returned by
     * {@link Consumer#receive()}, {@link MessageListener#received(Consumer, 
     * Message)} or the {@link java.util.concurrent.CompletableFuture} returned by
     * {@link Consumer#receiveAsync()} completes.
     * <p>
     * This method is allowed to modify message, in which case the new message
     * will be returned.
     * <p>
     * Any exception thrown by this method will be caught by the caller, logged,
     * but not propagated to client.
     * <p>
     * Since the consumer may run multiple interceptors, a particular 
     * interceptor's
     * <tt>beforeConsume</tt> callback will be called in the order specified by
     * {@link ConsumerBuilder#intercept(ConsumerInterceptor[])}. The first 
     * interceptor in the list gets the consumed message, the following 
     * interceptor will be passed
     * the message returned by the previous interceptor, and so on. Since 
     * interceptors are allowed to modify message, interceptors may potentially 
     * get the messages already modified by other interceptors. However building a   
     * pipeline of mutable
     * interceptors that depend on the output of the previous interceptor is   
     * discouraged, because of potential side-effects caused by interceptors 
     * potentially failing to modify the message and throwing an exception. 
     * if one of interceptors in the list throws an exception from 
     * <tt>beforeConsume</tt>, the exception is caught, logged,
     * and the next interceptor is called with the message returned by the last 
     * successful interceptor in the list, or otherwise the original consumed 
     * message.
     *
     * @param message the message to be consumed by the client.
     * @return message that is either modified by the interceptor or same message
     *         passed into the method.
     */
    Message<T> beforeConsume(Message<T> message);

    /**
     * This is called consumer sends the acknowledgment to the broker.
     *
     * <p>Any exception thrown by this method will be ignored by the caller.
     *
     * @param message message to ack, null if acknowledge fail.
     * @param cause the exception on acknowledge.
     */
    void onAcknowledge(Message<T> message, Throwable cause);

    /**
     * This is called consumer send the cumulative acknowledgment to the broker.
     *
     * <p>Any exception thrown by this method will be ignored by the caller.
     *
     * @param messages messages to ack cumulative, null if acknowledge fail.
     * @param cause the exception on acknowledge.
     */
    void onAcknowledgeCumulative(List<Message<T>> messages, Throwable cause);

}
```

### Configure Interceptors

Applications can configure interceptors via ProducerBuilder and ConsumerBuilder.

```java
/**
 * Intercept {@link Producer}.
 *
 * @param interceptors the list of interceptors to intercept the producer created by this builder.
 * @return producer builder.
 */
ProducerBuilder<T> intercept(ProducerInterceptor<T> ... interceptors);
```

```java
/**
 * Intercept {@link Consumer}.
 *
 * @param interceptors the list of interceptors to intercept the consumer created by this builder.
 * @return consumer builder.
 */
ConsumerBuilder<T> intercept(ConsumerInterceptor<T>... interceptors);
```

### Tracing Interceptor

Applications can choose its preferred tracing framework for integration. The tracing framework can be either OpenTracing or OpenCensus. Pulsar doesn’t enforce any tracing framework. 

Following shows an example how end-to-end tracing using OpenTracing works using Interceptors in Pulsar.

#### Trace Context

In order to achieve end-to-end tracing, the most important thing is to be able to propagate trace context from Producer to Consumer. This trace context can be serialized and propagated into the properties in message header and carried as part of message.

Use OpenTracing as an example:

- The trace context carrier is the Message.
- A trace context Format need to be defined how to serialize / deserialize trace context from the properties of this Message.

#### Producer Tracing Interceptor

A tracing interceptor is pretty straightforward:

- In the implementation of `beforeSend` method, it should first try to deserialize a trace context (because the application who is using Pulsar producer might already set a trace context in the message header). If there is a trace context, then use the trace context, otherwise it should start a new trace span.
- Once a trace context is established, it can record any information that needs to be traced.

Similarly,

- In the implementation of `onSendAcknowledgment`, it can retrieve the trace context and record any information that needs to be traced on acknowledgments.

#### Consumer Tracing Interceptor

Implementing a consumer tracing interceptor is similar as implementing a producer tracing interceptor as above.

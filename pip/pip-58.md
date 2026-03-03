# PIP-58: Support Consumers  Set Custom Retry Delay

- Status: Accepted
- Author: Dezhi Liu
- Pull Request:
- Mailing List discussion: https://lists.apache.org/thread.html/r5ab7533b6461f29b8ff18e4c51e058226a1b3081fddc5649b234b7fc%40%3Cdev.pulsar.apache.org%3E
- Release: 2.6.0

## Motivation

For many online business systems, various exceptions usually occur in
business logic processing, so the message needs to be re-consumed, but
users hope that this delay time can be controlled flexibly. The current
user's processing method is usually to send this message to a special retry
topic because production can specify any delay, so consumers subscribe to the
business topic and retry topic at the same time. I think this logic can be
supported by pulsar itself, making it easier for users to use, and it looks
like this is a very common requirement.

## Proposed changes

This change can be supported on the client side,  need to add a set of
interfaces to org.apache.pulsar.client.api.Consumer

```java
void reconsumeLater(Message<?> message, long delayTime, TimeUnit unit)
throws PulsarClientException;
CompletableFuture<Void> reconsumeLaterAsync(Message<?> message, long
delayTime, TimeUnit unit);
CompletableFuture<Void> reconsumeLaterAsync(Messages<?> messages, int
delayLevel);
 CompletableFuture<Void> reconsumeLaterCumulativeAsync(Message<?> message,
long delayTime, TimeUnit unit);
```

DeadLetterPolicy add retry topic
```java
public class DeadLetterPolicy {

    /**
     * Maximum number of times that a message will be redelivered before
being sent to the dead letter queue.
     */
    private int maxRedeliverCount;

    /**
     * Name of the retry topic where the failing messages will be sent.
     */
    private String retryLetterTopic;

    /**
     * Name of the dead topic where the failing messages will be sent.
     */
    private String deadLetterTopic;

}

```

org.apache.pulsar.client.impl.ConsumerImpl add a retry producer
```java
  private volatile Producer<T> deadLetterProducer;

  private volatile Producer<T> retryLetterProducer;
```

Can specify whether to enable retry when creating a consumer，default
is disabled.
```java
    @Override
    public ConsumerBuilder<T> enableRetry(boolean retryEnable) {
        conf.setRetryEnable(retryEnable);
        return this;
    }
```

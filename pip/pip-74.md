# PIP-74: Pulsar client memory limits

* **Status**: Proposal
* **Author**: Matteo Merli
* **Pull Request**:
* **Mailing List discussion**:
* **Release**:

## Motivation

Currently, there are multiple settings in producers and consumers that allow
to control the sizes of the message queues. These are ultimately used to
control the amount of memory used by the Pulsar client.

There are few issues with the current approach, that makes it complicated, in
non-trivial scenarios, to select an overall configuration that leads to a
certain use of memory:

 1. The settings are in terms of "number of messages", so one has to adjust
    based on the expected message size.

 2. The settings are per producer/consumer. If an application has a large
    (or simply unknown) number number of producers/consumer, it's very difficult
    to select an appropriate value for queue sizes. The same is true for topics
    that have many partitions.

## Goal

Allow to specify a maximum amount of memory on a given Pulsar client. The
producers and consumers will compete for the memory assigned.

The scope of this proposal is to track the "direct" memory used to hold
outgoing or incoming message payloads. The usage of JVM heap memory is outside
the scope, though normally it only represents a small fraction of the payloads
size.

The change will be done in multiple steps:
 1. Introduce new API to set the memory limit, keeping it disabled by default
  1.1 Implement memory limit for producers
 2. Implement memory limit for consumers
 3. (Once the feature is stable)
    Enable memory limit by default, with 64 MB
 4. Deprecate producer queue size related settings
 5. Ignore producer queue size related settings

## Client API

```java

public class ClientBuilder {
  // ....

  /**
   * Configure a limit on the amount of direct memory that will be allocated by this client instance.
   * <p>
   * Setting this to 0 will disable the limit.
   *
   * @param memoryLimit
   *            the limit
   * @param unit
   *            the memory limit size unit
   * @return the client builder instance
   */
  ClientBuilder memoryLimit(long memoryLimit, SizeUnit unit);
}
```

For example:

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .memoryLimit(64, SizeUnit.MEGA_BYTES)
    .create();
```

The same logic described here will be implemented first in Java and then
in the rest of supported client libraries.

## Implementation

The enforcement of the memory limit is done in a different way for producers as
compared to consumers, although both will share a common instance of a
`MemoryLimitController`, which is unique per each `PulsarClient` instance.

```java
interface MemoryLimitController {

    // Non-blocking
    boolean tryReserveMemory(long size);

    // Blocks until memory is available
    void reserveMemory(long size) throws InterruptedException;

    void releaseMemory(long size);
}
```

The implementation of `MemoryLimitController` will be optimized to avoid
contention across multiple threads trying to reserve and release memory.

### Producer

If the memory limit is set, the producer will first try to acquire the
semaphore that is currently set based on the producer queue size, then it
will try to reserve the memory, either in blocking or non-blocking way,
depending on the producer configuration.

### Consumer

The logic for consumers is slightly more complicated because a client needs to
give permits to brokers to push messages. Right now, the count is done in terms
of messages and it cannot simply switch to "bytes" because a consumer will
need to consume from multiple topics and partitions.

Because of that, we need to invert the order, by using the memory limit
controller as a wait to interact with the flow control, reserving memory
after we have already received the messages.

The proposal is to keep the receiver queue size as a configuration, although
treat it more as the "max receiver queue size".

When a consumer is created, and memory limit is set, it will use these
parameters:
 * `maxReceiverQueueSize`: the values configured by the application
 * `currentReceiverQueue`: a dynamic limit that will be auto adjusted, starting
    from an initial value (eg: 1) up to the max.

The goal is to step up the `currentReceiverQueue` if and only if:
 1. Doing it would improve throughput
 2. We have enough memory to do it


#### Controlling throughput

The rationale for `(1)` is that increasing `currentReceiverQueue` increases the
amount of pre-fetched messages for the consumer. The goal of the prefetch queue
is to make sure an application calling `receive()` will never be blocked waiting,
if there are messages available.

Therefore, we can determine that the `currentReceiverQueue` is limiting the
throughput if:

 1. Application calls receive and there are no pre-fetched messages
 2. And we know that there are messages pending to be sent for this consumer.
    For this, in case of exclusive/failover subscriptions, we can rely on the
    last message id, while for other subscription types, we need to introduce
    using a new special command to brokers that will tell if broker waiting on
    consumer for more permits.

If these conditions are true, we can increase the `currentReceiverQueue` to
increase the load.

With this mechanism, we can start with a very small window and expand as needed,
maintaining the minimum size that is able to sustain the requested throughput.

#### Limiting consumer memory

In order to limit the consumer memory, we would only increase the
`currentReceiverQueue` if the memory usage is below a certain threshold (eg: 75%).
Also, if the usage reaches a higher threshold, (eg: 95%) we will reduce the
`currentReceiverQueue` for all the consumers.

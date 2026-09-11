# PIP-5: Event time

- Status: Implemented in 1.20
- Author: [Sijie Guo](https://github.com/sijie)
- Discussion Thread: https://lists.apache.org/thread.html/7270626fcd5b0ffd486c45ab32deda48359c5bc3cca2082ca2d99bce@%3Cdev.pulsar.apache.org%3E
- Issue: [#732](https://github.com/apache/incubator-pulsar/issues/732)

# Motivation

In use cases such as streaming processing, they need a timestamp in messages to process. This timestamp is called `event time`,
which is different from `publish time` - the timestamp that this even occurs. The `event time` is typically provided and set
by the applications.

To solve these use cases, we propose to add a `event time` for pulsar messages.

# Changes

## Public Interfaces

- add a method `#setEventTime(long timestamp)` in `MessageBuilder.java`

```java

/**
 * Set the event time for a given message.
 * <p> Applications can retrieve the event time by calling `Message#getEventTime()`.
 * This field is useful for stream processing.
 * <p> Note: currently pulsar doesn't support event-time based index. so the subscribers can't
 * seek the messages by event time.
 */
MessageBuilder setEventTime(long timestamp);

```

- add a method `#getEventTime()` in `Message.java`

```java

/**
 * Get the event time associated with this event. It is typically set by the applications.
 * <p>If there isn't any event time associated with this event, it will return `-1`.
 */
long getEventTime();

```

## Wire Protocol

we propose to introduce an optional field called `event_time` in `MessageMetadata`.

```java
message MessageMetadata {

    ...

    // the timestamp that this event occurs. it is typically set by applications.
    // if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
    optional int32 event_time = 12 [default = -1];

}
```

# Compatibility, Deprecation, and Migration Plan

This change is backward compatible. There is no special migration plan required.

# Non Covered

This proposal doesn't cover following areas:

- we don't provide any event time index. that means we can't rewind based on event time.

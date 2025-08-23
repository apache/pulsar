# PIP-68: Exclusive Producer

* **Status**: Proposal
* **Author**: Matteo Merli
* **Pull Request**:
* **Mailing List discussion**:
* **Release**:

## Goal

Allow applications to require exclusive producer access on a topic in order to
achieve a "single-writer" situation.

There are several use cases that require exclusive access for a single writer,
of which few examples are:

 * Ensuring a linear non-interleaved history of messages
 * Providing basic mechanism for leader election

From Pulsar perspective, this feature needs to provide 2 fundamental properties:

 1. Guaranteed single-writer - There should be 1 single writer in any combination
    of errors.
 2. Producer fencing - Once a producer looses the exclusive access, no more
    messages from that producer can be published on the topic.

One example of concrete use case for this feature is in the Pulsar Functions
metadata controller. In order to write a single linear history of all the
functions metadata updates, it requires to elect 1 leader and that all the
"decisions" made by this leader be written on the topic.

If we assume single-writer and fencing, we can thus guarantee that the topic
will contain different segments of updates, one per each successive leader, and
that there will be no interleaving across different leaders.

## Client API

An application can require exclusive producer access on a topics by configuring
it on the producer builder.

```java
/**
 * The type of access to the topic that the producer requires.
 */
public enum ProducerAccessMode {
    /**
     * By default multiple producers can publish on a topic.
     */
    Shared,

    /**
     * Require exclusive access for producer. Fail immediately if there's
     * already a producer connected.
     */
    Exclusive,

    /**
     * Producer creation is pending until it can acquire exclusive access.
     */
    WaitForExclusive,
}

public interface ProducerBuilder<T> {
  // .....

  /**
   * Configure the type of access mode that the producer requires on the topic.
   *
   * <p>Possible values are:
   * <ul>
   * <li>{@link ProducerAccessMode#Shared}: By default multiple producers can
   *     publish on a topic
   * <li>{@link ProducerAccessMode#Exclusive}: Require exclusive access for
   *     producer. Fail immediately if there's already a producer connected.
   * <li>{@link ProducerAccessMode#WaitForExclusive}: Producer creation is
   *       pending until it can acquire exclusive access
   * </ul>
   *
   * @see ProducerAccessMode
   * @param accessMode
   *            The type of access to the topic that the producer requires
   * @return the producer builder instance
   */
  ProducerBuilder<T> accessMode(ProducerAccessMode accessMode);  
}
```

For example:

```java

Producer<String> producer = client.newProducer(Schema.STRING)
      .topic("my-topic")
      .accessMode(ProducerAccessMode.Exclusive)
      .create();
```

## Semantic

Once an application is successful in creating a producer with "exclusive"
access, either using `Exclusive` or `WaitForExclusive` mode, the particular
instance of that application is guaranteed to be only writer on that topic.

Other producers trying to produce on this topic, will either receive an error
immediately (if using `Exclusive` or `Shared` access mode) or will have to
wait until the current exclusive producer session is closed.

The application holding the "exclusive" access right to the topic can also
lose this right. This will happen for example if the producer experiences a
network partition with the broker. In this case, this producer will be evicted
and a new producer will be picked to be the next exclusive producer.

The application that has lost the exclusive producer access, will not be
immediately notified of that event, but it's guaranteed to receive a
`ProducerFencedException` the next time it attempts to publish a message on
the topic.

Once the application receives `ProducerFencedException`, the producer instance
is permanently fenced off. To restart producing on the topic, it will be
necessary to create a new instance.

### WaitForExclusive

`WaitForExclusive` access mode is very similar as `Exclusive` with just a
difference on what happens at the producer creation.

 * `Exclusive` : Try to become the only producer and fail immediately if there's
   already another producer connected.
 * `WaitForExclusive` : Same as before but, if there are producers connected,
   suspend the producer creation until we can be promoted to be exclusive
   producer.

In practice, the `ProducerBuilder.create()` will be blocking until we are
promoted to exclusive producer.

This access mode makes it very simple for applications to implement a leader
election scheme, in which the producers that succeeds in becoming the exclusive
producer is treated as the "leader".

## Implementation

A new concept of "topic epoch" is introduced for this feature. The epoch is
a counter that is incremented each time a new exclusive producer takes over
the topic.

The epoch is stored in metadata as part of ManagedLedger properties and it has
to be updated each time a new producer attempts to take over.

The `CommandProducer` protobuf command will be extended with these new fields:

```protobuf
    // Require that this producers will be the only producer allowed on the topic
    optional ProducerAccessMode producer_access_mode = 10 [default = Shared];

    // Topic epoch is used to fence off producers that reconnects after a new
    // exclusive producer has already taken over. This id is assigned by the
    // broker on the CommandProducerSuccess. The first time, the client will
    // leave it empty and then it will always carry the same epoch number on
    // the subsequent reconnections.
    optional uint64 topic_epoch = 11;
```

When a new producer tries to create a session for the 1st time, it will be
leaving the `topic_epoch` empty.

On `CommandProducerSuccess`, the broker will inform the exclusive producer of
it's associated epoch:

```protobuf

    // The topic epoch assigned by the broker. This field will only be set if we
    // were requiring exclusive access when creating the producer.
    optional uint64 topic_epoch = 5;
```

From that point on, the producer instance will always be associated with that
particular topic epoch.

When a producer reconnects, it will pass the existing topic epoch and if it's
lower than the current topic epoch that the broker has, the producer will be
considered "fenced" permanently.

That will ensure that no inflight message from this fenced producer will ever
make its way in the topic.

### Failure detection

The broker owning a particular topic is the one that detect that the current
exclusive producer has failed.

The producer "ownership" of the topic is immediately removed when either:
 1. The client sends a CloseProducer command, for a graceful close
 2. The TCP connection for the producer is closed (perhaps due to the internal
    keepalive probes).

In both cases, the topic is now up for grab for a new producer.

There are 2 scenarios:
 1. The old producer is the first in re-attempting to produce on the topic.
    In this case, the topic epoch will still match the producer epoch and the
    topic epoch doesn't need to get incremented.

 2. A new producer is the first in reconnecting and it will force the
    increment on the topic epoch.


## Example of usage

A typical scenario in which the exclusive producer is useful is to maintain
a consistent state through a compacted topic.

Application will use that to elect a "leader" through which all the updates
are written on the Pulsar topic.

When the producer becomes the exclusive writer on the topic, it will also
need to make to have applied all the update from the previous producers.

Example:


```java
Map<String, T> myState = new HashMap<>();

Producer<T> producer = client.newProducer(Schema.JSON(T.class))
      .topic("my-topic")
      .accessMode(ProducerAccessMode.WaitForExclusive)
      .create();

Reader<T> reader = client.newReader(Schema.JSON(T.class))
       .topic("my-topic")
       .readCompacted(true)
       .create();

while (reader.hasMessageAvailable()) {
  Message<T> msg = reader.readNext();
  myState.put(msg.getKey(), msg.getValue());
}
```

Done. We're now ready to use the producer as a write-ahead-log for the
updates on the state

```java
void write(String key, T value) {
  producer.newMessage().key(key).value(value).send();
  myState.put(key, value);
}
```

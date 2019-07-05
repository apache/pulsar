---
author: Penghui Li
authorURL: https://twitter.com/lipenghui6
title: Apache Pulsar 2.4.0
---

We are glad to present the new 2.4.0 release of Pulsar. This is the result of a huge 
effort from the community, with over 460 commits and a long list of new features, 
general improvements and bug fixes.

Check out the <b>[release notes](/release-notes/#2.4.0)</b> for a detailed list of 
the changes, with links to the relevant pull-requests, discussions and documentation.

Regarding new features introduced, I just want to highlight here a tiny subset of them:

<!--truncate-->

### Delayed message delivery

It's now possible to send delayed message by pulsar producer, and delayed message will 
available after delay time.

The Java code for a client using delayed message delivery will look like:

```java
producer.newMessage().value("delayed message").deliverAfter(10, TimeUnit.SECONDS).send()
```

> Note:
>
> 1. Messages are only delayed on shared subscriptions. Other subscriptions will deliver immediately.
> 2. Can't work well with batching messages while messages has different delay time.

### Go functions

Before 2.4.0, pulsar support use Java/Python to write pulsar functions. Now, it's 
possible to use golang to write pulsar functions, the following is an example of 
a Pulsar Function written in golang.

```go
import (
    "fmt"
    "context"

    "github.com/apache/pulsar/pulsar-function-go/pf"
)

func HandleRequest(ctx context.Context, in []byte) error {
    fmt.Println(string(in) + "!")
    return nil
}

func main() {
    pf.Start(HandleRequest)
}
```

### Key_shared subscription

A new subscribe type `Key_shared`. By `Key_shared` one partition could have several 
consumers to parallel consume messages, while all messages with the same key will be 
dispatched to only one consumer ordered. 
Here is [architecture](http://pulsar.apache.org/docs/en/concepts-messaging/#key_shared) 
for Key_Shared.

The following is an example to use `Key_shared` subscription:

```java
client.newConsumer()
        .topic("topic")
        .subscriptionType(SubscriptionType.Key_Shared)
        .subscriptionName("sub-1")
        .subscribe();
```

### Schema versioning

Before 2.4.0, avro schema is using one schema for both its writer schema and reader schema. 
Multiple schema version is support now.

Use multiple schema, producer can send messages with different schema version and consumer 
can read messages with different schema.

In 2.4.0, added `FORWARD_TRANSITIVE`, `BACKWARD_TRANSITIVE` and `FULL_TRANSITIVE` compatibility 
strategy to  check the compatibility with all existing schema version.

### Replicated subscription

In 2.4.0, added a mechanism to keep subscription state in-sync, within a sub-second timeframe, 
in the context of a topic that is being asynchronously replicated across multiple geographical 
regions. Here is [architecture](https://github.com/apache/pulsar/wiki/PIP-33%3A-Replicated-subscriptions) 
for replicated subscription.

The following is an example to use replicated subscription:

```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic("my-topic")
            .subscriptionName("my-subscription")
            .replicateSubscriptionState(true)
            .subscribe();
```

### New IO connectors

A new batch of connectors was added, including Flume, Redis sink, Solr sink, RabbitMQ sink, 
InfluxDB sink. Here is list of builtin [connectors](http://pulsar.apache.org/docs/en/io-connectors/) 
that pulsar already supported now.

### Security

In 2.4.0, added support Kerberos in pulsar broker and client. 
Following the [document](http://pulsar.apache.org/docs/en/security-kerberos/) to enable Kerberos authentication.

## Conclusion

Please [download](/download) Pulsar 2.4.0 and report feedback, issues or any comment into our mailing lists,
slack channel or Github page. ([Contact page](/contact))
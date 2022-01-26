---
author: Penghui Li
authorURL: https://twitter.com/lipenghui6
title: Apache Pulsar 2.4.0
---

We are glad to publish Apache Pulsar 2.4.0. This is the result of a huge 
effort from the community, with over 460 commits and a long list of new features, 
general improvements and bug fixes.

Check out the <b>[release notes](/release-notes/#2.4.0)</b> for a detailed list of 
the changes, with links to the relevant pull requests, discussions and documentation.

Regarding new features introduced, I just want to highlight here a tiny subset of them:

<!--truncate-->

### Delayed message delivery

It's now possible to send a delayed message by Pulsar producer, and a delayed message will be
available after a delay time.

The Java code for a client using delayed messages delivery looks as follows:

```java

producer.newMessage().value("delayed message").deliverAfter(10, TimeUnit.SECONDS).send()

```

:::note

1. Messages are only delayed on shared subscriptions, other subscriptions will deliver immediately.
2. Delayed messages are sent individually even if you enable message batching on producer.

:::

### Go Functions

Before 2.4.0 release, Java and Python are supported to write Pulsar Functions. Now, you can 
use Go to write Pulsar Functions, the following is an example of 
a Pulsar Function written in Go.

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

### Key_Shared subscription

A new subscription mode `Key_shared` is introduced in 2.4.0. In `Key_shared` subscription mode, 
one partition could have several consumers to consume messages in parallelism and ensure messages 
with the same key are distributed to a consumer in order. 
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

Before 2.4.0 release, Avro schema used one schema for both writer schema and reader schema. 
Multiple schemas version is supported now.

With multiple schemas, a producer can send messages with different schema versions and a consumer 
can read messages with different schemas.

In 2.4.0 release, `FORWARD_TRANSITIVE`, `BACKWARD_TRANSITIVE` and `FULL_TRANSITIVE` compatibility 
strategies are added to check the compatibility with all existing schema version.

### Replicated subscription

In 2.4.0 release, a mechanism is added to keep subscription state in sync, within a sub-second timeframe, 
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

A new batch of connectors is added, including Flume, Redis sink, Solr sink, RabbitMQ sink. 
The following lists builtin [connectors](http://pulsar.apache.org/docs/en/io-connectors/) 
that Pulsar supports.

### Security

In 2.4.0 release, Kerberos is supported in Apache Pulsar broker and client. 
To enable Kerberos authentication, refer to the [document](http://pulsar.apache.org/docs/en/security-kerberos/).

Also added role based Pulsar Function authentication and authorization.

## Conclusion

If you want to download Pulsar 2.4.0, click [here](/download). You can send any questions or suggestions 
to our mailing lists, contribute to Pulsar on [GitHub](https://github.com/apache/pulsar) or join 
the Apache Pulsar community on [Slack](https://apache-pulsar.herokuapp.com/).
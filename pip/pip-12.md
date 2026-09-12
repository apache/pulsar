# PIP-12: Introduce builder for creating Producer Consumer Reader

* **Status**: Implemented
* **Author**: [Matteo Merli](https://github.com/merlimat)
* **Pull Request**: [#1089](https://github.com/apache/incubator-pulsar/pull/1089)
* **Mailing List discussion**:

## Motivation

The current client API is using a mix of configuration object and "builder"
pattern.

`Client`, `Producer`, `Consumer` and `Reader` objects are created by passing
an optional `ClientConfiguration` (`ProducerConfiguration`, ..) object. If that
is not passed, all the default options are being used (same as passing
`new ClientConfiguration()`).

For example:
```java
ClientConfiguration conf = new ClientConfiguration();
conf.setIoThreads(8);
PulsarClient client = PulsarClient.create("pulsar://localhost:6650", conf);
```

On the contrary, when building a message to be published, we are following the
builder pattern:

```java
Message msg = MessageBuilder.create().setPayload(myPayload).build();
producer.send(msg);
```

The main problem with the configuration objects is that they need a lot of
methods to be used. For example, we have multiple methods for creating a
producer, depending on wether they accept a `conf` parameter or wether they
are sync or async:

```java
Producer createProducer(String topic);
CompletableFuture<Producer> createProducerAsync(String topic);

Producer createProducer(String topic,
              ProducerConfiguration conf);
CompletableFuture<Producer> createProducerAsync(
              String topic, ProducerConfiguration conf);
```

With that, we have 4 variations for creating a `Producer`, 4 for a `Consumer`
and 4 for a `Reader`.

If we want to add different ways to subscribe, for example subscribing to
a set of topics at once, or by specifying a regular expression matching
multiple topics, the number of methods in the `PulsarClient` API will just
explode, making it very difficult for people to navigate.

The same problem we would have for adding a type information at the API
level, since some of the information included in the configuration might
be typed as well (eg: the `MessageListener` on a `Consumer` is set on the
`ConsumerConfiguration` but that would be depending on the "type" of the
message).

## Proposal

This proposal is to introduce builder style constructors for `Producer`,
`Consumer`, `Reader` and also `PulsarClient` to have consistency across the
board.

```java
interface PulsarClient {
    public static ClientBuilder builder() {
       return new ClientBuilderImpl();
   }

    ProducerBuilder newProducer();
    ConsumerBuilder newConsumer();
    ReaderBuilder newReader();

    // All other methods will be deprecated in 2.0
    // and possibly removed in following versions
}
```

Another design choice made here is not treat the required arguments in a
a special way.

For example, when creating a `Producer`, the "topic" name is a required
information and that's why the previous API was differentiating it from
the optional configs:


```java
client.createProducer(topic, conf);
```

with the builder, topic will have to be set on the builder and its presence
will be validated when we try to convert the builder into a proper `Producer`.

## Examples

```java
Producer producer = client.newProducer()
        .topic("my-topic")
        .sendTimeout(30, TimeUnit.SECONDS)
        .create();
```

Similarly,

```java
CompletableFuture<Producer> future = client.newProducer()
        .topic("my-topic")
        .createAsync();
```


### Consumers

```java
Consumer consumer = client.newConsumer()
        .topic("my-topic")
        .subscriptionName("my-subscription")
        .subscribe();
```

or asynchronously:

```java
CompletableFuture<Consumer> consumer = client.newConsumer()
        .topic("my-topic")
        .subscriptionName("my-subscription")
        .subscribeAsync();
```

When subscribing to a set of topics, this could be expressed
as:

```java
Consumer consumer = client.newConsumer()
        .topicSet(myListOfTopics)
        .subscriptionName("my-subscription")
        .subscribe();
```

or to a regex matching multiple topics:

```java
Consumer consumer = client.newConsumer()
        .topicPattern(myRegex)
        .subscriptionName("my-subscription")
        .subscribe();
```

## Type information

When augmenting the API with the schema information, we could add the
`Schema` before getting the builder instance.

The reason for that is to have a "typed" builder that will create a
typed `Producer`/`Consumer`.

For that, the end result might look like:

```java
// No type info specified
Producer<byte[]> producer = client.newProducer()
        .topic("my-topic")
        .create();


// Schema provided
Producer<MyClass> producer = client
        .newProducer(Schema.Json.of(MyClass.class))
        .topic("my-topic")
        .create();
```

## Concerns

The only disadvantage with the new notation is that it makes the minimal
example of code sligtly longer.

```java
PulsarClient pulsarClient = PulsarClient.create("http://localhost:6650");

Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic");
```

With this proposal, it would become:

```java
PulsarClient client = PulsarClient.builder()
        .serviceUrl("http://localhost:6650")
        .build();

Producer producer = client.newProducer()
        .topic("persistent://my-property/use/my-ns/my-topic")
        .create();
```

The positive side is that it will make it more intuitive to switch from
the minimal example code to changing configuration options.

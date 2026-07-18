# Appendix A — Java Reference Mapping

**Status:** Draft · Informative

This appendix maps the language-neutral concepts and operations of this specification to the **Java V5
client** (`org.apache.pulsar.client.api.v5`), the reference implementation. It is **informative**: the
normative contract is in the other documents. Other-language SDKs use this only to cross-check intent.

## 1. Entry point and types

| Spec concept | Java type / call |
|--------------|------------------|
| Client | `PulsarClient` (`PulsarClient.builder()`) |
| Producer factory | `PulsarClient.newProducer(Schema<T>)` → `ProducerBuilder<T>` |
| Stream consumer factory | `PulsarClient.newStreamConsumer(Schema<T>)` → `StreamConsumerBuilder<T>` |
| Queue consumer factory | `PulsarClient.newQueueConsumer(Schema<T>)` → `QueueConsumerBuilder<T>` |
| Checkpoint consumer factory | `PulsarClient.newCheckpointConsumer(Schema<T>)` → `CheckpointConsumerBuilder<T>` |
| Transaction factory | `PulsarClient.newTransaction()` / `newTransactionAsync()` |
| Message | `Message<T>` |
| Message identifier | `MessageId` (`toByteArray`/`fromByteArray`, `earliest`/`latest`) |
| Checkpoint | `Checkpoint` (`toByteArray`/`fromByteArray`, `earliest`/`latest`) |
| Schema | `org.apache.pulsar.client.api.v5.schema.Schema` |
| Async views | `async()` → `org.apache.pulsar.client.api.v5.async.Async*` |

## 2. Producer

| Spec | Java |
|------|------|
| CreateProducer | `ProducerBuilder.create()` / `createAsync()`; `.topic`, `.producerName`, `.accessMode`, `.sendTimeout`, `.blockIfQueueFull`, `.compressionPolicy`, `.batchingPolicy`, `.chunkingPolicy`, `.encryptionPolicy`, `.initialSequenceId`, `.property/properties` |
| Send | `producer.newMessage()` → `MessageBuilder<T>`; `.value`, `.key`, `.property/properties`, `.eventTime`, `.sequenceId`, `.deliverAfter/deliverAt`, `.transaction`, `.replicationClusters`, `.send()` → `MessageId` |
| Last sequence id | `Producer.lastSequenceId()` |
| Flush / Close | `AsyncProducer.flushAsync()` ; `Producer.close()` |
| Access modes | `ProducerAccessMode.{SHARED, EXCLUSIVE, EXCLUSIVE_WITH_FENCING, WAIT_FOR_EXCLUSIVE}` |

## 3. Consumers

| Spec | Java |
|------|------|
| Stream subscribe | `StreamConsumerBuilder.subscribe()`; `.topic` XOR `.namespace(...)`, `.subscriptionName`, `.subscriptionInitialPosition`, `.acknowledgmentGroupTime`, `.readCompacted`, `.replicateSubscriptionState`, … |
| Stream receive | `StreamConsumer.receive()` / `receive(Duration)` / `receiveMulti(int, Duration)` |
| Stream cumulative ack | `StreamConsumer.acknowledgeCumulative(MessageId[, Transaction])` |
| Queue subscribe | `QueueConsumerBuilder.subscribe()` (topic + subscriptionName) |
| Queue ack / nack | `QueueConsumer.acknowledge(MessageId[, Transaction])` ; `negativeAcknowledge(MessageId)` |
| Checkpoint create | `CheckpointConsumerBuilder.create()`; `.topic`, `.startPosition(Checkpoint)`, `.consumerGroup`, … |
| Checkpoint receive | `CheckpointConsumer.receive()` / `receive(Duration)` / `receiveMulti(int, Duration)` |
| Capture checkpoint | `CheckpointConsumer.checkpoint()` → `Checkpoint` |
| Initial position (stream) | `SubscriptionInitialPosition.{Earliest, Latest}` |

> Note: `StreamConsumer` and `CheckpointConsumer` are `Closeable`; `QueueConsumer` is `AutoCloseable`.
> The grouped/ungrouped checkpoint distinction is `CheckpointConsumerBuilder.consumerGroup(...)` set vs.
> unset.

## 4. Message

| Spec field | Java `Message<T>` |
|------------|-------------------|
| value / raw payload / size | `value()` / `data()` / `size()` |
| message identifier | `id()` |
| key | `key()` → `Optional<String>` |
| properties | `properties()` |
| publish time / event time | `publishTime()` → `Instant` ; `eventTime()` → `Optional<Instant>` |
| sequence identifier | `sequenceId()` |
| producer name / topic | `producerName()` → `Optional<String>` ; `topic()` |
| redelivery count | `redeliveryCount()` |
| replicated-from | `replicatedFrom()` → `Optional<String>` |

## 5. Transactions

| Spec | Java `Transaction` |
|------|--------------------|
| state | `state()` → `Transaction.State.{OPEN, COMMITTING, ABORTING, COMMITTED, ABORTED, ERROR, TIMED_OUT}` |
| commit / abort | `commit()` / `abort()` (and `async()`) |
| transactional publish / ack | `MessageBuilder.transaction(txn)` ; `*Consumer.acknowledge*(id, txn)` |

## 6. Errors

Error categories ([Error Model](error-model.md)) map to `PulsarClientException` (v5) subtypes:
`NotFoundException`, `AuthenticationException`, `AuthorizationException`, `TimeoutException`,
`AlreadyClosedException`, `ProducerBusyException`, `ConsumerBusyException`,
`IncompatibleSchemaException`, `TopicTerminatedException`, `ConnectException`, `NotConnectedException`,
`ProducerQueueIsFullException`, `MemoryBufferIsFullException`, `TransactionConflictException`,
`CryptoException`, `InvalidConfigurationException`, `InvalidServiceURLException`.

## 7. Configuration / policies

Spec configuration maps to the `org.apache.pulsar.client.api.v5.config` package:
`ProducerAccessMode`, `SubscriptionInitialPosition`, `CompressionType`/`CompressionPolicy`,
`BatchingPolicy`, `ChunkingPolicy`, `ProducerEncryptionPolicy`/`ConsumerEncryptionPolicy`,
`ProducerCryptoFailureAction`/`ConsumerCryptoFailureAction`, `ConnectionPolicy`, `BackoffPolicy`,
`TlsPolicy`, `ProxyProtocol`, `TransactionPolicy`, `ProcessingTimeoutPolicy`, `DeadLetterPolicy`,
`MemorySize`.

## 8. Entry-bucketing

**Entry-bucketing** ([Scalable Key-Shared Consumption](key-shared.md)) has no application-facing Java
surface — the producer buckets and stamps internally, and the Stream consumer applies bucket-shared
assignments (Key_Shared attach, individual acks, drain on mode flips) internally. An application only
observes that creating more Stream consumers than segments increases parallelism. When the Java client
exposes controls (e.g. per-topic bucket policy), this section will map them.

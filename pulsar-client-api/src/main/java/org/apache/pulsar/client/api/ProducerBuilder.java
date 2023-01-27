/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClientException.ProducerQueueIsFullError;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * {@link ProducerBuilder} is used to configure and create instances of {@link Producer}.
 *
 * @see PulsarClient#newProducer()
 * @see PulsarClient#newProducer(Schema)
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ProducerBuilder<T> extends Cloneable {

    /**
     * Finalize the creation of the {@link Producer} instance.
     *
     * <p>This method will block until the producer is created successfully.
     *
     * @return the producer instance
     * @throws PulsarClientException.ProducerBusyException
     *             if a producer with the same "producer name" is already connected to the topic
     * @throws PulsarClientException
     *             if the producer creation fails
     */
    Producer<T> create() throws PulsarClientException;

    /**
     * Finalize the creation of the {@link Producer} instance in asynchronous mode.
     *
     * <p>This method will return a {@link CompletableFuture} that can be used to access the instance when it's ready.
     *
     * @return a future that will yield the created producer instance
     * @throws PulsarClientException.ProducerBusyException
     *             if a producer with the same "producer name" is already connected to the topic
     * @throws PulsarClientException
     *             if the producer creation fails
     */
    CompletableFuture<Producer<T>> createAsync();

    /**
     * Load the configuration from provided <tt>config</tt> map.
     *
     * <p>Example:
     * <pre>{@code
     * Map<String, Object> config = new HashMap<>();
     * config.put("producerName", "test-producer");
     * config.put("sendTimeoutMs", 2000);
     *
     * ProducerBuilder<byte[]> builder = client.newProducer()
     *                  .loadConf(config);
     *
     * Producer<byte[]> producer = builder.create();
     * }</pre>
     *
     * @param config configuration map to load
     * @return the producer builder instance
     */
    ProducerBuilder<T> loadConf(Map<String, Object> config);

    /**
     * Create a copy of the current {@link ProducerBuilder}.
     *
     * <p>Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     * <pre>{@code
     * ProducerBuilder<String> builder = client.newProducer(Schema.STRING)
     *                  .sendTimeout(10, TimeUnit.SECONDS)
     *                  .blockIfQueueFull(true);
     *
     * Producer<String> producer1 = builder.clone().topic("topic-1").create();
     * Producer<String> producer2 = builder.clone().topic("topic-2").create();
     * }</pre>
     *
     * @return a clone of the producer builder instance
     */
    ProducerBuilder<T> clone();

    /**
     * Specify the topic this producer will be publishing on.
     *
     * <p>This argument is required when constructing the producer.
     *
     * @param topicName the name of the topic
     * @return the producer builder instance
     */
    ProducerBuilder<T> topic(String topicName);

    /**
     * Specify a name for the producer.
     *
     * <p>If not assigned, the system will generate a globally unique name which can be accessed with
     * {@link Producer#getProducerName()}.
     *
     * <p><b>Warning</b>: When specifying a name, it is up to the user to ensure that, for a given topic,
     * the producer name is unique across all Pulsar's clusters.
     * Brokers will enforce that only a single producer a given name can be publishing on a topic.
     *
     * @param producerName
     *            the custom name to use for the producer
     * @return the producer builder instance
     */
    ProducerBuilder<T> producerName(String producerName);

    /**
     * Configure the type of access mode that the producer requires on the topic.
     *
     * <p>Possible values are:
     * <ul>
     * <li>{@link ProducerAccessMode#Shared}: By default multiple producers can publish on a topic.
     * <li>{@link ProducerAccessMode#Exclusive}: Require exclusive access for producer. Fail immediately if there's
     * already a producer connected.
     * <li>{@link ProducerAccessMode#ExclusiveWithFencing}: Require exclusive access for the producer.
     * Any existing producer will be removed and invalidated immediately.
     * <li>{@link ProducerAccessMode#WaitForExclusive}: Producer creation is pending until it can acquire exclusive
     * access.
     * </ul>
     *
     * @see ProducerAccessMode
     * @param accessMode
     *            The type of access to the topic that the producer requires
     * @return the producer builder instance
     */
    ProducerBuilder<T> accessMode(ProducerAccessMode accessMode);

    /**
     * Set the send timeout <i>(default: 30 seconds)</i>.
     *
     * <p>If a message is not acknowledged by the server before the sendTimeout expires, an error will be reported.
     *
     * <p>Setting the timeout to zero with {@code setTimeout(0, TimeUnit.SECONDS)} will set the timeout
     * to infinity. This can be useful when using Pulsar's message deduplication feature, since the client
     * library will retry forever to publish a message. No errors will be propagated back to the application.
     *
     * @param sendTimeout
     *            the send timeout
     * @param unit
     *            the time unit of the {@code sendTimeout}
     * @return the producer builder instance
     */
    ProducerBuilder<T> sendTimeout(int sendTimeout, TimeUnit unit);

    /**
     * Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
     *
     * <p>When the queue is full, by default, all calls to {@link Producer#send} and {@link Producer#sendAsync}
     * will fail unless {@code blockIfQueueFull=true}. Use {@link #blockIfQueueFull(boolean)}
     * to change the blocking behavior.
     *
     * <p>The producer queue size also determines the max amount of memory that will be required by
     * the client application. Until the producer gets a successful acknowledgment back from the broker,
     * it will keep in memory (direct memory pool) all the messages in the pending queue.
     *
     * <p>Default is 0, which disables the pending messages check.
     *
     * @param maxPendingMessages
     *            the max size of the pending messages queue for the producer
     * @return the producer builder instance
     */
    ProducerBuilder<T> maxPendingMessages(int maxPendingMessages);

    /**
     * Set the number of max pending messages across all partitions.
     *
     * <p>This setting will be used to lower the max pending messages for each partition
     * ({@link #maxPendingMessages(int)}), if the total exceeds the configured value.
     * The purpose of this setting is to have an upper-limit on the number
     * of pending messages when publishing on a partitioned topic.
     *
     * <p>Default is 0, which disables the pending messages across partitions check.
     *
     * <p>If publishing at a high rate over a topic with many partitions (especially when publishing messages without a
     * partitioning key), it might be beneficial to increase this parameter to allow for more pipelining within the
     * individual partitions' producers.
     *
     * @param maxPendingMessagesAcrossPartitions
     *            max pending messages across all the partitions
     * @return the producer builder instance
     */
    @Deprecated
    ProducerBuilder<T> maxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions);

    /**
     * Set whether the {@link Producer#send} and {@link Producer#sendAsync} operations should block when the outgoing
     * message queue is full.
     *
     * <p>Default is {@code false}. If set to {@code false}, send operations will immediately fail with
     * {@link ProducerQueueIsFullError} when there is no space left in pending queue. If set to
     * {@code true}, the {@link Producer#sendAsync} operation will instead block.
     *
     * <p>Setting {@code blockIfQueueFull=true} simplifies the task of an application that
     * just wants to publish messages as fast as possible, without having to worry
     * about overflowing the producer send queue.
     *
     * <p>For example:
     * <pre><code>
     * Producer&lt;String&gt; producer = client.newProducer()
     *                  .topic("my-topic")
     *                  .blockIfQueueFull(true)
     *                  .create();
     *
     * while (true) {
     *     producer.sendAsync("my-message")
     *          .thenAccept(messageId -> {
     *              System.out.println("Published message: " + messageId);
     *          })
     *          .exceptionally(ex -> {
     *              System.err.println("Failed to publish: " + e);
     *              return null;
     *          });
     * }
     * </code></pre>
     *
     * @param blockIfQueueFull
     *            whether to block {@link Producer#send} and {@link Producer#sendAsync} operations on queue full
     * @return the producer builder instance
     */
    ProducerBuilder<T> blockIfQueueFull(boolean blockIfQueueFull);

    /**
     * Set the {@link MessageRoutingMode} for a partitioned producer.
     *
     * <p>Default routing mode is to round-robin across the available partitions.
     *
     * <p>This logic is applied when the application is not setting a key on a
     * particular message. If the key is set with {@link MessageBuilder#setKey(String)},
     * then the hash of the key will be used to select a partition for the message.
     *
     * @param messageRoutingMode
     *            the message routing mode
     * @return the producer builder instance
     * @see MessageRoutingMode
     */
    ProducerBuilder<T> messageRoutingMode(MessageRoutingMode messageRoutingMode);

    /**
     * Change the {@link HashingScheme} used to choose the partition on which to publish a particular message.
     *
     * <p>Standard hashing functions available are:
     * <ul>
     * <li>{@link HashingScheme#JavaStringHash}: Java {@code String.hashCode()} (Default)
     * <li>{@link HashingScheme#Murmur3_32Hash}: Use Murmur3 hashing function.
     * <a href="https://en.wikipedia.org/wiki/MurmurHash">https://en.wikipedia.org/wiki/MurmurHash</a>
     * </ul>
     *
     * @param hashingScheme
     *            the chosen {@link HashingScheme}
     * @return the producer builder instance
     */
    ProducerBuilder<T> hashingScheme(HashingScheme hashingScheme);

    /**
     * Set the compression type for the producer.
     *
     * <p>By default, message payloads are not compressed.
     * <p>Supported compression types are:
     * <ul>
     * <li>{@link CompressionType#NONE}: No compression (default)</li>
     * <li>{@link CompressionType#LZ4}: Compress with LZ4 algorithm. Faster but lower compression than ZLib.</li>
     * <li>{@link CompressionType#ZLIB}: Standard ZLib compression.</li>
     * <li>{@link CompressionType#ZSTD} Compress with Zstd codec. Since Pulsar 2.3, Zstd can only be used
     * if consumer applications are also in version >= 2.3.</li>
     * <li>{@link CompressionType#SNAPPY} Compress with Snappy codec. Since Pulsar 2.4, Snappy can only be used if
     * consumer applications are also in version >= 2.4.</li>
     * </ul>
     *
     * @param compressionType
     *            the selected compression type
     * @return the producer builder instance
     */
    ProducerBuilder<T> compressionType(CompressionType compressionType);

    /**
     * Set a custom message routing policy by passing an implementation of MessageRouter.
     *
     * @param messageRouter
     * @return the producer builder instance
     */
    ProducerBuilder<T> messageRouter(MessageRouter messageRouter);

    /**
     * Set automatic batching of messages for the producer. <i>default: enabled</i>
     *
     * <p>When batching is enabled, multiple calls to {@link Producer#sendAsync} can result in a single batch
     * being sent to the broker, leading to better throughput, especially when publishing small messages.
     * If compression is enabled, messages will be compressed at the batch level, leading to a much better
     * compression ratio for similar headers or contents.
     *
     * <p>When enabled, default batch delay is set to 1 ms and default batch size is 1000 messages.
     *
     * <p>Batching is enabled by default since 2.0.0.
     *
     * @see #batchingMaxPublishDelay(long, TimeUnit)
     * @see #batchingMaxMessages(int)
     * @return the producer builder instance
     */
    ProducerBuilder<T> enableBatching(boolean enableBatching);

    /**
     * If a message's size is higher than the broker's allowed max publish-payload size, enableChunking allows
     * the producer to split the message into multiple chunks and publish it to the broker separately and in order.
     *
     * <p>This feature allows the publishing of large messages by splitting messages into multiple chunks and
     * re-assembling them with the consumer to form the original large published message. Therefore, this configuration
     * of the pulsar producer and consumer is recommended to use this feature:
     *
     * <pre>
     * 1. This feature is currently only supported for non-shared subscriptions and persistent topics.
     * 2. Disable batching to use chunking feature.
     * 3. Pulsar-client stores published messages in buffer cache until it receives acknowledgement from the broker.
     * Therefore, it's best practice to reduce the "maxPendingMessages" size to avoid the producer occupying large
     * amounts of memory with buffered messages.
     * 4. Set message-ttl on the namespace to clean up incomplete chunked messages.
     * (If a producer fails to publish an entire large message, the consumer will be unable to consume and acknowledge
     * those messages. These messages can only be discarded by message TTL or by configuring
     * {@link ConsumerBuilder#expireTimeOfIncompleteChunkedMessage}.
     * 5. Consumer configuration: consumer should also configure receiverQueueSize and maxPendingChunkedMessage
     * </pre>
     * @param enableChunking
     * @return
     */
    ProducerBuilder<T> enableChunking(boolean enableChunking);

    /**
     * Max chunk-message size in bytes. Producer chunks the message if chunking is enabled and message size is larger
     * than max chunk-message size. By default, chunkMaxMessageSize value is -1 and the producer chunks based
     * on the max-message size configured on the broker.
     *
     * @param chunkMaxMessageSize
     * @return
     */
    ProducerBuilder<T> chunkMaxMessageSize(int chunkMaxMessageSize);

    /**
     * Sets a {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to encrypt the message payloads.
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     * @return the producer builder instance
     */
    ProducerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Sets the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to encrypt the message payloads.
     *
     * @param publicKey
     *            the public key that is always used to encrypt message payloads.
     * @return the producer builder instance
     * @since 2.8.0
     */
    ProducerBuilder<T> defaultCryptoKeyReader(String publicKey);

    /**
     * Sets the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to encrypt the message payloads.
     *
     * @param publicKeys
     *            the map of public key names and their URIs used to encrypt message payloads.
     * @return the producer builder instance
     * @since 2.8.0
     */
    ProducerBuilder<T> defaultCryptoKeyReader(Map<String, String> publicKeys);

    /**
     * Add public encryption key, used by producer to encrypt the data key.
     *
     * <p>At the time of producer creation, the Pulsar client checks if there are keys added to encryptionKeys. If keys
     * are found, a callback {@link CryptoKeyReader#getPrivateKey(String, Map)} and
     * {@link CryptoKeyReader#getPublicKey(String, Map)} is invoked against each key to load the values of the key.
     *
     * <p>Application should implement this callback to return the key in pkcs8 format. If compression is enabled,
     * message is encrypted after compression. If batch messaging is enabled, the batched message is encrypted.
     *
     * @param key
     *            the name of the encryption key in the key store
     * @return the producer builder instance
     */
    ProducerBuilder<T> addEncryptionKey(String key);

    /**
     * Set the ProducerCryptoFailureAction to the value specified.
     *
     * @param action
     *            the action the producer will take in case of encryption failures
     * @return the producer builder instance
     */
    ProducerBuilder<T> cryptoFailureAction(ProducerCryptoFailureAction action);

    /**
     * Set the time period within which messages sent will be batched if batch messages are
     * enabled. The default value is 1 ms. If set to a non-zero value, messages will be queued until either:
     * <ul>
     * <li>this time interval expires</li>
     * <li>the max number of messages in a batch is reached ({@link #batchingMaxMessages(int)})
     * <li>the max size of batch is reached
     * </ul>
     *
     * <p>All messages will be published as a single batch message. The consumer will be delivered individual
     * messages in the batch in the same order they were enqueued.
     *
     * @param batchDelay
     *            the batch delay
     * @param timeUnit
     *            the time unit of the {@code batchDelay}
     * @return the producer builder instance
     * @see #batchingMaxMessages(int)
     * @see #batchingMaxBytes(int)
     */
    ProducerBuilder<T> batchingMaxPublishDelay(long batchDelay, TimeUnit timeUnit);

    /**
     * Set the partition switch frequency while batching of messages is enabled and
     * using round-robin routing mode for non-keyed messages. <i>Default: 10</i>.
     *
     * <p>The time period of partition switch is frequency * batchingMaxPublishDelay. During this period,
     * all arriving messages will be routed to the same partition.
     *
     * @param frequency the frequency of partition switch
     * @return the producer builder instance
     * @see #messageRoutingMode(MessageRoutingMode)
     * @see #batchingMaxPublishDelay(long, TimeUnit)
     */
    ProducerBuilder<T> roundRobinRouterBatchingPartitionSwitchFrequency(int frequency);

    /**
     * Set the maximum number of messages permitted in a batch. The default value is 1000. If set to a value greater
     * than 1, messages will be queued until this threshold is reached or batch interval has elapsed.
     *
     * <p>All messages in batch will be published as a single batch message. The consumer will be delivered individual
     * messages in the batch in the same order they were enqueued.
     *
     * @param batchMessagesMaxMessagesPerBatch
     *            maximum number of messages in a batch
     * @return the producer builder instance
     * @see #batchingMaxPublishDelay(long, TimeUnit)
     * @see #batchingMaxBytes(int)
     */
    ProducerBuilder<T> batchingMaxMessages(int batchMessagesMaxMessagesPerBatch);

    /**
     * Set the maximum number of bytes permitted in a batch. The default value is 128KB.
     * If set to a value greater than 0, messages will be queued until this threshold is reached
     * or other batching conditions are met.
     *
     * <p>All messages in a batch will be published as a single batched message. The consumer will be delivered
     * individual messages in the batch in the same order they were enqueued.
     *
     * @param batchingMaxBytes maximum number of bytes in a batch
     * @return the producer builder instance
     * @see #batchingMaxPublishDelay(long, TimeUnit)
     * @see #batchingMaxMessages(int)
     */
    ProducerBuilder<T> batchingMaxBytes(int batchingMaxBytes);

    /**
     * Set the batcher builder {@link BatcherBuilder} of the producer. Producer will use the batcher builder to
     * build a batch message container.This is only be used when batching is enabled.
     *
     * @param batcherBuilder
     *          batcher builder
     * @return the producer builder instance
     */
    ProducerBuilder<T> batcherBuilder(BatcherBuilder batcherBuilder);

    /**
     * Set the baseline for sequence ids for messages published by the producer.
     *
     * <p>First message will use {@code (initialSequenceId + 1)} as its sequence id, and
     * subsequent messages will be assigned incremental sequence ids, if not otherwise specified.
     *
     * @param initialSequenceId the initial sequence id for the producer
     * @return the producer builder instance
     */
    ProducerBuilder<T> initialSequenceId(long initialSequenceId);

    /**
     * Set a name/value property for this producer.
     *
     * <p>Properties are application-defined metadata that can be attached to the producer.
     * When getting topic stats, this metadata will be associated to the producer
     * stats for easier identification.
     *
     * @param key
     *            the property key
     * @param value
     *            the property value
     * @return the producer builder instance
     */
    ProducerBuilder<T> property(String key, String value);

    /**
     * Add all the properties in the provided map to the producer.
     *
     * <p>Properties are application-defined metadata that can be attached to the producer.
     * When getting topic stats, this metadata will be associated to the producer
     * stats for easier identification.
     *
     * @param properties the map of properties
     * @return the producer builder instance
     */
    ProducerBuilder<T> properties(Map<String, String> properties);

    /**
     * Add a set of {@link ProducerInterceptor}s to the producer.
     *
     * <p>Interceptors can be used to trace the publish and acknowledgments operations happening in a producer.
     *
     * @param interceptors
     *            the list of interceptors to intercept the producer created by this builder.
     * @return the producer builder instance
     */
    @Deprecated
    ProducerBuilder<T> intercept(ProducerInterceptor<T> ... interceptors);

    /**
     * Add a set of {@link org.apache.pulsar.client.api.interceptor.ProducerInterceptor} to the producer.
     *
     * <p>Interceptors can be used to trace the publish and acknowledgments operation happening in a producer.
     *
     * @param interceptors
     *            the list of interceptors to intercept the producer created by this builder.
     * @return the producer builder instance
     */
    ProducerBuilder<T> intercept(org.apache.pulsar.client.api.interceptor.ProducerInterceptor... interceptors);

    /**
     * If enabled, partitioned producer will automatically discover new partitions at runtime. This is only applied on
     * partitioned topics.
     *
     * <p>Default is true.
     *
     * @param autoUpdate
     *            whether to auto discover partition configuration changes
     * @return the producer builder instance
     */
    ProducerBuilder<T> autoUpdatePartitions(boolean autoUpdate);

    /**
     * Set the interval of updating partitions. The default value is 1 minute. This only works if autoUpdatePartitions
     * is enabled.
     *
     * @param interval
     *            the interval of updating partitions
     * @param unit
     *            the time unit of the interval.
     * @return the producer builder instance
     */
    ProducerBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit);

    /**
     * Set the multiple schema mode for producer.
     * If enabled, the producer can send a message with a schema different from the schema specified at creation.
     * <p>>Otherwise, if the producer wanted to send a message with different schema,
     * an invalid message exception would be thrown
     *
     * <p>Enabled by default.
     *
     * @param multiSchema
     *            enable or disable multiple schema mode
     * @return the producer builder instance
     * @since 2.5.0
     */
    ProducerBuilder<T> enableMultiSchema(boolean multiSchema);

    /**
     * This config affects Shared mode producers of partitioned topics only. It controls whether
     * producers register and connect immediately to the owner broker of each partition
     * or start lazily on demand. The internal producer of one partition always
     * starts immediately, as chosen by the routing policy, but the internal producers of
     * any additional partitions are started on demand upon receiving their first
     * message.
     * Using this mode can reduce the strain on brokers for topics with large numbers of
     * partitions and when the SinglePartition or some custom partial partition routing policy
     * like PartialRoundRobinMessageRouterImpl is used without keyed messages.
     * Because producer connection can be on demand, this can produce extra send latency
     * for the first messages of a given partition.
     *
     * @param lazyStartPartitionedProducers
     *            enable or disable starting partition producers lazily
     * @return the producer builder instance
     */
    ProducerBuilder<T> enableLazyStartPartitionedProducers(boolean lazyStartPartitionedProducers);
}

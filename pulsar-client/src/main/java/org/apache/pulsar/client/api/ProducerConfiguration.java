/**
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;

import com.google.common.base.Objects;

/**
 * Producer's configuration
 *
 * @deprecated use {@link PulsarClient#newProducer()} to construct and configure a {@link Producer} instance
 */
@Deprecated
public class ProducerConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;
    private String producerName = null;
    private long sendTimeoutMs = 30000;
    private boolean blockIfQueueFull = false;
    private int maxPendingMessages = 1000;
    private int maxPendingMessagesAcrossPartitions = 50000;
    private MessageRoutingMode messageRouteMode = MessageRoutingMode.SinglePartition;
    private HashingScheme hashingScheme = HashingScheme.JavaStringHash;
    private MessageRouter customMessageRouter = null;
    private long batchingMaxPublishDelayMs = 10;
    private int batchingMaxMessages = 1000;
    private boolean batchingEnabled = false; // disabled by default

    private CryptoKeyReader cryptoKeyReader;
    private ConcurrentOpenHashSet<String> encryptionKeys;

    private CompressionType compressionType = CompressionType.NONE;

    // Cannot use Optional<Long> since it's not serializable
    private Long initialSequenceId = null;

    private final Map<String, String> properties = new HashMap<>();

    public enum MessageRoutingMode {
        SinglePartition, RoundRobinPartition, CustomPartition
    }

    public enum HashingScheme {
        JavaStringHash,
        Murmur3_32Hash
    }

    private ProducerCryptoFailureAction cryptoFailureAction = ProducerCryptoFailureAction.FAIL;

    /**
     * @return the configured custom producer name or null if no custom name was specified
     * @since 1.20.0
     */
    public String getProducerName() {
        return producerName;
    }

    /**
     * Specify a name for the producer
     * <p>
     * If not assigned, the system will generate a globally unique name which can be access with
     * {@link Producer#getProducerName()}.
     * <p>
     * When specifying a name, it is app to the user to ensure that, for a given topic, the producer name is unique
     * across all Pulsar's clusters.
     * <p>
     * If a producer with the same name is already connected to a particular topic, the
     * {@link PulsarClient#createProducer(String)} operation will fail with {@link ProducerBusyException}.
     *
     * @param producerName
     *            the custom name to use for the producer
     * @since 1.20.0
     */
    public void setProducerName(String producerName) {
        this.producerName = producerName;
    }

    /**
     * @return the message send timeout in ms
     */
    public long getSendTimeoutMs() {
        return sendTimeoutMs;
    }

    /**
     * Set the send timeout <i>(default: 30 seconds)</i>
     * <p>
     * If a message is not acknowledged by the server before the sendTimeout expires, an error will be reported.
     *
     * @param sendTimeout
     *            the send timeout
     * @param unit
     *            the time unit of the {@code sendTimeout}
     */
    public ProducerConfiguration setSendTimeout(int sendTimeout, TimeUnit unit) {
        checkArgument(sendTimeout >= 0);
        this.sendTimeoutMs = unit.toMillis(sendTimeout);
        return this;
    }

    /**
     * @return the maximum number of messages allowed in the outstanding messages queue for the producer
     */
    public int getMaxPendingMessages() {
        return maxPendingMessages;
    }

    /**
     * Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
     * <p>
     * When the queue is full, by default, all calls to {@link Producer#send} and {@link Producer#sendAsync} will fail
     * unless blockIfQueueFull is set to true. Use {@link #setBlockIfQueueFull} to change the blocking behavior.
     *
     * @param maxPendingMessages
     * @return
     */
    public ProducerConfiguration setMaxPendingMessages(int maxPendingMessages) {
        checkArgument(maxPendingMessages > 0);
        this.maxPendingMessages = maxPendingMessages;
        return this;
    }

    public HashingScheme getHashingScheme() {
        return hashingScheme;
    }

    public ProducerConfiguration setHashingScheme(HashingScheme hashingScheme) {
        this.hashingScheme = hashingScheme;
        return this;
    }

    /**
     *
     * @return the maximum number of pending messages allowed across all the partitions
     */
    public int getMaxPendingMessagesAcrossPartitions() {
        return maxPendingMessagesAcrossPartitions;
    }

    /**
     * Set the number of max pending messages across all the partitions
     * <p>
     * This setting will be used to lower the max pending messages for each partition
     * ({@link #setMaxPendingMessages(int)}), if the total exceeds the configured value.
     *
     * @param maxPendingMessagesAcrossPartitions
     */
    public void setMaxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions) {
        checkArgument(maxPendingMessagesAcrossPartitions >= maxPendingMessages);
        this.maxPendingMessagesAcrossPartitions = maxPendingMessagesAcrossPartitions;
    }

    /**
     *
     * @return whether the producer will block {@link Producer#send} and {@link Producer#sendAsync} operations when the
     *         pending queue is full
     */
    public boolean getBlockIfQueueFull() {
        return blockIfQueueFull;
    }

    /**
     * Set whether the {@link Producer#send} and {@link Producer#sendAsync} operations should block when the outgoing
     * message queue is full.
     * <p>
     * Default is <code>false</code>. If set to <code>false</code>, send operations will immediately fail with
     * {@link PulsarClientException.ProducerQueueIsFullError} when there is no space left in pending queue.
     *
     * @param blockIfQueueFull
     *            whether to block {@link Producer#send} and {@link Producer#sendAsync} operations on queue full
     * @return
     */
    public ProducerConfiguration setBlockIfQueueFull(boolean blockIfQueueFull) {
        this.blockIfQueueFull = blockIfQueueFull;
        return this;
    }

    /**
     * Set the message routing mode for the partitioned producer
     *
     * @param mode
     * @return
     */
    public ProducerConfiguration setMessageRoutingMode(MessageRoutingMode messageRouteMode) {
        checkNotNull(messageRouteMode);
        this.messageRouteMode = messageRouteMode;
        return this;
    }

    /**
     * Get the message routing mode for the partitioned producer
     *
     * @return
     */
    public MessageRoutingMode getMessageRoutingMode() {
        return messageRouteMode;
    }

    /**
     * Set the compression type for the producer.
     * <p>
     * By default, message payloads are not compressed. Supported compression types are:
     * <ul>
     * <li><code>CompressionType.LZ4</code></li>
     * <li><code>CompressionType.ZLIB</code></li>
     * </ul>
     *
     * @param compressionType
     * @return
     *
     * @since 1.0.28 <br>
     *        Make sure all the consumer applications have been updated to use this client version, before starting to
     *        compress messages.
     */
    public ProducerConfiguration setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    /**
     * @return the configured compression type for this producer
     */
    public CompressionType getCompressionType() {
        return compressionType;
    }

    /**
     * Set a custom message routing policy by passing an implementation of MessageRouter
     *
     *
     * @param messageRouter
     */
    public ProducerConfiguration setMessageRouter(MessageRouter messageRouter) {
        checkNotNull(messageRouter);
        setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        customMessageRouter = messageRouter;
        return this;
    }

    /**
     * Get the message router set by {@link #setMessageRouter(MessageRouter)}.
     *
     * @return message router.
     * @deprecated since 1.22.0-incubating. <tt>numPartitions</tt> is already passed as parameter in
     *             {@link MessageRouter#choosePartition(Message, TopicMetadata)}.
     * @see MessageRouter
     */
    @Deprecated
    public MessageRouter getMessageRouter(int numPartitions) {
        return customMessageRouter;
    }

    /**
     * Get the message router set by {@link #setMessageRouter(MessageRouter)}.
     *
     * @return message router set by {@link #setMessageRouter(MessageRouter)}.
     */
    public MessageRouter getMessageRouter() {
        return customMessageRouter;
    }

    /**
     * @ return if batch messages are enabled
     */

    public boolean getBatchingEnabled() {
        return batchingEnabled;
    }

    /**
     * Control whether automatic batching of messages is enabled for the producer. <i>default: false [No batching]</i>
     *
     * When batching is enabled, multiple calls to Producer.sendAsync can result in a single batch to be sent to the
     * broker, leading to better throughput, especially when publishing small messages. If compression is enabled,
     * messages will be compressed at the batch level, leading to a much better compression ratio for similar headers or
     * contents.
     *
     * When enabled default batch delay is set to 10 ms and default batch size is 1000 messages
     *
     * @see ProducerConfiguration#setBatchingMaxPublishDelay(long, TimeUnit)
     * @since 1.0.36 <br>
     *        Make sure all the consumer applications have been updated to use this client version, before starting to
     *        batch messages.
     */

    public ProducerConfiguration setBatchingEnabled(boolean batchMessagesEnabled) {
        this.batchingEnabled = batchMessagesEnabled;
        return this;
    }

    /**
     * @return the CryptoKeyReader
     */
    public CryptoKeyReader getCryptoKeyReader() {
        return this.cryptoKeyReader;
    }

    /**
     * Sets a {@link CryptoKeyReader}
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     */
    public ProducerConfiguration setCryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        checkNotNull(cryptoKeyReader);
        this.cryptoKeyReader = cryptoKeyReader;
        return this;
    }

    /**
     *
     * @return encryptionKeys
     *
     */
    public ConcurrentOpenHashSet<String> getEncryptionKeys() {
        return this.encryptionKeys;
    }

    /**
     *
     * Returns true if encryption keys are added
     *
     */
    public boolean isEncryptionEnabled() {
        return (this.encryptionKeys != null) && !this.encryptionKeys.isEmpty() && (this.cryptoKeyReader != null);
    }

    /**
     * Add public encryption key, used by producer to encrypt the data key.
     *
     * At the time of producer creation, Pulsar client checks if there are keys added to encryptionKeys. If keys are
     * found, a callback getKey(String keyName) is invoked against each key to load the values of the key. Application
     * should implement this callback to return the key in pkcs8 format. If compression is enabled, message is encrypted
     * after compression. If batch messaging is enabled, the batched message is encrypted.
     *
     */
    public void addEncryptionKey(String key) {
        if (this.encryptionKeys == null) {
            this.encryptionKeys = new ConcurrentOpenHashSet<String>(16, 1);
        }
        this.encryptionKeys.add(key);
    }

    public void removeEncryptionKey(String key) {
        if (this.encryptionKeys != null) {
            this.encryptionKeys.remove(key);
        }
    }

    /**
     * Sets the ProducerCryptoFailureAction to the value specified
     *
     * @param action
     *            The producer action
     */
    public void setCryptoFailureAction(ProducerCryptoFailureAction action) {
        cryptoFailureAction = action;
    }

    /**
     * @return The ProducerCryptoFailureAction
     */
    public ProducerCryptoFailureAction getCryptoFailureAction() {
        return this.cryptoFailureAction;
    }

    /**
     *
     * @return the batch time period in ms.
     * @see ProducerConfiguration#setBatchingMaxPublishDelay(long, TimeUnit)
     */
    public long getBatchingMaxPublishDelayMs() {
        return batchingMaxPublishDelayMs;
    }

    /**
     * Set the time period within which the messages sent will be batched <i>default: 10ms</i> if batch messages are
     * enabled. If set to a non zero value, messages will be queued until this time interval or until
     *
     * @see ProducerConfiguration#batchingMaxMessages threshold is reached; all messages will be published as a single
     *      batch message. The consumer will be delivered individual messages in the batch in the same order they were
     *      enqueued
     * @since 1.0.36 <br>
     *        Make sure all the consumer applications have been updated to use this client version, before starting to
     *        batch messages.
     * @param batchDelay
     *            the batch delay
     * @param timeUnit
     *            the time unit of the {@code batchDelay}
     * @return
     */
    public ProducerConfiguration setBatchingMaxPublishDelay(long batchDelay, TimeUnit timeUnit) {
        long delayInMs = timeUnit.toMillis(batchDelay);
        checkArgument(delayInMs >= 1, "configured value for batch delay must be at least 1ms");
        this.batchingMaxPublishDelayMs = delayInMs;
        return this;
    }

    /**
     *
     * @return the maximum number of messages permitted in a batch.
     */
    public int getBatchingMaxMessages() {
        return batchingMaxMessages;
    }

    /**
     * Set the maximum number of messages permitted in a batch. <i>default: 1000</i> If set to a value greater than 1,
     * messages will be queued until this threshold is reached or batch interval has elapsed
     *
     * @see ProducerConfiguration#setBatchingMaxPublishDelay(long, TimeUnit) All messages in batch will be published as
     *      a single batch message. The consumer will be delivered individual messages in the batch in the same order
     *      they were enqueued
     * @param batchMessagesMaxMessagesPerBatch
     *            maximum number of messages in a batch
     * @return
     */
    public ProducerConfiguration setBatchingMaxMessages(int batchMessagesMaxMessagesPerBatch) {
        checkArgument(batchMessagesMaxMessagesPerBatch > 0);
        this.batchingMaxMessages = batchMessagesMaxMessagesPerBatch;
        return this;
    }

    public Optional<Long> getInitialSequenceId() {
        return initialSequenceId != null ? Optional.of(initialSequenceId) : Optional.empty();
    }

    /**
     * Set the baseline for the sequence ids for messages published by the producer.
     * <p>
     * First message will be using (initialSequenceId + 1) as its sequence id and subsequent messages will be assigned
     * incremental sequence ids, if not otherwise specified.
     *
     * @param initialSequenceId
     * @return
     */
    public ProducerConfiguration setInitialSequenceId(long initialSequenceId) {
        this.initialSequenceId = initialSequenceId;
        return this;
    }

    /**
     * Set a name/value property with this producer.
     *
     * @param key
     * @param value
     * @return
     */
    public ProducerConfiguration setProperty(String key, String value) {
        checkArgument(key != null);
        checkArgument(value != null);
        properties.put(key, value);
        return this;
    }

    /**
     * Add all the properties in the provided map
     *
     * @param properties
     * @return
     */
    public ProducerConfiguration setProperties(Map<String, String> properties) {
        if (properties != null) {
            this.properties.putAll(properties);
        }
        return this;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ProducerConfiguration) {
            ProducerConfiguration other = (ProducerConfiguration) obj;
            return Objects.equal(this.sendTimeoutMs, other.sendTimeoutMs)
                    && Objects.equal(maxPendingMessages, other.maxPendingMessages)
                    && Objects.equal(this.messageRouteMode, other.messageRouteMode)
                    && Objects.equal(this.hashingScheme, other.hashingScheme);
        }

        return false;
    }
}

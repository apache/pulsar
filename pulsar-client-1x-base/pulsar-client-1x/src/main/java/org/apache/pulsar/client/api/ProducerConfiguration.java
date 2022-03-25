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
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;

/**
 * Producer's configuration.
 *
 * @deprecated use {@link PulsarClient#newProducer()} to construct and configure a {@link Producer} instance
 */
@Deprecated
@EqualsAndHashCode
public class ProducerConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ProducerConfigurationData conf = new ProducerConfigurationData();

    @Deprecated
    public enum MessageRoutingMode {
        SinglePartition, RoundRobinPartition, CustomPartition
    }

    @Deprecated
    public enum HashingScheme {
        JavaStringHash, Murmur3_32Hash
    }

    /**
     * @return the configured custom producer name or null if no custom name was specified
     * @since 1.20.0
     */
    public String getProducerName() {
        return conf.getProducerName();
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
        conf.setProducerName(producerName);
    }

    /**
     * @return the message send timeout in ms
     */
    public long getSendTimeoutMs() {
        return conf.getSendTimeoutMs();
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
        conf.setSendTimeoutMs(sendTimeout, unit);
        return this;
    }

    /**
     * @return the maximum number of messages allowed in the outstanding messages queue for the producer
     */
    public int getMaxPendingMessages() {
        return conf.getMaxPendingMessages();
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
        conf.setMaxPendingMessages(maxPendingMessages);
        return this;
    }

    public HashingScheme getHashingScheme() {
        return HashingScheme.valueOf(conf.getHashingScheme().toString());
    }

    public ProducerConfiguration setHashingScheme(HashingScheme hashingScheme) {
        conf.setHashingScheme(org.apache.pulsar.client.api.HashingScheme.valueOf(hashingScheme.toString()));
        return this;
    }

    /**
     *
     * @return the maximum number of pending messages allowed across all the partitions
     */
    public int getMaxPendingMessagesAcrossPartitions() {
        return conf.getMaxPendingMessagesAcrossPartitions();
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
        conf.setMaxPendingMessagesAcrossPartitions(maxPendingMessagesAcrossPartitions);
    }

    /**
     *
     * @return whether the producer will block {@link Producer#send} and {@link Producer#sendAsync} operations when the
     *         pending queue is full
     */
    public boolean getBlockIfQueueFull() {
        return conf.isBlockIfQueueFull();
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
        conf.setBlockIfQueueFull(blockIfQueueFull);
        return this;
    }

    /**
     * Set the message routing mode for the partitioned producer.
     *
     * @param messageRouteMode message routing mode.
     * @return producer configuration
     * @see MessageRoutingMode
     */
    public ProducerConfiguration setMessageRoutingMode(MessageRoutingMode messageRouteMode) {
        Objects.requireNonNull(messageRouteMode);
        conf.setMessageRoutingMode(
                org.apache.pulsar.client.api.MessageRoutingMode.valueOf(messageRouteMode.toString()));
        return this;
    }

    /**
     * Get the message routing mode for the partitioned producer.
     *
     * @return message routing mode, default is round-robin routing.
     * @see MessageRoutingMode#RoundRobinPartition
     */
    public MessageRoutingMode getMessageRoutingMode() {
        return MessageRoutingMode.valueOf(conf.getMessageRoutingMode().toString());
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
        conf.setCompressionType(compressionType);
        return this;
    }

    /**
     * @return the configured compression type for this producer
     */
    public CompressionType getCompressionType() {
        return conf.getCompressionType();
    }

    /**
     * Set a custom message routing policy by passing an implementation of MessageRouter.
     *
     *
     * @param messageRouter
     */
    public ProducerConfiguration setMessageRouter(MessageRouter messageRouter) {
        Objects.requireNonNull(messageRouter);
        setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        conf.setCustomMessageRouter(messageRouter);
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
        return conf.getCustomMessageRouter();
    }

    /**
     * Get the message router set by {@link #setMessageRouter(MessageRouter)}.
     *
     * @return message router set by {@link #setMessageRouter(MessageRouter)}.
     */
    public MessageRouter getMessageRouter() {
        return conf.getCustomMessageRouter();
    }

    /**
     * Return the flag whether automatic message batching is enabled or not.
     *
     * @return true if batch messages are enabled. otherwise false.
     * @since 2.0.0 <br>
     *        It is enabled by default.
     */
    public boolean getBatchingEnabled() {
        return conf.isBatchingEnabled();
    }

    /**
     * Control whether automatic batching of messages is enabled for the producer. <i>default: false [No batching]</i>
     *
     * When batching is enabled, multiple calls to Producer.sendAsync can result in a single batch to be sent to the
     * broker, leading to better throughput, especially when publishing small messages. If compression is enabled,
     * messages will be compressed at the batch level, leading to a much better compression ratio for similar headers or
     * contents.
     *
     * When enabled default batch delay is set to 1 ms and default batch size is 1000 messages
     *
     * @see ProducerConfiguration#setBatchingMaxPublishDelay(long, TimeUnit)
     * @since 1.0.36 <br>
     *        Make sure all the consumer applications have been updated to use this client version, before starting to
     *        batch messages.
     *
     */
    public ProducerConfiguration setBatchingEnabled(boolean batchMessagesEnabled) {
        conf.setBatchingEnabled(batchMessagesEnabled);
        return this;
    }

    /**
     * @return the CryptoKeyReader
     */
    public CryptoKeyReader getCryptoKeyReader() {
        return conf.getCryptoKeyReader();
    }

    /**
     * Sets a {@link CryptoKeyReader}.
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     */
    public ProducerConfiguration setCryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        Objects.requireNonNull(cryptoKeyReader);
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    /**
     *
     * @return encryptionKeys
     *
     */
    public Set<String> getEncryptionKeys() {
        return conf.getEncryptionKeys();
    }

    /**
     *
     * Returns true if encryption keys are added.
     *
     */
    public boolean isEncryptionEnabled() {
        return conf.isEncryptionEnabled();
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
        conf.getEncryptionKeys().add(key);
    }

    public void removeEncryptionKey(String key) {
        conf.getEncryptionKeys().remove(key);
    }

    /**
     * Sets the ProducerCryptoFailureAction to the value specified.
     *
     * @param action
     *            The producer action
     */
    public void setCryptoFailureAction(ProducerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
    }

    /**
     * @return The ProducerCryptoFailureAction
     */
    public ProducerCryptoFailureAction getCryptoFailureAction() {
        return conf.getCryptoFailureAction();
    }

    /**
     *
     * @return the batch time period in ms.
     * @see ProducerConfiguration#setBatchingMaxPublishDelay(long, TimeUnit)
     */
    public long getBatchingMaxPublishDelayMs() {
        return TimeUnit.MICROSECONDS.toMillis(conf.getBatchingMaxPublishDelayMicros());
    }

    /**
     * Set the time period within which the messages sent will be batched <i>default: 1ms</i> if batch messages are
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
        conf.setBatchingMaxPublishDelayMicros(batchDelay, timeUnit);
        return this;
    }

    /**
     *
     * @return the maximum number of messages permitted in a batch.
     */
    public int getBatchingMaxMessages() {
        return conf.getBatchingMaxMessages();
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
        conf.setBatchingMaxMessages(batchMessagesMaxMessagesPerBatch);
        return this;
    }

    public Optional<Long> getInitialSequenceId() {
        return Optional.ofNullable(conf.getInitialSequenceId());
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
        conf.setInitialSequenceId(initialSequenceId);
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
        conf.getProperties().put(key, value);
        return this;
    }

    /**
     * Add all the properties in the provided map.
     *
     * @param properties
     * @return
     */
    public ProducerConfiguration setProperties(Map<String, String> properties) {
        conf.getProperties().putAll(properties);
        return this;
    }

    public Map<String, String> getProperties() {
        return conf.getProperties();
    }

    public ProducerConfigurationData getProducerConfigurationData() {
        return conf;
    }
}

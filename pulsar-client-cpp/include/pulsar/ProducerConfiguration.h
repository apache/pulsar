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
#ifndef PULSAR_PRODUCERCONFIGURATION_H_
#define PULSAR_PRODUCERCONFIGURATION_H_
#include <pulsar/defines.h>
#include <pulsar/CompressionType.h>
#include <pulsar/MessageRoutingPolicy.h>
#include <pulsar/Result.h>
#include <pulsar/Message.h>
#include <functional>
#include <pulsar/ProducerCryptoFailureAction.h>
#include <pulsar/CryptoKeyReader.h>
#include <pulsar/Schema.h>

#include <set>

namespace pulsar {

typedef std::function<void(Result, const MessageId& messageId)> SendCallback;
typedef std::function<void(Result)> CloseCallback;

struct ProducerConfigurationImpl;
class PulsarWrapper;

/**
 * Class that holds the configuration for a producer
 */
class PULSAR_PUBLIC ProducerConfiguration {
   public:
    enum PartitionsRoutingMode
    {
        UseSinglePartition,
        RoundRobinDistribution,
        CustomPartition
    };
    enum HashingScheme
    {
        Murmur3_32Hash,
        BoostHash,
        JavaStringHash
    };
    enum BatchingType
    {
        /**
         * Default batching.
         *
         * <p>incoming single messages:
         * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
         *
         * <p>batched into single batch message:
         * [(k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)]
         */
        DefaultBatching,

        /**
         * Key based batching.
         *
         * <p>incoming single messages:
         * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
         *
         * <p>batched into single batch message:
         * [(k1, v1), (k1, v2), (k1, v3)], [(k2, v1), (k2, v2), (k2, v3)], [(k3, v1), (k3, v2), (k3, v3)]
         */
        KeyBasedBatching
    };

    ProducerConfiguration();
    ~ProducerConfiguration();
    ProducerConfiguration(const ProducerConfiguration&);
    ProducerConfiguration& operator=(const ProducerConfiguration&);

    /**
     * Set the producer name which could be assigned by the system or specified by the client.
     *
     * @param producerName producer name.
     * @return
     */
    ProducerConfiguration& setProducerName(const std::string& producerName);

    /**
     * The getter associated with setProducerName().
     */
    const std::string& getProducerName() const;

    /**
     * Declare the schema of the data that will be published by this producer.
     *
     * The schema will be checked against the schema of the topic, and it
     * will fail if it's not compatible, though the client library will
     * not perform any validation that the actual message payload are
     * conforming to the specified schema.
     *
     * For all purposes, this
     * @param schemaInfo
     * @return
     */
    ProducerConfiguration& setSchema(const SchemaInfo& schemaInfo);

    /**
     * @return the schema information declared for this producer
     */
    const SchemaInfo& getSchema() const;

    /**
     * The getter associated with getSendTimeout()
     */
    ProducerConfiguration& setSendTimeout(int sendTimeoutMs);

    /**
     * Get the send timeout is milliseconds.
     *
     * If a message is not acknowledged by the server before the sendTimeout expires, an error will be
     * reported.
     *
     * If the timeout is zero, there will be no timeout.
     *
     * @return the send timeout in milliseconds (Default: 30000)
     */
    int getSendTimeout() const;

    /**
     * Set the baseline of the sequence ID for messages published by the producer.
     * <p>
     * The first message uses (initialSequenceId + 1) as its sequence ID and subsequent messages are assigned
     * incremental sequence IDs.
     *
     * Default: -1, which means the first message's sequence ID is 0.
     *
     * @param initialSequenceId the initial sequence ID for the producer.
     * @return
     */
    ProducerConfiguration& setInitialSequenceId(int64_t initialSequenceId);

    /**
     * The getter associated with setInitialSequenceId().
     */
    int64_t getInitialSequenceId() const;

    /**
     * Set the compression type for the producer.
     * <p>
     * By default, message payloads are not compressed. Supported compression types are:
     * <ul>
     *
     * <li>{@link CompressionNone}: No compression</li>
     * <li>{@link CompressionLZ4}: LZ4 Compression https://lz4.github.io/lz4/
     * <li>{@link CompressionZLib}: ZLib Compression http://zlib.net/</li>
     * <li>{@link CompressionZSTD}: Zstandard Compression  https://facebook.github.io/zstd/ (Since Pulsar 2.3.
     * Zstd cannot be used if consumer applications are not in version >= 2.3 as well)</li>
     * <li>{@link CompressionSNAPPY}: Snappy Compression  https://google.github.io/snappy/ (Since Pulsar 2.4.
     * Snappy cannot be used if consumer applications are not in version >= 2.4 as well)</li>
     * </ul>
     */
    ProducerConfiguration& setCompressionType(CompressionType compressionType);

    /**
     * The getter associated with setCompressionType().
     */
    CompressionType getCompressionType() const;

    /**
     * Set the max size of the queue holding the messages pending to receive an acknowledgment from the
     * broker. <p> When the queue is full, by default, all calls to Producer::send and Producer::sendAsync
     * would fail unless blockIfQueueFull is set to true. Use {@link #setBlockIfQueueFull} to change the
     * blocking behavior.
     *
     * Default: 1000
     *
     * @param maxPendingMessages max number of pending messages.
     * @return
     */
    ProducerConfiguration& setMaxPendingMessages(int maxPendingMessages);

    /**
     * The getter associated with setMaxPendingMessages().
     */
    int getMaxPendingMessages() const;

    /**
     * Set the number of max pending messages across all the partitions
     * <p>
     * This setting will be used to lower the max pending messages for each partition
     * ({@link #setMaxPendingMessages(int)}), if the total exceeds the configured value.
     *
     * Default: 50000
     *
     * @param maxPendingMessagesAcrossPartitions
     */
    ProducerConfiguration& setMaxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions);

    /**
     * @return the maximum number of pending messages allowed across all the partitions
     */
    int getMaxPendingMessagesAcrossPartitions() const;

    /**
     * Set the message routing modes for partitioned topics.
     *
     * Default: UseSinglePartition
     *
     * @param PartitionsRoutingMode partition routing mode.
     * @return
     */
    ProducerConfiguration& setPartitionsRoutingMode(const PartitionsRoutingMode& mode);

    /**
     * The getter associated with setPartitionsRoutingMode().
     */
    PartitionsRoutingMode getPartitionsRoutingMode() const;

    /**
     * Set a custom message routing policy by passing an implementation of MessageRouter.
     *
     * @param messageRouter message router.
     * @return
     */
    ProducerConfiguration& setMessageRouter(const MessageRoutingPolicyPtr& router);

    /**
     * The getter associated with setMessageRouter().
     */
    const MessageRoutingPolicyPtr& getMessageRouterPtr() const;

    /**
     * Set the hashing scheme, which is a standard hashing function available when choosing the partition
     * used for a particular message.
     *
     * Default: HashingScheme::BoostHash
     *
     * <p>Standard hashing functions available are:
     * <ul>
     * <li>{@link HashingScheme::JavaStringHash}: Java {@code String.hashCode()} (Default).
     * <li>{@link HashingScheme::BoostHash}: Use [Boost hashing
     * function](https://www.boost.org/doc/libs/1_72_0/doc/html/boost/hash.html).
     * <li>{@link HashingScheme::Murmur3_32Hash}: Use [Murmur3 hashing
     * function](https://en.wikipedia.org/wiki/MurmurHash").
     * </ul>
     *
     * @param scheme hashing scheme.
     * @return
     */
    ProducerConfiguration& setHashingScheme(const HashingScheme& scheme);

    /**
     * The getter associated with setHashingScheme().
     */
    HashingScheme getHashingScheme() const;

    /**
     * This config affects producers of partitioned topics only. It controls whether
     * producers register and connect immediately to the owner broker of each partition
     * or start lazily on demand. The internal producer of one partition is always
     * started eagerly, chosen by the routing policy, but the internal producers of
     * any additional partitions are started on demand, upon receiving their first
     * message.
     * Using this mode can reduce the strain on brokers for topics with large numbers of
     * partitions and when the SinglePartition routing policy is used without keyed messages.
     * Because producer connection can be on demand, this can produce extra send latency
     * for the first messages of a given partition.
     * @param true/false as to whether to start partition producers lazily
     * @return
     */
    ProducerConfiguration& setLazyStartPartitionedProducers(bool);

    /**
     * The getter associated with setLazyStartPartitionedProducers()
     */
    bool getLazyStartPartitionedProducers() const;

    /**
     * The setter associated with getBlockIfQueueFull()
     */
    ProducerConfiguration& setBlockIfQueueFull(bool);

    /**
     * @return whether Producer::send or Producer::sendAsync operations should block when the outgoing message
     * queue is full. (Default: false)
     */
    bool getBlockIfQueueFull() const;

    // Zero queue size feature will not be supported on consumer end if batching is enabled

    /**
     * Control whether automatic batching of messages is enabled or not for the producer.
     *
     * Default: true
     *
     * When automatic batching is enabled, multiple calls to Producer::sendAsync can result in a single batch
     * to be sent to the broker, leading to better throughput, especially when publishing small messages. If
     * compression is enabled, messages are compressed at the batch level, leading to a much better
     * compression ratio for similar headers or contents.
     *
     * When the default batch delay is set to 10 ms and the default batch size is 1000 messages.
     *
     * @see ProducerConfiguration::setBatchingMaxPublishDelayMs
     *
     */
    ProducerConfiguration& setBatchingEnabled(const bool& batchingEnabled);

    /**
     * Return the flag whether automatic message batching is enabled or not for the producer.
     *
     * @return true if automatic message batching is enabled. Otherwise it returns false.
     * @since 2.0.0 <br>
     *        It is enabled by default.
     */
    const bool& getBatchingEnabled() const;

    /**
     * Set the max number of messages permitted in a batch. <i>Default value: 1000.</i> If you set this option
     * to a value greater than 1, messages are queued until this threshold is reached or batch interval has
     * elapsed.
     *
     * All messages in a batch are published as
     *      a single batch message. The consumer is delivered individual messages in the batch in the same
     * order they are enqueued.
     * @param batchMessagesMaxMessagesPerBatch max number of messages permitted in a batch
     * @return
     */
    ProducerConfiguration& setBatchingMaxMessages(const unsigned int& batchingMaxMessages);

    /**
     * The getter associated with setBatchingMaxMessages().
     */
    const unsigned int& getBatchingMaxMessages() const;

    /**
     * Set the max size of messages permitted in a batch.
     * <i>Default value: 128 KB.</i> If you set this option to a value greater than 1,
     * messages are queued until this threshold is reached or
     * batch interval has elapsed.
     *
     * <p>All messages in a batch are published as a single batch message.
     * The consumer is delivered individual
     * messages in the batch in the same order they are enqueued.
     *
     * @param batchingMaxAllowedSizeInBytes
     */
    ProducerConfiguration& setBatchingMaxAllowedSizeInBytes(
        const unsigned long& batchingMaxAllowedSizeInBytes);

    /**
     * The getter associated with setBatchingMaxAllowedSizeInBytes().
     */
    const unsigned long& getBatchingMaxAllowedSizeInBytes() const;

    /**
     * Set the max time for message publish delay permitted in a batch.
     * <i>Default value: 10 ms.</i>
     *
     * @param batchingMaxPublishDelayMs max time for message publish delay permitted in a batch.
     * @return
     */
    ProducerConfiguration& setBatchingMaxPublishDelayMs(const unsigned long& batchingMaxPublishDelayMs);

    /**
     * The getter associated with setBatchingMaxPublishDelayMs().
     */
    const unsigned long& getBatchingMaxPublishDelayMs() const;

    /**
     * Default: DefaultBatching
     *
     * @see BatchingType
     */
    ProducerConfiguration& setBatchingType(BatchingType batchingType);

    /**
     * @return batching type.
     * @see BatchingType.
     */
    BatchingType getBatchingType() const;

    /**
     * The getter associated with setCryptoKeyReader().
     */
    const CryptoKeyReaderPtr getCryptoKeyReader() const;

    /**
     * Set the shared pointer to CryptoKeyReader.
     *
     * @param shared pointer to CryptoKeyReader.
     * @return
     */
    ProducerConfiguration& setCryptoKeyReader(CryptoKeyReaderPtr cryptoKeyReader);

    /**
     * The getter associated with setCryptoFailureAction().
     */
    ProducerCryptoFailureAction getCryptoFailureAction() const;

    /**
     * Sets the ProducerCryptoFailureAction to the value specified.
     *
     * @param action
     *            the action taken by the producer in case of encryption failures.
     * @return
     */
    ProducerConfiguration& setCryptoFailureAction(ProducerCryptoFailureAction action);

    /**
     * @return all the encryption keys added
     */
    const std::set<std::string>& getEncryptionKeys() const;

    /**
     * @return true if encryption keys are added
     */
    bool isEncryptionEnabled() const;

    /**
     * Add public encryption key, used by producer to encrypt the data key.
     *
     * At the time of producer creation, Pulsar client checks if there are keys added to encryptionKeys. If
     * keys are found, a callback getKey(String keyName) is invoked against each key to load the values of the
     * key. Application should implement this callback to return the key in pkcs8 format. If compression is
     * enabled, message is encrypted after compression. If batch messaging is enabled, the batched message is
     * encrypted.
     *
     * @key the encryption key to add
     * @return the ProducerConfiguration self
     */
    ProducerConfiguration& addEncryptionKey(std::string key);

    /**
     * Check whether the producer has a specific property attached.
     *
     * @param name the name of the property to check
     * @return true if the message has the specified property
     * @return false if the property is not defined
     */
    bool hasProperty(const std::string& name) const;

    /**
     * Get the value of a specific property
     *
     * @param name the name of the property
     * @return the value of the property or null if the property was not defined
     */
    const std::string& getProperty(const std::string& name) const;

    /**
     * Get all the properties attached to this producer.
     */
    std::map<std::string, std::string>& getProperties() const;

    /**
     * Sets a new property on the producer
     * .
     * @param name   the name of the property
     * @param value  the associated value
     */
    ProducerConfiguration& setProperty(const std::string& name, const std::string& value);

    /**
     * Add all the properties in the provided map
     */
    ProducerConfiguration& setProperties(const std::map<std::string, std::string>& properties);

    /**
     * If message size is higher than allowed max publish-payload size by broker then enableChunking helps
     * producer to split message into multiple chunks and publish them to broker separately in order. So, it
     * allows client to successfully publish large size of messages in pulsar.
     *
     * Set it true to enable this feature. If so, you must disable batching (see setBatchingEnabled),
     * otherwise the producer creation will fail.
     *
     * There are some other recommendations when it's enabled:
     * 1. This features is right now only supported for non-shared subscription and persistent-topic.
     * 2. It's better to reduce setMaxPendingMessages to avoid producer accupying large amount of memory by
     * buffered messages.
     * 3. Set message-ttl on the namespace to cleanup chunked messages. Sometimes due to broker-restart or
     * publish time, producer might fail to publish entire large message. So, consumer will not be able to
     * consume and ack those messages.
     *
     * Default: false
     *
     * @param chunkingEnabled whether chunking is enabled
     * @return the ProducerConfiguration self
     */
    ProducerConfiguration& setChunkingEnabled(bool chunkingEnabled);

    /**
     * The getter associated with setChunkingEnabled().
     */
    bool isChunkingEnabled() const;

    friend class PulsarWrapper;

   private:
    struct Impl;
    std::shared_ptr<ProducerConfigurationImpl> impl_;
};
}  // namespace pulsar
#endif /* PULSAR_PRODUCERCONFIGURATION_H_ */

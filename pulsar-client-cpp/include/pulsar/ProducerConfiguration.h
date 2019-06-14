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

typedef std::function<void(Result, const Message& msg)> SendCallback;
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

    ProducerConfiguration();
    ~ProducerConfiguration();
    ProducerConfiguration(const ProducerConfiguration&);
    ProducerConfiguration& operator=(const ProducerConfiguration&);

    ProducerConfiguration& setProducerName(const std::string& producerName);
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

    ProducerConfiguration& setSendTimeout(int sendTimeoutMs);
    int getSendTimeout() const;

    ProducerConfiguration& setInitialSequenceId(int64_t initialSequenceId);
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
    CompressionType getCompressionType() const;

    ProducerConfiguration& setMaxPendingMessages(int maxPendingMessages);
    int getMaxPendingMessages() const;

    /**
     * Set the number of max pending messages across all the partitions
     * <p>
     * This setting will be used to lower the max pending messages for each partition
     * ({@link #setMaxPendingMessages(int)}), if the total exceeds the configured value.
     *
     * @param maxPendingMessagesAcrossPartitions
     */
    ProducerConfiguration& setMaxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions);

    /**
     *
     * @return the maximum number of pending messages allowed across all the partitions
     */
    int getMaxPendingMessagesAcrossPartitions() const;

    ProducerConfiguration& setPartitionsRoutingMode(const PartitionsRoutingMode& mode);
    PartitionsRoutingMode getPartitionsRoutingMode() const;

    ProducerConfiguration& setMessageRouter(const MessageRoutingPolicyPtr& router);
    const MessageRoutingPolicyPtr& getMessageRouterPtr() const;

    ProducerConfiguration& setHashingScheme(const HashingScheme& scheme);
    HashingScheme getHashingScheme() const;

    ProducerConfiguration& setBlockIfQueueFull(bool);
    bool getBlockIfQueueFull() const;

    // Zero queue size feature will not be supported on consumer end if batching is enabled
    ProducerConfiguration& setBatchingEnabled(const bool& batchingEnabled);
    const bool& getBatchingEnabled() const;

    ProducerConfiguration& setBatchingMaxMessages(const unsigned int& batchingMaxMessages);
    const unsigned int& getBatchingMaxMessages() const;

    ProducerConfiguration& setBatchingMaxAllowedSizeInBytes(
        const unsigned long& batchingMaxAllowedSizeInBytes);
    const unsigned long& getBatchingMaxAllowedSizeInBytes() const;

    ProducerConfiguration& setBatchingMaxPublishDelayMs(const unsigned long& batchingMaxPublishDelayMs);
    const unsigned long& getBatchingMaxPublishDelayMs() const;

    const CryptoKeyReaderPtr getCryptoKeyReader() const;
    ProducerConfiguration& setCryptoKeyReader(CryptoKeyReaderPtr cryptoKeyReader);

    ProducerCryptoFailureAction getCryptoFailureAction() const;
    ProducerConfiguration& setCryptoFailureAction(ProducerCryptoFailureAction action);

    std::set<std::string>& getEncryptionKeys();
    bool isEncryptionEnabled() const;
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

    friend class PulsarWrapper;

   private:
    struct Impl;
    std::shared_ptr<ProducerConfigurationImpl> impl_;
};
}  // namespace pulsar
#endif /* PULSAR_PRODUCERCONFIGURATION_H_ */

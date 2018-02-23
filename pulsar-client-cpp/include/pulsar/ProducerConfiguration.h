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
#include <pulsar/CompressionType.h>
#include <pulsar/MessageRoutingPolicy.h>
#include <pulsar/Result.h>
#include <pulsar/Message.h>
#include <boost/function.hpp>
#include <pulsar/ProducerCryptoFailureAction.h>
#include <pulsar/CryptoKeyReader.h>

#include <set>

#pragma GCC visibility push(default)

namespace pulsar {

typedef boost::function<void(Result, const Message& msg)> SendCallback;
typedef boost::function<void(Result)> CloseCallback;

class ProducerConfigurationImpl;
class PulsarWrapper;

/**
 * Class that holds the configuration for a producer
 */
class ProducerConfiguration {
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

    ProducerConfiguration& setSendTimeout(int sendTimeoutMs);
    int getSendTimeout() const;

    ProducerConfiguration& setInitialSequenceId(int64_t initialSequenceId);
    int64_t getInitialSequenceId() const;

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

    friend class PulsarWrapper;

   private:
    struct Impl;
    boost::shared_ptr<ProducerConfigurationImpl> impl_;
};
}  // namespace pulsar
#pragma GCC visibility pop
#endif /* PULSAR_PRODUCERCONFIGURATION_H_ */

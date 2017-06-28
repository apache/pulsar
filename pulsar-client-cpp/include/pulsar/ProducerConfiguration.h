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
    enum PartitionsRoutingMode {
        UseSinglePartition,
        RoundRobinDistribution,
        CustomPartition
    };
    ProducerConfiguration();
    ~ProducerConfiguration();
    ProducerConfiguration(const ProducerConfiguration&);
    ProducerConfiguration& operator=(const ProducerConfiguration&);

    ProducerConfiguration& setSendTimeout(int sendTimeoutMs);
    int getSendTimeout() const;

    ProducerConfiguration& setCompressionType(CompressionType compressionType);
    CompressionType getCompressionType() const;

    ProducerConfiguration& setMaxPendingMessages(int maxPendingMessages);
    int getMaxPendingMessages() const;

    ProducerConfiguration& setPartitionsRoutingMode(const PartitionsRoutingMode& mode);
    PartitionsRoutingMode getPartitionsRoutingMode() const;

    ProducerConfiguration& setMessageRouter(const MessageRoutingPolicyPtr& router);
    const MessageRoutingPolicyPtr& getMessageRouterPtr() const;

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

    ProducerConfiguration& setBatchingMaxPublishDelayMs(
            const unsigned long& batchingMaxPublishDelayMs);
    const unsigned long& getBatchingMaxPublishDelayMs() const;

    friend class PulsarWrapper;

 private:
    struct Impl;
    boost::shared_ptr<ProducerConfigurationImpl> impl_;
};
}
#pragma GCC visibility pop
#endif /* PULSAR_PRODUCERCONFIGURATION_H_ */


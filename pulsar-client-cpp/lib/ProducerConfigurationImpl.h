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
#ifndef LIB_PRODUCERCONFIGURATIONIMPL_H_
#define LIB_PRODUCERCONFIGURATIONIMPL_H_

#include <pulsar/ProducerConfiguration.h>
#include <memory>

#include "Utils.h"

namespace pulsar {

struct ProducerConfigurationImpl {
    SchemaInfo schemaInfo;
    Optional<std::string> producerName;
    Optional<int64_t> initialSequenceId;
    int sendTimeoutMs;
    CompressionType compressionType;
    int maxPendingMessages;
    int maxPendingMessagesAcrossPartitions;
    ProducerConfiguration::PartitionsRoutingMode routingMode;
    MessageRoutingPolicyPtr messageRouter;
    ProducerConfiguration::HashingScheme hashingScheme;
    bool blockIfQueueFull;
    bool batchingEnabled;
    unsigned int batchingMaxMessages;
    unsigned long batchingMaxAllowedSizeInBytes;
    unsigned long batchingMaxPublishDelayMs;
    CryptoKeyReaderPtr cryptoKeyReader;
    std::set<std::string> encryptionKeys;
    ProducerCryptoFailureAction cryptoFailureAction;
    std::map<std::string, std::string> properties;
    ProducerConfigurationImpl()
        : schemaInfo(),
          sendTimeoutMs(30000),
          compressionType(CompressionNone),
          maxPendingMessages(1000),
          maxPendingMessagesAcrossPartitions(50000),
          routingMode(ProducerConfiguration::UseSinglePartition),
          hashingScheme(ProducerConfiguration::BoostHash),
          blockIfQueueFull(false),
          batchingEnabled(true),
          batchingMaxMessages(1000),
          batchingMaxAllowedSizeInBytes(128 * 1024),  // 128 KB
          batchingMaxPublishDelayMs(10),              // 10 milli seconds
          cryptoKeyReader(),
          encryptionKeys(),
          cryptoFailureAction(ProducerCryptoFailureAction::FAIL) {}
};
}  // namespace pulsar

#endif /* LIB_PRODUCERCONFIGURATIONIMPL_H_ */

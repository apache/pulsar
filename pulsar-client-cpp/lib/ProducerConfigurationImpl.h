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
    int sendTimeoutMs{30000};
    CompressionType compressionType{CompressionNone};
    int maxPendingMessages{1000};
    int maxPendingMessagesAcrossPartitions{50000};
    ProducerConfiguration::PartitionsRoutingMode routingMode{ProducerConfiguration::UseSinglePartition};
    MessageRoutingPolicyPtr messageRouter;
    ProducerConfiguration::HashingScheme hashingScheme{ProducerConfiguration::BoostHash};
    bool useLazyStartPartitionedProducers{false};
    bool blockIfQueueFull{false};
    bool batchingEnabled{true};
    unsigned int batchingMaxMessages{1000};
    unsigned long batchingMaxAllowedSizeInBytes{128 * 1024};  // 128 KB
    unsigned long batchingMaxPublishDelayMs{10};              // 10 milli seconds
    ProducerConfiguration::BatchingType batchingType{ProducerConfiguration::DefaultBatching};
    CryptoKeyReaderPtr cryptoKeyReader;
    std::set<std::string> encryptionKeys;
    ProducerCryptoFailureAction cryptoFailureAction{ProducerCryptoFailureAction::FAIL};
    std::map<std::string, std::string> properties;
    bool chunkingEnabled{false};
};
}  // namespace pulsar

#endif /* LIB_PRODUCERCONFIGURATIONIMPL_H_ */

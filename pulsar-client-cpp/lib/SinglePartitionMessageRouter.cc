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
#include "SinglePartitionMessageRouter.h"

#include <chrono>
#include <random>

namespace pulsar {
SinglePartitionMessageRouter::~SinglePartitionMessageRouter() {}

SinglePartitionMessageRouter::SinglePartitionMessageRouter(const int numberOfPartitions,
                                                           ProducerConfiguration::HashingScheme hashingScheme)
    : MessageRouterBase(hashingScheme) {
    std::default_random_engine generator(
        std::chrono::high_resolution_clock::now().time_since_epoch().count());
    selectedSinglePartition_ = generator() % numberOfPartitions;
}

SinglePartitionMessageRouter::SinglePartitionMessageRouter(const int partitionIndex,
                                                           const int numberOfPartitions,
                                                           ProducerConfiguration::HashingScheme hashingScheme)
    : MessageRouterBase(hashingScheme) {
    selectedSinglePartition_ = partitionIndex;
}

// override
int SinglePartitionMessageRouter::getPartition(const Message& msg, const TopicMetadata& topicMetadata) {
    // if message has a key, hash the key and return the partition
    if (msg.hasPartitionKey()) {
        return hash->makeHash(msg.getPartitionKey()) % topicMetadata.getNumPartitions();
    } else {
        // else pick the next partition
        return selectedSinglePartition_;
    }
}
}  // namespace pulsar

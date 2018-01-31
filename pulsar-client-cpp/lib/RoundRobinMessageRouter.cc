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
#include "RoundRobinMessageRouter.h"

namespace pulsar {
RoundRobinMessageRouter::RoundRobinMessageRouter(ProducerConfiguration::HashingScheme hashingScheme)
    : MessageRouterBase(hashingScheme), prevPartition_(0) {}

RoundRobinMessageRouter::~RoundRobinMessageRouter() {}

// override
int RoundRobinMessageRouter::getPartition(const Message& msg, const TopicMetadata& topicMetadata) {
    // if message has a key, hash the key and return the partition
    if (msg.hasPartitionKey()) {
        return hash->makeHash(msg.getPartitionKey()) % topicMetadata.getNumPartitions();
    } else {
        Lock lock(mutex_);
        // else pick the next partition
        return prevPartition_++ % topicMetadata.getNumPartitions();
    }
}

}  // namespace pulsar

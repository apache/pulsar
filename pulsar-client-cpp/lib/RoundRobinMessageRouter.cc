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

#include "TimeUtils.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

namespace pulsar {
RoundRobinMessageRouter::RoundRobinMessageRouter(ProducerConfiguration::HashingScheme hashingScheme,
                                                 bool batchingEnabled, uint32_t maxBatchingMessages,
                                                 uint32_t maxBatchingSize,
                                                 boost::posix_time::time_duration maxBatchingDelay)
    : MessageRouterBase(hashingScheme),
      batchingEnabled_(batchingEnabled),
      maxBatchingMessages_(maxBatchingMessages),
      maxBatchingSize_(maxBatchingSize),
      maxBatchingDelay_(maxBatchingDelay),
      lastPartitionChange_(TimeUtils::currentTimeMillis()),
      msgCounter_(0),
      cumulativeBatchSize_(0) {
    boost::random::mt19937 rng(time(nullptr));
    boost::random::uniform_int_distribution<int> dist;
    currentPartitionCursor_ = dist(rng);
}

RoundRobinMessageRouter::~RoundRobinMessageRouter() {}

// override
int RoundRobinMessageRouter::getPartition(const Message& msg, const TopicMetadata& topicMetadata) {
    if (topicMetadata.getNumPartitions() == 1) {
        // When there are no partitions, don't even bother
        return 0;
    }

    // if message has a key, hash the key and return the partition
    if (msg.hasPartitionKey()) {
        return hash->makeHash(msg.getPartitionKey()) % topicMetadata.getNumPartitions();
    }

    if (!batchingEnabled_) {
        // If there's no batching, do the round-robin at the message scope
        // as there is no gain otherwise.
        return currentPartitionCursor_++ % topicMetadata.getNumPartitions();
    }

    // If there's no key, we do round-robin across partition, sticking with a given
    // partition for a certain amount of messages or volume buffered or the max delay to batch is reached so
    // that we ensure having a decent amount of batching of the messages. Note that it is possible that we
    // skip more than one partition if multiple goroutines increment currentPartitionCursor at the same time.
    // If that happens it shouldn't be a problem because we only want to spread the data on different
    // partitions but not necessarily in a specific sequence.
    uint32_t messageSize = msg.getLength();
    uint32_t messageCount = msgCounter_;
    uint32_t batchSize = cumulativeBatchSize_;
    int64_t lastPartitionChange = lastPartitionChange_;
    int64_t now = TimeUtils::currentTimeMillis();

    if (messageCount >= maxBatchingMessages_ || (messageSize >= maxBatchingSize_ - batchSize) ||
        (now - lastPartitionChange >= maxBatchingDelay_.total_milliseconds())) {
        uint32_t currentPartitionCursor = ++currentPartitionCursor_;
        lastPartitionChange_ = now;
        cumulativeBatchSize_ = messageSize;
        msgCounter_ = 1;
        return currentPartitionCursor % topicMetadata.getNumPartitions();
    }

    ++msgCounter_;
    cumulativeBatchSize_ += messageSize;
    return currentPartitionCursor_ % topicMetadata.getNumPartitions();
}

}  // namespace pulsar

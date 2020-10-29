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

#pragma once

#include <pulsar/defines.h>
#include <pulsar/MessageRoutingPolicy.h>
#include <pulsar/ProducerConfiguration.h>
#include <pulsar/TopicMetadata.h>
#include "Hash.h"
#include "MessageRouterBase.h"

#include <atomic>
#include <boost/date_time/local_time/local_time.hpp>

namespace pulsar {
class PULSAR_PUBLIC RoundRobinMessageRouter : public MessageRouterBase {
   public:
    RoundRobinMessageRouter(ProducerConfiguration::HashingScheme hashingScheme, bool batchingEnabled,
                            uint32_t maxBatchingMessages, uint32_t maxBatchingSize,
                            boost::posix_time::time_duration maxBatchingDelay);
    virtual ~RoundRobinMessageRouter();
    virtual int getPartition(const Message& msg, const TopicMetadata& topicMetadata);

   private:
    const bool batchingEnabled_;
    const uint32_t maxBatchingMessages_;
    const uint32_t maxBatchingSize_;
    const boost::posix_time::time_duration maxBatchingDelay_;

    std::atomic<uint32_t> currentPartitionCursor_;
    std::atomic<int64_t> lastPartitionChange_;
    std::atomic<uint32_t> msgCounter_;
    std::atomic<uint32_t> cumulativeBatchSize_;
};

}  // namespace pulsar

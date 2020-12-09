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
#ifndef CUSTOM_ROUTER_POLICY_HEADER_
#define CUSTOM_ROUTER_POLICY_HEADER_

#include <cstdlib>  // rand()
#include <boost/algorithm/string.hpp>
#include <pulsar/DeprecatedException.h>

namespace pulsar {
class CustomRoutingPolicy : public MessageRoutingPolicy {
    /** @deprecated */
    int getPartition(const Message& msg) {
        throw DeprecatedException("Use getPartition(const Message&, const TopicMetadata&) instead.");
    }

    int getPartition(const Message& msg, const TopicMetadata& topicMetadata) { return 0; }
};

class SimpleRoundRobinRoutingPolicy : public MessageRoutingPolicy {
   public:
    SimpleRoundRobinRoutingPolicy() : counter_(0) {}

    int getPartition(const Message& msg, const TopicMetadata& topicMetadata) {
        return counter_++ % topicMetadata.getNumPartitions();
    }

   private:
    uint32_t counter_;
};

}  // namespace pulsar

#endif  // CUSTOM_ROUTER_POLICY_HEADER_

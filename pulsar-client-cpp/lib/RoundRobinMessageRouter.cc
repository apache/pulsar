/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "RoundRobinMessageRouter.h"
#include <boost/algorithm/string.hpp>

namespace pulsar {
    RoundRobinMessageRouter::RoundRobinMessageRouter(unsigned int numPartitions):prevPartition_(0), numPartitions_(numPartitions) {
    }

    RoundRobinMessageRouter::~RoundRobinMessageRouter() {
    }

    //override
    int RoundRobinMessageRouter::getPartition(const Message& msg) {
        //if message has a key, hash the key and return the partition
        if (msg.hasPartitionKey()) {
            static StringHash hash;
            return hash(msg.getPartitionKey()) % numPartitions_;
        } else {
            Lock lock(mutex_);
            //else pick the next partition
            return prevPartition_++ % numPartitions_;
        }
    }

}

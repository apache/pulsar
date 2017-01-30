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

#include "SinglePartitionMessageRouter.h"
#include <cstdlib> // rand()
#include <boost/algorithm/string.hpp>
namespace pulsar {
    SinglePartitionMessageRouter::~SinglePartitionMessageRouter(){}
    SinglePartitionMessageRouter::SinglePartitionMessageRouter(unsigned int numPartitions):numPartitions_(numPartitions) {
        unsigned int random = rand();
        selectedSinglePartition_ = random % numPartitions_;
    }

    //override
    int SinglePartitionMessageRouter::getPartition(const Message& msg) {
        //if message has a key, hash the key and return the partition
        if (msg.hasPartitionKey()) {
            StringHash hash;
            return hash(msg.getPartitionKey()) % numPartitions_;
        } else {
            //else pick the next partition
            return selectedSinglePartition_;
        }
    }
}

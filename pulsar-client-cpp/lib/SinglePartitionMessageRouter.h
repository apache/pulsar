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
#ifndef PULSAR_SINGLE_PARTITION_MESSAGE_ROUTER_HEADER_
#define PULSAR_SINGLE_PARTITION_MESSAGE_ROUTER_HEADER_

#include <pulsar/defines.h>
#include <pulsar/MessageRoutingPolicy.h>
#include <include/pulsar/ProducerConfiguration.h>
#include "Hash.h"
#include <pulsar/TopicMetadata.h>
#include "MessageRouterBase.h"

namespace pulsar {

class PULSAR_PUBLIC SinglePartitionMessageRouter : public MessageRouterBase {
   public:
    SinglePartitionMessageRouter(const int partitionIndex, const int numberOfPartitions,
                                 ProducerConfiguration::HashingScheme hashingScheme);
    SinglePartitionMessageRouter(const int numberOfPartitions,
                                 ProducerConfiguration::HashingScheme hashingScheme);
    virtual ~SinglePartitionMessageRouter();
    virtual int getPartition(const Message& msg, const TopicMetadata& topicMetadata);

   private:
    int selectedSinglePartition_;
};

}  // namespace pulsar
#endif  // PULSAR_SINGLE_PARTITION_MESSAGE_ROUTER_HEADER_

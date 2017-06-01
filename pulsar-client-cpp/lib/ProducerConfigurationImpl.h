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

#ifndef LIB_PRODUCERCONFIGURATIONIMPL_H_
#define LIB_PRODUCERCONFIGURATIONIMPL_H_

#include <pulsar/ProducerConfiguration.h>
#include <boost/make_shared.hpp>

namespace pulsar {

struct ProducerConfigurationImpl {
    int sendTimeoutMs;
    CompressionType compressionType;
    int maxPendingMessages;
    ProducerConfiguration::PartitionsRoutingMode routingMode;
    MessageRoutingPolicyPtr messageRouter;
    bool blockIfQueueFull;
    bool batchingEnabled;
    unsigned int batchingMaxMessages;
    unsigned long batchingMaxAllowedSizeInBytes;
    unsigned long batchingMaxPublishDelayMs;
    ProducerConfigurationImpl()
            : sendTimeoutMs(30000),
              compressionType(CompressionNone),
              maxPendingMessages(30000),
              routingMode(ProducerConfiguration::UseSinglePartition),
              blockIfQueueFull(true),
              batchingEnabled(false),
              batchingMaxMessages(1000),
              batchingMaxAllowedSizeInBytes(128 * 1024), // 128 KB
              batchingMaxPublishDelayMs(3000) { // 3 seconds
    }
};
}



#endif /* LIB_PRODUCERCONFIGURATIONIMPL_H_ */

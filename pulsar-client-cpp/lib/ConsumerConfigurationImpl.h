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
#ifndef LIB_CONSUMERCONFIGURATIONIMPL_H_
#define LIB_CONSUMERCONFIGURATIONIMPL_H_

#include <pulsar/ConsumerConfiguration.h>

#include <chrono>

namespace pulsar {
struct ConsumerConfigurationImpl {
    SchemaInfo schemaInfo;
    long unAckedMessagesTimeoutMs{0};
    long tickDurationInMs{1000};

    long negativeAckRedeliveryDelayMs{60000};
    long ackGroupingTimeMs{100};
    long ackGroupingMaxSize{1000};
    ConsumerType consumerType{ConsumerExclusive};
    MessageListener messageListener;
    bool hasMessageListener{false};
    ConsumerEventListenerPtr eventListener;
    bool hasConsumerEventListener{false};
    int receiverQueueSize{1000};
    int maxTotalReceiverQueueSizeAcrossPartitions{50000};
    std::string consumerName;
    long brokerConsumerStatsCacheTimeInMs{30 * 1000L};  // 30 seconds
    CryptoKeyReaderPtr cryptoKeyReader;
    ConsumerCryptoFailureAction cryptoFailureAction{ConsumerCryptoFailureAction::FAIL};
    bool readCompacted{false};
    InitialPosition subscriptionInitialPosition{InitialPosition::InitialPositionLatest};
    int patternAutoDiscoveryPeriod{60};
    bool replicateSubscriptionStateEnabled{false};
    std::map<std::string, std::string> properties;
    int priorityLevel{0};
    KeySharedPolicy keySharedPolicy;
    size_t maxPendingChunkedMessage{10};
    bool autoAckOldestChunkedMessageOnQueueFull{false};
};
}  // namespace pulsar
#endif /* LIB_CONSUMERCONFIGURATIONIMPL_H_ */

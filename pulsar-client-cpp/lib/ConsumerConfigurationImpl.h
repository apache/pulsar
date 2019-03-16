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
    long unAckedMessagesTimeoutMs;

    std::chrono::milliseconds negativeAckRedeliveryDelay;
    ConsumerType consumerType;
    MessageListener messageListener;
    bool hasMessageListener;
    int receiverQueueSize;
    int maxTotalReceiverQueueSizeAcrossPartitions;
    std::string consumerName;
    long brokerConsumerStatsCacheTimeInMs;
    CryptoKeyReaderPtr cryptoKeyReader;
    ConsumerCryptoFailureAction cryptoFailureAction;
    bool readCompacted;
    InitialPosition subscriptionInitialPosition;
    int patternAutoDiscoveryPeriod;
    std::map<std::string, std::string> properties;
    ConsumerConfigurationImpl()
        : schemaInfo(),
          unAckedMessagesTimeoutMs(0),
          consumerType(ConsumerExclusive),
          messageListener(),
          hasMessageListener(false),
          brokerConsumerStatsCacheTimeInMs(30 * 1000),  // 30 seconds
          receiverQueueSize(1000),
          maxTotalReceiverQueueSizeAcrossPartitions(50000),
          cryptoKeyReader(),
          cryptoFailureAction(ConsumerCryptoFailureAction::FAIL),
          readCompacted(false),
          subscriptionInitialPosition(InitialPosition::InitialPositionLatest),
          patternAutoDiscoveryPeriod(60),
          properties() {}
};
}  // namespace pulsar
#endif /* LIB_CONSUMERCONFIGURATIONIMPL_H_ */

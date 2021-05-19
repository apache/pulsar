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
#ifndef LIB_READERCONFIGURATIONIMPL_H_
#define LIB_READERCONFIGURATIONIMPL_H_

#include <pulsar/ReaderConfiguration.h>

namespace pulsar {
struct ReaderConfigurationImpl {
    SchemaInfo schemaInfo;
    ReaderListener readerListener;
    bool hasReaderListener{false};
    int receiverQueueSize{1000};
    std::string readerName;
    std::string subscriptionRolePrefix;
    bool readCompacted{false};
    std::string internalSubscriptionName;
    long unAckedMessagesTimeoutMs{0};
    long tickDurationInMs{1000};
    long ackGroupingTimeMs{100};
    long ackGroupingMaxSize{1000};
    CryptoKeyReaderPtr cryptoKeyReader;
    ConsumerCryptoFailureAction cryptoFailureAction;
    std::map<std::string, std::string> properties;
};
}  // namespace pulsar
#endif /* LIB_READERCONFIGURATIONIMPL_H_ */

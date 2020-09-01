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
#include "BatchMessageContainerBase.h"
#include "MessageCrypto.h"
#include "ProducerImpl.h"
#include "SharedBuffer.h"
#include "PulsarApi.pb.h"

namespace pulsar {

BatchMessageContainerBase::BatchMessageContainerBase(const ProducerImpl& producer)
    : topicName_(producer.topic_),
      producerConfig_(producer.conf_),
      producerName_(producer.producerName_),
      producerId_(producer.producerId_),
      msgCryptoWeakPtr_(producer.msgCrypto_) {}

bool BatchMessageContainerBase::encryptMessage(proto::MessageMetadata& metadata, SharedBuffer& payload,
                                               SharedBuffer& encryptedPayload) const {
    auto msgCrypto = msgCryptoWeakPtr_.lock();
    if (!msgCrypto || producerConfig_.isEncryptionEnabled()) {
        encryptedPayload = payload;
        return true;
    }

    return msgCrypto->encrypt(producerConfig_.getEncryptionKeys(), producerConfig_.getCryptoKeyReader(),
                              metadata, payload, encryptedPayload);
}

}  // namespace pulsar

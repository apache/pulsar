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
#include <pulsar/BatchMessageId.h>
#include <lib/ProducerImpl.h>
#include <lib/ConsumerImpl.h>
#include <string>

using std::string;

namespace pulsar{
class PulsarFriend {
    public:
    static int getBatchIndex(const BatchMessageId& mId) {
        return mId.batchIndex_;
    }

    static ProducerStatsImplPtr getProducerStatsPtr(Producer producer) {
        ProducerImpl* producerImpl = static_cast<ProducerImpl*>(producer.impl_.get());
        return boost::static_pointer_cast<ProducerStatsImpl>(producerImpl->producerStatsBasePtr_);
    }

    template<typename T>
    static unsigned long sum(std::map<T, unsigned long> m) {
        unsigned long sum = 0;
        for (typename std::map<T, unsigned long>::iterator iter = m.begin(); iter != m.end(); iter++) {
            sum += iter->second;
        }
        return sum;
    }

    static ConsumerStatsImplPtr getConsumerStatsPtr(Consumer consumer) {
        ConsumerImpl* consumerImpl = static_cast<ConsumerImpl*>(consumer.impl_.get());
        return boost::static_pointer_cast<ConsumerStatsImpl>(consumerImpl->consumerStatsBasePtr_);
    }

    static ProducerImpl& getProducerImpl(Producer producer) {
        ProducerImpl* producerImpl = static_cast<ProducerImpl*>(producer.impl_.get());
        return *producerImpl;
    }

    static ConsumerImpl& getConsumerImpl(Consumer consumer) {
        ConsumerImpl* consumerImpl = static_cast<ConsumerImpl*>(consumer.impl_.get());
        return *consumerImpl;
    }

    static ClientConnectionWeakPtr getClientConnection(HandlerBase&  handler) {
        return handler.connection_;
    }
};
}

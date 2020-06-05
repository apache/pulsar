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

#include <lib/ProducerImpl.h>
#include <lib/PartitionedProducerImpl.h>
#include <lib/ConsumerImpl.h>
#include <lib/ClientImpl.h>
#include <string>

using std::string;

namespace pulsar {
class PulsarFriend {
   public:
    static MessageId getMessageId(int32_t partition, int64_t ledgerId, int64_t entryId, int32_t batchIndex) {
        return MessageId(partition, ledgerId, entryId, batchIndex);
    }

    static int getBatchIndex(const MessageId& mId) { return mId.batchIndex(); }

    static ProducerStatsImplPtr getProducerStatsPtr(Producer producer) {
        ProducerImpl* producerImpl = static_cast<ProducerImpl*>(producer.impl_.get());
        return std::static_pointer_cast<ProducerStatsImpl>(producerImpl->producerStatsBasePtr_);
    }

    static std::vector<ProducerImpl::MessageQueue*> getProducerMessageQueue(Producer producer,
                                                                            ConsumerTopicType type) {
        ProducerImplBasePtr producerBaseImpl = producer.impl_;
        if (type == Partitioned) {
            std::vector<ProducerImpl::MessageQueue*> queues;
            for (const auto& producer :
                 std::static_pointer_cast<PartitionedProducerImpl>(producerBaseImpl)->producers_) {
                queues.emplace_back(&producer->pendingMessagesQueue_);
            }
            return queues;
        } else {
            return {&std::static_pointer_cast<ProducerImpl>(producerBaseImpl)->pendingMessagesQueue_};
        }
    }

    template <typename T>
    static unsigned long sum(std::map<T, unsigned long> m) {
        unsigned long sum = 0;
        for (typename std::map<T, unsigned long>::iterator iter = m.begin(); iter != m.end(); iter++) {
            sum += iter->second;
        }
        return sum;
    }

    static ConsumerStatsImplPtr getConsumerStatsPtr(Consumer consumer) {
        ConsumerImpl* consumerImpl = static_cast<ConsumerImpl*>(consumer.impl_.get());
        return std::static_pointer_cast<ConsumerStatsImpl>(consumerImpl->consumerStatsBasePtr_);
    }

    static ProducerImpl& getProducerImpl(Producer producer) {
        ProducerImpl* producerImpl = static_cast<ProducerImpl*>(producer.impl_.get());
        return *producerImpl;
    }

    static void producerFailMessages(Producer producer, Result result) {
        producer.producerFailMessages(result);
    }

    static ConsumerImpl& getConsumerImpl(Consumer consumer) {
        ConsumerImpl* consumerImpl = static_cast<ConsumerImpl*>(consumer.impl_.get());
        return *consumerImpl;
    }

    static std::shared_ptr<ClientImpl> getClientImplPtr(Client client) { return client.impl_; }

    static void setNegativeAckEnabled(Consumer consumer, bool enabled) {
        consumer.impl_->setNegativeAcknowledgeEnabledForTesting(enabled);
    }

    static ClientConnectionWeakPtr getClientConnection(HandlerBase& handler) { return handler.connection_; }

    static boost::posix_time::ptime& getFirstBackoffTime(Backoff& backoff) {
        return backoff.firstBackoffTime_;
    }
};
}  // namespace pulsar

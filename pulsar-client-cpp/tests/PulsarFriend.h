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

#include <string>

#include "lib/ClientImpl.h"
#include "lib/ProducerImpl.h"
#include "lib/PartitionedProducerImpl.h"
#include "lib/ConsumerImpl.h"
#include "lib/MultiTopicsConsumerImpl.h"
#include "lib/ReaderImpl.h"

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

    static ProducerImpl& getInternalProducerImpl(Producer producer, int index) {
        PartitionedProducerImpl* producerImpl = static_cast<PartitionedProducerImpl*>(producer.impl_.get());
        return *(producerImpl->producers_[index]);
    }

    static void producerFailMessages(Producer producer, Result result) {
        producer.producerFailMessages(result);
    }

    static ConsumerImpl& getConsumerImpl(Consumer consumer) {
        ConsumerImpl* consumerImpl = static_cast<ConsumerImpl*>(consumer.impl_.get());
        return *consumerImpl;
    }

    static std::shared_ptr<ConsumerImpl> getConsumerImplPtr(Consumer consumer) {
        return std::static_pointer_cast<ConsumerImpl>(consumer.impl_);
    }

    static ConsumerImplPtr getConsumer(Reader reader) {
        return std::static_pointer_cast<ConsumerImpl>(reader.impl_->getConsumer().lock());
    }

    static ReaderImplWeakPtr getReaderImplWeakPtr(Reader reader) { return reader.impl_; }

    static decltype(ConsumerImpl::chunkedMessageCache_) & getChunkedMessageCache(Consumer consumer) {
        auto consumerImpl = getConsumerImplPtr(consumer);
        ConsumerImpl::Lock lock(consumerImpl->chunkProcessMutex_);
        return consumerImpl->chunkedMessageCache_;
    }

    static std::shared_ptr<MultiTopicsConsumerImpl> getMultiTopicsConsumerImplPtr(Consumer consumer) {
        return std::static_pointer_cast<MultiTopicsConsumerImpl>(consumer.impl_);
    }

    static std::shared_ptr<ClientImpl> getClientImplPtr(Client client) { return client.impl_; }

    static ClientImpl::ProducersList& getProducers(const Client& client) {
        return getClientImplPtr(client)->producers_;
    }

    static ClientImpl::ConsumersList& getConsumers(const Client& client) {
        return getClientImplPtr(client)->consumers_;
    }

    static void setNegativeAckEnabled(Consumer consumer, bool enabled) {
        consumer.impl_->setNegativeAcknowledgeEnabledForTesting(enabled);
    }

    static ClientConnectionWeakPtr getClientConnection(HandlerBase& handler) { return handler.connection_; }

    static boost::posix_time::ptime& getFirstBackoffTime(Backoff& backoff) {
        return backoff.firstBackoffTime_;
    }
};
}  // namespace pulsar

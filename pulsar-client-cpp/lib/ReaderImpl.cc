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

#include "ClientImpl.h"
#include "ReaderImpl.h"

namespace pulsar {

static ResultCallback emptyCallback;

ReaderImpl::ReaderImpl(const ClientImplPtr client, const std::string& topic, const ReaderConfiguration& conf,
                       const ExecutorServicePtr listenerExecutor, ReaderCallback readerCreatedCallback)
    : topic_(topic), client_(client), readerConf_(conf), readerCreatedCallback_(readerCreatedCallback) {}

void ReaderImpl::start(const MessageId& startMessageId) {
    ConsumerConfiguration consumerConf;
    consumerConf.setConsumerType(ConsumerExclusive);
    consumerConf.setReceiverQueueSize(readerConf_.getReceiverQueueSize());
    consumerConf.setReadCompacted(readerConf_.isReadCompacted());
    consumerConf.setSchema(readerConf_.getSchema());

    if (readerConf_.getReaderName().length() > 0) {
        consumerConf.setConsumerName(readerConf_.getReaderName());
    }

    if (readerConf_.hasReaderListener()) {
        // Adapt the message listener to be a reader-listener
        readerListener_ = readerConf_.getReaderListener();
        consumerConf.setMessageListener(std::bind(&ReaderImpl::messageListener, shared_from_this(),
                                                  std::placeholders::_1, std::placeholders::_2));
    }

    std::string subscription = "reader-" + generateRandomName();
    if (!readerConf_.getSubscriptionRolePrefix().empty()) {
        subscription = readerConf_.getSubscriptionRolePrefix() + "-" + subscription;
    }

    consumer_ = std::make_shared<ConsumerImpl>(
        client_.lock(), topic_, subscription, consumerConf, ExecutorServicePtr(), NonPartitioned,
        Commands::SubscriptionModeNonDurable, Optional<MessageId>::of(startMessageId));
    consumer_->getConsumerCreatedFuture().addListener(std::bind(&ReaderImpl::handleConsumerCreated,
                                                                shared_from_this(), std::placeholders::_1,
                                                                std::placeholders::_2));
    consumer_->start();
}

const std::string& ReaderImpl::getTopic() const { return consumer_->getTopic(); }

void ReaderImpl::handleConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumer) {
    readerCreatedCallback_(result, Reader(shared_from_this()));
}

ConsumerImplPtr ReaderImpl::getConsumer() { return consumer_; }

Result ReaderImpl::readNext(Message& msg) {
    Result res = consumer_->receive(msg);
    acknowledgeIfNecessary(res, msg);
    return res;
}

Result ReaderImpl::readNext(Message& msg, int timeoutMs) {
    Result res = consumer_->receive(msg, timeoutMs);
    acknowledgeIfNecessary(res, msg);
    return res;
}

void ReaderImpl::messageListener(Consumer consumer, const Message& msg) {
    readerListener_(Reader(shared_from_this()), msg);
    acknowledgeIfNecessary(ResultOk, msg);
}

void ReaderImpl::acknowledgeIfNecessary(Result result, const Message& msg) {
    if (result != ResultOk) {
        return;
    }

    // Only acknowledge on the first message in the batch
    if (msg.getMessageId().batchIndex() <= 0) {
        // Acknowledge message immediately because the reader is based on non-durable
        // subscription. When it reconnects, it will specify the subscription position anyway
        consumer_->acknowledgeCumulativeAsync(msg.getMessageId(), emptyCallback);
    }
}

void ReaderImpl::closeAsync(ResultCallback callback) { consumer_->closeAsync(callback); }

void ReaderImpl::hasMessageAvailableAsync(HasMessageAvailableCallback callback) {
    consumer_->hasMessageAvailableAsync(callback);
}

}  // namespace pulsar

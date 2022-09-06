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
#include "ConsumerImpl.h"
#include "MessageImpl.h"
#include "MessagesImpl.h"
#include "LogUtils.h"
#include "TimeUtils.h"
#include "pulsar/Result.h"
#include "MessageIdUtil.h"
#include "AckGroupingTracker.h"
#include "ConsumerImplBase.h"

#include <algorithm>

DECLARE_LOG_OBJECT()

namespace pulsar {

ConsumerImplBase::ConsumerImplBase(ClientImplPtr client, const std::string& topic, Backoff backoff,
                                   const ConsumerConfiguration& conf, ExecutorServicePtr listenerExecutor)
    : HandlerBase(client, topic, backoff),
      listenerExecutor_(listenerExecutor),
      batchReceivePolicy_(conf.getBatchReceivePolicy()) {
    auto userBatchReceivePolicy = conf.getBatchReceivePolicy();
    if (userBatchReceivePolicy.getMaxNumMessages() > conf.getReceiverQueueSize()) {
        batchReceivePolicy_ =
            BatchReceivePolicy(conf.getReceiverQueueSize(), userBatchReceivePolicy.getMaxNumBytes(),
                               userBatchReceivePolicy.getTimeoutMs());
        LOG_WARN("BatchReceivePolicy maxNumMessages: {" << userBatchReceivePolicy.getMaxNumMessages()
                                                        << "} is greater than maxReceiverQueueSize: {"
                                                        << conf.getReceiverQueueSize()
                                                        << "}, reset to "
                                                           "maxReceiverQueueSize. ");
    }
    batchReceiveTimer_ = listenerExecutor_->createDeadlineTimer();
}

void ConsumerImplBase::triggerBatchReceiveTimerTask(long timeoutMs) {
    if (timeoutMs > 0) {
        batchReceiveTimer_->expires_from_now(boost::posix_time::milliseconds(timeoutMs));
        std::weak_ptr<ConsumerImplBase> weakSelf{shared_from_this()};
        batchReceiveTimer_->async_wait([weakSelf](const boost::system::error_code& ec) {
            // If two requests call runPartitionUpdateTask at the same time, the timer will fail, and it
            // cannot continue at this time, and the request needs to be ignored.
            auto self = weakSelf.lock();
            if (self && !ec) {
                self->doBatchReceiveTimeTask();
            }
        });
    }
}

void ConsumerImplBase::doBatchReceiveTimeTask() {
    if (state_ != Ready) {
        return;
    }

    bool hasPendingReceives = false;
    long timeToWaitMs = batchReceivePolicy_.getTimeoutMs();

    Lock lock(batchPendingReceiveMutex_);
    while (!batchPendingReceives_.empty()) {
        OpBatchReceive& batchReceive = batchPendingReceives_.front();
        long diff =
            batchReceivePolicy_.getTimeoutMs() - (TimeUtils::currentTimeMillis() - batchReceive.createAt_);
        if (diff <= 0) {
            notifyBatchPendingReceivedCallback(batchReceive.batchReceiveCallback_);
            batchPendingReceives_.pop();
        } else {
            hasPendingReceives = true;
            timeToWaitMs = diff;
            break;
        }
    }
    lock.unlock();

    if (hasPendingReceives) {
        triggerBatchReceiveTimerTask(timeToWaitMs);
    }
}

void ConsumerImplBase::failPendingBatchReceiveCallback() {
    Messages msgs;
    Lock lock(batchPendingReceiveMutex_);
    while (!batchPendingReceives_.empty()) {
        OpBatchReceive opBatchReceive = batchPendingReceives_.front();
        batchPendingReceives_.pop();
        auto self = shared_from_this();
        listenerExecutor_->postWork([opBatchReceive, self, msgs]() {
            opBatchReceive.batchReceiveCallback_(ResultAlreadyClosed, msgs);
        });
    }
    lock.unlock();
}

void ConsumerImplBase::notifyBatchPendingReceivedCallback() {
    Lock lock(batchPendingReceiveMutex_);
    if (!batchPendingReceives_.empty()) {
        OpBatchReceive& batchReceive = batchPendingReceives_.front();
        batchPendingReceives_.pop();
        notifyBatchPendingReceivedCallback(batchReceive.batchReceiveCallback_);
    }
}

void ConsumerImplBase::batchReceiveAsync(BatchReceiveCallback callback) {
    // fail the callback if consumer is closing or closed
    if (state_ != Ready) {
        callback(ResultAlreadyClosed, Messages());
        return;
    }

    if (hasEnoughMessagesForBatchReceive()) {
        Lock lock(batchPendingReceiveMutex_);
        notifyBatchPendingReceivedCallback(callback);
        lock.unlock();
    } else {
        // expectmoreIncomingMessages();
        OpBatchReceive opBatchReceive(callback);
        Lock lock(batchPendingReceiveMutex_);
        batchPendingReceives_.emplace(opBatchReceive);
        lock.unlock();
        triggerBatchReceiveTimerTask(batchReceivePolicy_.getTimeoutMs());
    }
}

OpBatchReceive::OpBatchReceive(const BatchReceiveCallback& batchReceiveCallback)
    : batchReceiveCallback_(batchReceiveCallback), createAt_(TimeUtils::currentTimeMillis()) {}

} /* namespace pulsar */

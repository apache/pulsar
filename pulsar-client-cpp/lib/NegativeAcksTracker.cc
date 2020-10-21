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

#include "NegativeAcksTracker.h"

#include "ConsumerImpl.h"

#include <set>
#include <functional>

#include "LogUtils.h"
DECLARE_LOG_OBJECT()

namespace pulsar {

NegativeAcksTracker::NegativeAcksTracker(ClientImplPtr client, ConsumerImpl &consumer,
                                         const ConsumerConfiguration &conf)
    : consumer_(consumer),
      timerInterval_(0),
      executor_(client->getIOExecutorProvider()->get()),
      enabledForTesting_(true) {
    static const long MIN_NACK_DELAY_MILLIS = 100;

    nackDelay_ =
        std::chrono::milliseconds(std::max(conf.getNegativeAckRedeliveryDelayMs(), MIN_NACK_DELAY_MILLIS));
    timerInterval_ = boost::posix_time::milliseconds((long)(nackDelay_.count() / 3));
    LOG_DEBUG("Created negative ack tracker with delay: " << nackDelay_.count()
                                                          << " ms - Timer interval: " << timerInterval_);
}

void NegativeAcksTracker::scheduleTimer() {
    timer_ = executor_->createDeadlineTimer();
    timer_->expires_from_now(timerInterval_);
    timer_->async_wait(std::bind(&NegativeAcksTracker::handleTimer, this, std::placeholders::_1));
}

void NegativeAcksTracker::handleTimer(const boost::system::error_code &ec) {
    if (ec) {
        // Ignore cancelled events
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    timer_ = nullptr;

    if (nackedMessages_.empty() || !enabledForTesting_) {
        return;
    }

    // Group all the nacked messages into one single re-delivery request
    std::set<MessageId> messagesToRedeliver;

    auto now = Clock::now();

    for (auto it = nackedMessages_.begin(); it != nackedMessages_.end();) {
        if (it->second < now) {
            messagesToRedeliver.insert(it->first);
            it = nackedMessages_.erase(it);
        } else {
            ++it;
        }
    }

    if (!messagesToRedeliver.empty()) {
        consumer_.redeliverMessages(messagesToRedeliver);
    }
    scheduleTimer();
}

void NegativeAcksTracker::add(const MessageId &m) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto now = Clock::now();

    // Erase batch id to group all nacks from same batch
    MessageId batchMessageId = MessageId(m.partition(), m.ledgerId(), m.entryId(), -1);
    nackedMessages_[batchMessageId] = now + nackDelay_;

    if (!timer_) {
        scheduleTimer();
    }
}

void NegativeAcksTracker::close() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (timer_) {
        boost::system::error_code ec;
        timer_->cancel(ec);
    }
}

void NegativeAcksTracker::setEnabledForTesting(bool enabled) {
    std::lock_guard<std::mutex> lock(mutex_);
    enabledForTesting_ = enabled;

    if (enabledForTesting_ && !timer_) {
        scheduleTimer();
    }
}

}  // namespace pulsar

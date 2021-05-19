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

#include "AckGroupingTrackerEnabled.h"

#include <mutex>
#include <algorithm>

#include "Commands.h"
#include "LogUtils.h"
#include "ClientImpl.h"
#include "HandlerBase.h"
#include "PulsarApi.pb.h"
#include <pulsar/MessageId.h>

namespace pulsar {

DECLARE_LOG_OBJECT();

AckGroupingTrackerEnabled::AckGroupingTrackerEnabled(ClientImplPtr clientPtr,
                                                     const HandlerBasePtr& handlerPtr, uint64_t consumerId,
                                                     long ackGroupingTimeMs, long ackGroupingMaxSize)
    : AckGroupingTracker(),
      handlerWeakPtr_(handlerPtr),
      consumerId_(consumerId),
      nextCumulativeAckMsgId_(MessageId::earliest()),
      requireCumulativeAck_(false),
      mutexCumulativeAckMsgId_(),
      pendingIndividualAcks_(),
      rmutexPendingIndAcks_(),
      ackGroupingTimeMs_(ackGroupingTimeMs),
      ackGroupingMaxSize_(ackGroupingMaxSize),
      executor_(clientPtr->getIOExecutorProvider()->get()),
      timer_(),
      mutexTimer_() {
    LOG_DEBUG("ACK grouping is enabled, grouping time " << ackGroupingTimeMs << "ms, grouping max size "
                                                        << ackGroupingMaxSize);
}

void AckGroupingTrackerEnabled::start() { this->scheduleTimer(); }

bool AckGroupingTrackerEnabled::isDuplicate(const MessageId& msgId) {
    {
        // Check if the message ID is already ACKed by a previous (or pending) cumulative request.
        std::lock_guard<std::mutex> lock(this->mutexCumulativeAckMsgId_);
        if (msgId <= this->nextCumulativeAckMsgId_) {
            return true;
        }
    }

    // Check existence in pending individual ACKs set.
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    return this->pendingIndividualAcks_.count(msgId) > 0;
}

void AckGroupingTrackerEnabled::addAcknowledge(const MessageId& msgId) {
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    this->pendingIndividualAcks_.insert(msgId);
    if (this->ackGroupingMaxSize_ > 0 && this->pendingIndividualAcks_.size() >= this->ackGroupingMaxSize_) {
        this->flush();
    }
}

void AckGroupingTrackerEnabled::addAcknowledgeCumulative(const MessageId& msgId) {
    std::lock_guard<std::mutex> lock(this->mutexCumulativeAckMsgId_);
    if (msgId > this->nextCumulativeAckMsgId_) {
        this->nextCumulativeAckMsgId_ = msgId;
        this->requireCumulativeAck_ = true;
    }
}

void AckGroupingTrackerEnabled::close() {
    this->flush();
    std::lock_guard<std::mutex> lock(this->mutexTimer_);
    if (this->timer_) {
        boost::system::error_code ec;
        this->timer_->cancel(ec);
    }
}

void AckGroupingTrackerEnabled::flush() {
    auto handler = handlerWeakPtr_.lock();
    if (!handler) {
        LOG_DEBUG("Reference to the HandlerBase is not valid.");
        return;
    }
    auto cnx = handler->getCnx().lock();
    if (cnx == nullptr) {
        LOG_DEBUG("Connection is not ready, grouping ACK failed.");
        return;
    }

    // Send ACK for cumulative ACK requests.
    {
        std::lock_guard<std::mutex> lock(this->mutexCumulativeAckMsgId_);
        if (this->requireCumulativeAck_) {
            if (!this->doImmediateAck(cnx, this->consumerId_, this->nextCumulativeAckMsgId_,
                                      proto::CommandAck::Cumulative)) {
                // Failed to send ACK.
                LOG_WARN("Failed to send cumulative ACK.");
                return;
            }
            this->requireCumulativeAck_ = false;
        }
    }

    // Send ACK for individual ACK requests.
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    if (!this->pendingIndividualAcks_.empty()) {
        if (Commands::peerSupportsMultiMessageAcknowledgement(cnx->getServerProtocolVersion())) {
            auto cmd = Commands::newMultiMessageAck(this->consumerId_, this->pendingIndividualAcks_);
            cnx->sendCommand(cmd);
        } else {
            // Broker does not support multi-message ACK, use multiple individual ACK instead.
            this->doImmediateAck(cnx, this->consumerId_, this->pendingIndividualAcks_);
        }
        this->pendingIndividualAcks_.clear();
    }
}

void AckGroupingTrackerEnabled::flushAndClean() {
    this->flush();
    {
        std::lock_guard<std::mutex> lock(this->mutexCumulativeAckMsgId_);
        this->nextCumulativeAckMsgId_ = MessageId::earliest();
        this->requireCumulativeAck_ = false;
    }
    std::lock_guard<std::recursive_mutex> lock(this->rmutexPendingIndAcks_);
    this->pendingIndividualAcks_.clear();
}

void AckGroupingTrackerEnabled::scheduleTimer() {
    std::lock_guard<std::mutex> lock(this->mutexTimer_);
    this->timer_ = this->executor_->createDeadlineTimer();
    this->timer_->expires_from_now(boost::posix_time::milliseconds(std::max(1L, this->ackGroupingTimeMs_)));
    auto self = shared_from_this();
    this->timer_->async_wait([this, self](const boost::system::error_code& ec) -> void {
        if (!ec) {
            this->flush();
            this->scheduleTimer();
        }
    });
}

}  // namespace pulsar

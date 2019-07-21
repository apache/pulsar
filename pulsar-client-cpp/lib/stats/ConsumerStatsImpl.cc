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

#include <lib/stats/ConsumerStatsImpl.h>
#include <lib/LogUtils.h>

#include <functional>

namespace pulsar {
DECLARE_LOG_OBJECT();

ConsumerStatsImpl::ConsumerStatsImpl(std::string consumerStr, DeadlineTimerPtr timer,
                                     unsigned int statsIntervalInSeconds)
    : consumerStr_(consumerStr),
      timer_(timer),
      statsIntervalInSeconds_(statsIntervalInSeconds),
      totalNumBytesRecieved_(0),
      numBytesRecieved_(0) {
    timer_->expires_from_now(boost::posix_time::seconds(statsIntervalInSeconds_));
    timer_->async_wait(std::bind(&pulsar::ConsumerStatsImpl::flushAndReset, this, std::placeholders::_1));
}

ConsumerStatsImpl::ConsumerStatsImpl(const ConsumerStatsImpl& stats)
    : consumerStr_(stats.consumerStr_),
      totalNumBytesRecieved_(stats.totalNumBytesRecieved_),
      numBytesRecieved_(stats.numBytesRecieved_),
      receivedMsgMap_(stats.receivedMsgMap_),
      ackedMsgMap_(stats.ackedMsgMap_),
      totalReceivedMsgMap_(stats.totalReceivedMsgMap_),
      totalAckedMsgMap_(stats.totalAckedMsgMap_),
      statsIntervalInSeconds_(stats.statsIntervalInSeconds_) {}

void ConsumerStatsImpl::flushAndReset(const boost::system::error_code& ec) {
    if (ec) {
        LOG_DEBUG("Ignoring timer cancelled event, code[" << ec << "]");
        return;
    }

    Lock lock(mutex_);
    ConsumerStatsImpl tmp = *this;
    numBytesRecieved_ = 0;
    receivedMsgMap_.clear();
    ackedMsgMap_.clear();
    lock.unlock();

    timer_->expires_from_now(boost::posix_time::seconds(statsIntervalInSeconds_));
    timer_->async_wait(std::bind(&pulsar::ConsumerStatsImpl::flushAndReset, this, std::placeholders::_1));
    LOG_INFO(tmp);
}

ConsumerStatsImpl::~ConsumerStatsImpl() {
    Lock lock(mutex_);
    if (timer_) {
        timer_->cancel();
    }
}

void ConsumerStatsImpl::receivedMessage(Message& msg, Result res) {
    Lock lock(mutex_);
    if (res == ResultOk) {
        totalNumBytesRecieved_ += msg.getLength();
        numBytesRecieved_ += msg.getLength();
    }
    receivedMsgMap_[res] += 1;
    totalReceivedMsgMap_[res] += 1;
}

void ConsumerStatsImpl::messageAcknowledged(Result res, proto::CommandAck_AckType ackType) {
    Lock lock(mutex_);
    ackedMsgMap_[std::make_pair(res, ackType)] += 1;
    totalAckedMsgMap_[std::make_pair(res, ackType)] += 1;
}

std::ostream& operator<<(std::ostream& os,
                         const std::map<std::pair<Result, proto::CommandAck_AckType>, unsigned long>& m) {
    os << "{";
    for (std::map<std::pair<Result, proto::CommandAck_AckType>, unsigned long>::const_iterator it = m.begin();
         it != m.end(); it++) {
        os << "[Key: {"
           << "Result: " << strResult((it->first).first) << ", ackType: " << (it->first).second
           << "}, Value: " << it->second << "], ";
    }
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, const ConsumerStatsImpl& obj) {
    os << "Consumer " << obj.consumerStr_ << ", ConsumerStatsImpl ("
       << "numBytesRecieved_ = " << obj.numBytesRecieved_
       << ", totalNumBytesRecieved_ = " << obj.totalNumBytesRecieved_
       << ", receivedMsgMap_ = " << obj.receivedMsgMap_ << ", ackedMsgMap_ = " << obj.ackedMsgMap_
       << ", totalReceivedMsgMap_ = " << obj.totalReceivedMsgMap_
       << ", totalAckedMsgMap_ = " << obj.totalAckedMsgMap_ << ")";
    return os;
}
} /* namespace pulsar */

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

#include <lib/stats/ProducerStatsImpl.h>

#include <lib/LogUtils.h>

#include <array>

namespace pulsar {
DECLARE_LOG_OBJECT();

static const std::array<double, 4> probs = {0.5, 0.9, 0.99, 0.999};

std::string ProducerStatsImpl::latencyToString(const LatencyAccumulator& obj) {
    boost::accumulators::detail::extractor_result<
        LatencyAccumulator, boost::accumulators::tag::extended_p_square>::type latencies =
        boost::accumulators::extended_p_square(obj);
    std::stringstream os;
    os << "Latencies [ 50pct: " << latencies[0] / 1e3 << "ms"
       << ", 90pct: " << latencies[1] / 1e3 << "ms"
       << ", 99pct: " << latencies[2] / 1e3 << "ms"
       << ", 99.9pct: " << latencies[3] / 1e3 << "ms"
       << "]";
    return os.str();
}

ProducerStatsImpl::ProducerStatsImpl(std::string producerStr, DeadlineTimerPtr timer,
                                     unsigned int statsIntervalInSeconds)
    : numMsgsSent_(0),
      numBytesSent_(0),
      totalMsgsSent_(0),
      totalBytesSent_(0),
      timer_(timer),
      producerStr_(producerStr),
      statsIntervalInSeconds_(statsIntervalInSeconds),
      mutex_(),
      latencyAccumulator_(boost::accumulators::tag::extended_p_square::probabilities = probs),
      totalLatencyAccumulator_(boost::accumulators::tag::extended_p_square::probabilities = probs) {
    timer_->expires_from_now(boost::posix_time::seconds(statsIntervalInSeconds_));
    timer_->async_wait(std::bind(&pulsar::ProducerStatsImpl::flushAndReset, this, std::placeholders::_1));
}

ProducerStatsImpl::ProducerStatsImpl(const ProducerStatsImpl& stats)
    : numMsgsSent_(stats.numMsgsSent_),
      numBytesSent_(stats.numBytesSent_),
      totalMsgsSent_(stats.totalMsgsSent_),
      totalBytesSent_(stats.totalBytesSent_),
      sendMap_(stats.sendMap_),
      totalSendMap_(stats.totalSendMap_),
      timer_(),
      producerStr_(stats.producerStr_),
      statsIntervalInSeconds_(stats.statsIntervalInSeconds_),
      mutex_(),
      latencyAccumulator_(stats.latencyAccumulator_),
      totalLatencyAccumulator_(stats.totalLatencyAccumulator_) {}

void ProducerStatsImpl::flushAndReset(const boost::system::error_code& ec) {
    if (ec) {
        LOG_DEBUG("Ignoring timer cancelled event, code[" << ec << "]");
        return;
    }

    Lock lock(mutex_);
    ProducerStatsImpl tmp = *this;
    numMsgsSent_ = 0;
    numBytesSent_ = 0;
    sendMap_.clear();
    latencyAccumulator_ =
        LatencyAccumulator(boost::accumulators::tag::extended_p_square::probabilities = probs);
    lock.unlock();

    timer_->expires_from_now(boost::posix_time::seconds(statsIntervalInSeconds_));
    timer_->async_wait(std::bind(&pulsar::ProducerStatsImpl::flushAndReset, this, std::placeholders::_1));
    LOG_INFO(tmp);
}

void ProducerStatsImpl::messageSent(const Message& msg) {
    Lock lock(mutex_);
    numMsgsSent_++;
    totalMsgsSent_++;
    numBytesSent_ += msg.getLength();
    totalBytesSent_ += msg.getLength();
}

void ProducerStatsImpl::messageReceived(Result& res, boost::posix_time::ptime& publishTime) {
    boost::posix_time::ptime currentTime = boost::posix_time::microsec_clock::universal_time();
    double diffInMicros = (currentTime - publishTime).total_microseconds();
    Lock lock(mutex_);
    totalLatencyAccumulator_(diffInMicros);
    latencyAccumulator_(diffInMicros);
    sendMap_[res] += 1;       // Value will automatically be initialized to 0 in the constructor
    totalSendMap_[res] += 1;  // Value will automatically be initialized to 0 in the constructor
}

ProducerStatsImpl::~ProducerStatsImpl() {
    Lock lock(mutex_);
    if (timer_) {
        timer_->cancel();
    }
}

std::ostream& operator<<(std::ostream& os, const ProducerStatsImpl& obj) {
    os << "Producer " << obj.producerStr_ << ", ProducerStatsImpl ("
       << "numMsgsSent_ = " << obj.numMsgsSent_ << ", numBytesSent_ = " << obj.numBytesSent_
       << ", sendMap_ = " << obj.sendMap_
       << ", latencyAccumulator_ = " << ProducerStatsImpl::latencyToString(obj.latencyAccumulator_)
       << ", totalMsgsSent_ = " << obj.totalMsgsSent_ << ", totalBytesSent_ = " << obj.totalBytesSent_
       << ", totalAcksReceived_ = "
       << ", totalSendMap_ = " << obj.totalSendMap_
       << ", totalLatencyAccumulator_ = " << ProducerStatsImpl::latencyToString(obj.totalLatencyAccumulator_)
       << ")";
    return os;
}
}  // namespace pulsar

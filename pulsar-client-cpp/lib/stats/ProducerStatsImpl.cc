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
#include <sstream>

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

ProducerStatsImpl::ProducerStatsImpl(std::string producerStr, ExecutorServicePtr executor,
                                     unsigned int statsIntervalInSeconds)
    : producerStr_(producerStr),
      executor_(executor),
      timer_(executor->createDeadlineTimer()),
      statsIntervalInSeconds_(statsIntervalInSeconds),
      callbackLock_(std::make_shared<stats::AsyncCallbackLock>()),
      latencyAccumulator_(boost::accumulators::tag::extended_p_square::probabilities = probs),
      totalLatencyAccumulator_(boost::accumulators::tag::extended_p_square::probabilities = probs) {
    scheduleReport();
}

void ProducerStatsImpl::flushAndReset(const boost::system::error_code& ec) {
    if (ec) {
        LOG_DEBUG("Ignoring timer cancelled event, code[" << ec << "]");
        return;
    }

    const bool isLogLevelEnabled = logger()->isEnabled(Logger::LEVEL_INFO);
    std::string logMessage;

    {
        Lock lockSent(sentMutex_);
        Lock lockRcv(rcvMutex_);

        if (isLogLevelEnabled) {
            std::ostringstream ss;
            ss << *this;
            logMessage = ss.str();
        }

        numMsgsSent_ = 0;
        numBytesSent_ = 0;
        sendMap_.clear();
        latencyAccumulator_ =
            LatencyAccumulator(boost::accumulators::tag::extended_p_square::probabilities = probs);
    };

    scheduleReport();

    if (isLogLevelEnabled) {
        LOG_INFO(logMessage);
    }
}

void ProducerStatsImpl::messageSent(const Message& msg) {
    Lock lock(sentMutex_);
    numMsgsSent_++;
    totalMsgsSent_++;
    numBytesSent_ += msg.getLength();
    totalBytesSent_ += msg.getLength();
}

void ProducerStatsImpl::messageReceived(Result& res, boost::posix_time::ptime& publishTime) {
    boost::posix_time::ptime currentTime = boost::posix_time::microsec_clock::universal_time();
    double diffInMicros = (currentTime - publishTime).total_microseconds();

    Lock lock(rcvMutex_);
    totalLatencyAccumulator_(diffInMicros);
    latencyAccumulator_(diffInMicros);
    sendMap_[res] += 1;       // Value will automatically be initialized to 0 in the constructor
    totalSendMap_[res] += 1;  // Value will automatically be initialized to 0 in the constructor
}

ProducerStatsImpl::~ProducerStatsImpl() {
    if (callbackLock_) {
        Lock lock(callbackLock_->mutex);

        callbackLock_->enabled = false;
        timer_->cancel();
    }
}

void ProducerStatsImpl::scheduleReport() {
    auto callbackLock = callbackLock_;
    auto flushCallback = [this, callbackLock](const boost::system::error_code& error_code) {
        Lock lock(callbackLock->mutex);
        if (callbackLock->enabled) {
            flushAndReset(error_code);
        }
    };

    timer_->expires_from_now(boost::posix_time::seconds(statsIntervalInSeconds_));
    timer_->async_wait(std::move(flushCallback));
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

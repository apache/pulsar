/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <lib/PublisherStatsImpl.h>
#include <lib/LogUtils.h>

namespace pulsar {
DECLARE_LOG_OBJECT();

    static const boost::array<double, 4> probs = {0.5, 0.9,0.99,0.999};

    std::string PublisherStatsImpl::latencyToString(const LatencyAccumulator& obj) {
        boost::accumulators::detail::extractor_result<LatencyAccumulator, boost::accumulators::tag::extended_p_square>::type latencies = boost::accumulators::extended_p_square(obj);
        std::stringstream os;
        os <<   "Latencies [ 50pct: " << latencies[0] / 1e3 << "ms" <<
                ", 90pct: " << latencies[1] / 1e3 << "ms" <<
                ", 99pct: " << latencies[2] / 1e3 << "ms" <<
                ", 99.9pct: " << latencies[3] / 1e3 << "ms" <<
                "]";
        return os.str();
    }

    PublisherStatsImpl::PublisherStatsImpl(std::string producerStr, DeadlineTimerPtr timer, unsigned int statsIntervalInSeconds)
    : numMsgsSent_(0),
    numBytesSent_(0),
    numAcksReceived_(0),
    totalMsgsSent_(0),
    totalBytesSent_(0),
    totalAcksReceived_(0),
    timer_(timer),
    producerStr_(producerStr),
    statsIntervalInSeconds_(statsIntervalInSeconds),
    mutex_(),
    latencyAccumulator_(boost::accumulators::tag::extended_p_square::probabilities = probs),
    totalLatencyAccumulator_(boost::accumulators::tag::extended_p_square::probabilities = probs) {
        timer_->expires_from_now(boost::posix_time::seconds(statsIntervalInSeconds_));
        timer_->async_wait(
                boost::bind(&pulsar::PublisherStatsImpl::flushAndReset, this,
                        boost::asio::placeholders::error));
    }

    PublisherStatsImpl::PublisherStatsImpl(const PublisherStatsImpl& stats)
    : numMsgsSent_(stats.numMsgsSent_),
      numBytesSent_(stats.numBytesSent_),
      numAcksReceived_(stats.numAcksReceived_),
      totalMsgsSent_(stats.totalMsgsSent_),
      totalBytesSent_(stats.totalBytesSent_),
      totalAcksReceived_(stats.totalAcksReceived_),
      timer_(),
      producerStr_(stats.producerStr_),
      statsIntervalInSeconds_(stats.statsIntervalInSeconds_),
      mutex_(),
      latencyAccumulator_(stats.latencyAccumulator_),
      totalLatencyAccumulator_(stats.totalLatencyAccumulator_){
    }

    void PublisherStatsImpl::flushAndReset(const boost::system::error_code& ec) {
        if (ec) {
            LOG_DEBUG("Ignoring timer cancelled event, code[" << ec <<"]");
            return;
        }

        Lock lock(mutex_);
        PublisherStatsImpl tmp = *this;
        numMsgsSent_ = 0;
        numBytesSent_ = 0;
        numAcksReceived_ = 0;
        sendMap_.clear();
        latencyAccumulator_ = LatencyAccumulator(
                boost::accumulators::tag::extended_p_square::probabilities = probs);
        lock.unlock();

        timer_->expires_from_now(boost::posix_time::seconds(statsIntervalInSeconds_));
        timer_->async_wait(
                boost::bind(&pulsar::PublisherStatsImpl::flushAndReset, this,
                            boost::asio::placeholders::error));
        LOG_INFO(tmp);
    }

    void PublisherStatsImpl::messageSent(const Message& msg) {
        Lock lock(mutex_);
        numMsgsSent_++;
        totalMsgsSent_++;
        numBytesSent_ += msg.getLength();
        totalBytesSent_ += msg.getLength();
    }

    void PublisherStatsImpl::messageReceived(Result& res, boost::posix_time::ptime& publishTime) {
        boost::posix_time::ptime currentTime = boost::posix_time::microsec_clock::universal_time();
        double diffInMicros = (currentTime - publishTime).total_microseconds();
        Lock lock(mutex_);
        totalLatencyAccumulator_(diffInMicros);
        latencyAccumulator_(diffInMicros);
        numAcksReceived_++;
        totalAcksReceived_++;
        sendMap_[res] += 1; // Value will automatically be initialized to 0 in the constructor
        totalSendMap_[res] += 1; // Value will automatically be initialized to 0 in the constructor
    }

    PublisherStatsImpl::~PublisherStatsImpl() {
        if (timer_) {
            timer_->cancel();
        }
    }

    std::ostream& operator<<(std::ostream& os, const std::map<Result, unsigned long>& m) {
        os << "{";
        for (std::map<Result, unsigned long>::const_iterator it = m.begin(); it != m.end(); it++) {
            os << "[Key: " << strResult(it->first) << ", Value: " << it->second << "], ";
        }
        os << "}";
        return os;
    }

    std::ostream& operator<<(std::ostream& os, const PublisherStatsImpl& obj) {
        os << "Producer " << obj.producerStr_ << ", PublisherStatsImpl (" << ", numMsgsSent_ = "
           << obj.numMsgsSent_ << ", numBytesSent_ = " << obj.numBytesSent_ << ", numAcksReceived_ = "
           << obj.numAcksReceived_ << ", sendMap_ = " << obj.sendMap_
           << ", latencyAccumulator_ = " << PublisherStatsImpl::latencyToString(obj.latencyAccumulator_)
           << ", totalMsgsSent_ = " << obj.totalMsgsSent_ << ", totalBytesSent_ = "
           << obj.totalBytesSent_ << ", totalAcksReceived_ = " << obj.totalAcksReceived_
           << ", totalSendMap_ = " << obj.totalSendMap_ << ", totalLatencyAccumulator_ = "
           << PublisherStatsImpl::latencyToString(obj.totalLatencyAccumulator_) << ")";
        return os;
    }
}

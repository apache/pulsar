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

#ifndef PULSAR_PUBLISHER_STATS_IMPL_HEADER
#define PULSAR_PUBLISHER_STATS_IMPL_HEADER

#include <pulsar/Message.h>
#include <map>
#include <lib/ExecutorService.h>
#include <boost/accumulators/framework/features.hpp>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics.hpp>
#include <boost/accumulators/framework/accumulator_set.hpp>
#include <boost/accumulators/statistics/extended_p_square.hpp>
#include <boost/array.hpp>

#include <boost/date_time/local_time/local_time.hpp>
#include <boost/array.hpp>
#include <iostream>
#include <vector>
#include <lib/PublisherStatsBase.h>
#include <lib/Utils.h>
#include <boost/bind.hpp>

namespace pulsar {
typedef boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::mean, boost::accumulators::tag::extended_p_square> > LatencyAccumulator;

class PublisherStatsImpl : public boost::enable_shared_from_this<PublisherStatsImpl>, public PublisherStatsBase {
private:
    uint64_t numMsgsSent_;
    uint64_t numBytesSent_;
    std::map<Result, uint64_t> sendMap_;
    uint64_t numAcksReceived_;
    LatencyAccumulator latencyAccumulator_;

    uint64_t totalMsgsSent_;
    uint64_t totalBytesSent_;
    std::map<Result, uint64_t> totalSendMap_;
    uint64_t totalAcksReceived_;
    LatencyAccumulator totalLatencyAccumulator_;

    std::string producerStr_;
    DeadlineTimerPtr timer_;
    boost::mutex mutex_;
    uint64_t statsIntervalInSeconds_;

    friend std::ostream& operator<<(std::ostream&, const PublisherStatsImpl&);
    friend std::ostream& operator<<(std::ostream&, const std::map<Result, uint64_t>&);
    friend class PulsarFriend;

    static std::string latencyToString(const LatencyAccumulator&);
public:
    PublisherStatsImpl(std::string, DeadlineTimerPtr, uint64_t);

    PublisherStatsImpl(const PublisherStatsImpl& stats);

    void flushAndReset(const boost::system::error_code&);

    void messageSent(const Message&);

    void messageReceived(Result&, boost::posix_time::ptime&);

    ~PublisherStatsImpl();

    inline uint64_t getNumMsgsSent() {
        return numMsgsSent_;
    }

    inline uint64_t getNumBytesSent() {
        return numBytesSent_;
    }

    inline uint64_t getNumAcksReceived() {
        return numAcksReceived_;
    }

    inline std::map<Result, uint64_t> getSendMap() {
        return sendMap_;
    }

    inline uint64_t getTotalMsgsSent() {
        return totalMsgsSent_;
    }

    inline uint64_t getTotalBytesSent() {
        return totalBytesSent_;
    }

    inline uint64_t getTotalAcksReceived() {
        return totalAcksReceived_;
    }

    inline std::map<Result, uint64_t> getTotalSendMap() {
        return totalSendMap_;
    }

    inline LatencyAccumulator getLatencyAccumulator() {
        return latencyAccumulator_;
    }

    inline LatencyAccumulator getTotalLatencyAccumulator() {
        return totalLatencyAccumulator_;
    }
};
}

#endif // PULSAR_PUBLISHER_STATS_IMPL_HEADER

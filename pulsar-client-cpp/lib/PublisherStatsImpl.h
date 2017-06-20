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
    unsigned long numMsgsSent_;
    unsigned long numBytesSent_;
    std::map<Result, unsigned long> sendMap_;
    unsigned long numAcksReceived_;
    LatencyAccumulator latencyAccumulator_;

    unsigned long totalMsgsSent_;
    unsigned long totalBytesSent_;
    std::map<Result, unsigned long> totalSendMap_;
    unsigned long totalAcksReceived_;
    LatencyAccumulator totalLatencyAccumulator_;

    std::string producerStr_;
    DeadlineTimerPtr timer_;
    boost::mutex mutex_;
    unsigned int statsIntervalInSeconds_;

    friend std::ostream& operator<<(std::ostream&, const PublisherStatsImpl&);
    friend std::ostream& operator<<(std::ostream&, const std::map<Result, unsigned long>&);
    friend class PulsarFriend;

    static std::string latencyToString(const LatencyAccumulator&);
public:
    PublisherStatsImpl(std::string, DeadlineTimerPtr, unsigned int);

    PublisherStatsImpl(const PublisherStatsImpl& stats);

    void flushAndReset(const boost::system::error_code&);

    void messageSent(const Message&);

    void messageReceived(Result&, boost::posix_time::ptime&);

    ~PublisherStatsImpl();

    inline unsigned long getNumMsgsSent() {
        return numMsgsSent_;
    }

    inline unsigned long getNumBytesSent() {
        return numBytesSent_;
    }

    inline unsigned long getNumAcksReceived() {
        return numAcksReceived_;
    }

    inline std::map<Result, unsigned long> getSendMap() {
        return sendMap_;
    }

    inline unsigned long getTotalMsgsSent() {
        return totalMsgsSent_;
    }

    inline unsigned long getTotalBytesSent() {
        return totalBytesSent_;
    }

    inline unsigned long getTotalAcksReceived() {
        return totalAcksReceived_;
    }

    inline std::map<Result, unsigned long> getTotalSendMap() {
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

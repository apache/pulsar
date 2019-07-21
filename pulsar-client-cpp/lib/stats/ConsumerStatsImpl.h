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

#ifndef PULSAR_CONSUMER_STATS_IMPL_H_
#define PULSAR_CONSUMER_STATS_IMPL_H_

#include <lib/stats/ConsumerStatsBase.h>
#include <lib/ExecutorService.h>
#include <lib/Utils.h>
#include <utility>
namespace pulsar {

class ConsumerStatsImpl : public ConsumerStatsBase {
   private:
    unsigned long numBytesRecieved_;
    std::map<Result, unsigned long> receivedMsgMap_;
    std::map<std::pair<Result, proto::CommandAck_AckType>, unsigned long> ackedMsgMap_;

    unsigned long totalNumBytesRecieved_;
    std::map<Result, unsigned long> totalReceivedMsgMap_;
    std::map<std::pair<Result, proto::CommandAck_AckType>, unsigned long> totalAckedMsgMap_;

    std::string consumerStr_;
    DeadlineTimerPtr timer_;
    std::mutex mutex_;
    unsigned int statsIntervalInSeconds_;

    friend std::ostream& operator<<(std::ostream&, const ConsumerStatsImpl&);
    friend std::ostream& operator<<(std::ostream&, const std::map<Result, unsigned long>&);
    friend class PulsarFriend;

   public:
    ConsumerStatsImpl(std::string, DeadlineTimerPtr, unsigned int);
    ConsumerStatsImpl(const ConsumerStatsImpl& stats);
    void flushAndReset(const boost::system::error_code&);
    virtual void receivedMessage(Message&, Result);
    virtual void messageAcknowledged(Result, proto::CommandAck_AckType);
    virtual ~ConsumerStatsImpl();

    const inline std::map<std::pair<Result, proto::CommandAck_AckType>, unsigned long>& getAckedMsgMap()
        const {
        return ackedMsgMap_;
    }

    inline unsigned long getNumBytesRecieved() const { return numBytesRecieved_; }

    const inline std::map<Result, unsigned long>& getReceivedMsgMap() const { return receivedMsgMap_; }

    inline const std::map<std::pair<Result, proto::CommandAck_AckType>, unsigned long>& getTotalAckedMsgMap()
        const {
        return totalAckedMsgMap_;
    }

    inline unsigned long getTotalNumBytesRecieved() const { return totalNumBytesRecieved_; }

    const inline std::map<Result, unsigned long>& getTotalReceivedMsgMap() const {
        return totalReceivedMsgMap_;
    }
};
typedef std::shared_ptr<ConsumerStatsImpl> ConsumerStatsImplPtr;
} /* namespace pulsar */

#endif /* PULSAR_CONSUMER_STATS_IMPL_H_ */

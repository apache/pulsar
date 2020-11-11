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
#ifndef LIB_ACKGROUPINGTRACKERENABLED_H_
#define LIB_ACKGROUPINGTRACKERENABLED_H_

#include <cstdint>

#include <set>
#include <mutex>

#include "ClientImpl.h"
#include "HandlerBase.h"
#include <pulsar/MessageId.h>
#include "AckGroupingTracker.h"

namespace pulsar {

/**
 * @class AckGroupingTrackerEnabled
 * Ack grouping tracker for consumers of persistent topics that enabled ACK grouping.
 */
class AckGroupingTrackerEnabled : public AckGroupingTracker {
   public:
    virtual ~AckGroupingTrackerEnabled() { this->close(); }

    /**
     * Constructing ACK grouping tracker for peresistent topics.
     * @param[in] clientPtr pointer to client object.
     * @param[in] handlerPtr the shared pointer to connection handler.
     * @param[in] consumerId consumer ID that this tracker belongs to.
     * @param[in] ackGroupingTimeMs ACK grouping time window in milliseconds.
     * @param[in] ackGroupingMaxSize max. number of ACK requests can be grouped.
     */
    AckGroupingTrackerEnabled(ClientImplPtr clientPtr, const HandlerBasePtr& handlerPtr, uint64_t consumerId,
                              long ackGroupingTimeMs, long ackGroupingMaxSize);

    void start() override;
    bool isDuplicate(const MessageId& msgId) override;
    void addAcknowledge(const MessageId& msgId) override;
    void addAcknowledgeCumulative(const MessageId& msgId) override;
    void close() override;
    void flush() override;
    void flushAndClean() override;

   protected:
    //! Method for scheduling grouping timer.
    void scheduleTimer();

    //! The connection handler.
    HandlerBaseWeakPtr handlerWeakPtr_;

    //! ID of the consumer that this tracker belongs to.
    uint64_t consumerId_;

    //! Next message ID to be cumulatively cumulatively.
    MessageId nextCumulativeAckMsgId_;
    bool requireCumulativeAck_;
    std::mutex mutexCumulativeAckMsgId_;

    //! Individual ACK requests that have not been sent to broker.
    std::set<MessageId> pendingIndividualAcks_;
    std::recursive_mutex rmutexPendingIndAcks_;

    //! Time window in milliseconds for grouping ACK requests.
    const long ackGroupingTimeMs_;

    //! Max number of ACK requests can be grouped.
    const long ackGroupingMaxSize_;

    //! ACK request sender's scheduled executor.
    ExecutorServicePtr executor_;

    //! Pointer to a deadline timer.
    DeadlineTimerPtr timer_;
    std::mutex mutexTimer_;
};  // class AckGroupingTrackerEnabled

}  // namespace pulsar
#endif /* LIB_ACKGROUPINGTRACKERENABLED_H_ */

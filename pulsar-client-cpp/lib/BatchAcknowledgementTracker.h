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

#ifndef LIB_BATCHACKNOWLEDGEMENTTRACKER_H_
#define LIB_BATCHACKNOWLEDGEMENTTRACKER_H_

#include "pulsar/BatchMessageId.h"
#include "MessageImpl.h"
#include <map>
#include <boost/thread/mutex.hpp>
#include <boost/dynamic_bitset.hpp>
#include <lib/PulsarApi.pb.h>
#include <algorithm>
#include "LogUtils.h"
#include <string>
#include <sstream>
namespace pulsar{

class ConsumerImpl;

class BatchAcknowledgementTracker {
 private:
    typedef boost::unique_lock<boost::mutex> Lock;
    typedef std::pair<BatchMessageId, boost::dynamic_bitset<> > TrackerPair;
    typedef std::map<BatchMessageId, boost::dynamic_bitset<> > TrackerMap;
    boost::mutex mutex_;

    TrackerMap trackerMap_;

    // SendList is used to reduce the time required to go over the dynamic_bitset and check if the entire batch is acked.
    // It is useful in cases where the entire batch is acked but cnx is broken. In this case when any of the batch index
    // is acked again, we just check the sendList to verify that the batch is acked w/o iterating over the dynamic_bitset.
    std::vector<BatchMessageId> sendList_;

    // we don't need to track MessageId < greatestCumulativeAckReceived
    BatchMessageId greatestCumulativeAckSent_;
    std::string name_;

 public:
    BatchAcknowledgementTracker(const std::string topic, const std::string subscription, const long consumerId);

    bool isBatchReady(const BatchMessageId& msgID, const proto::CommandAck_AckType ackType);
    const BatchMessageId getGreatestCumulativeAckReady(const BatchMessageId& messageId);

    void deleteAckedMessage(const BatchMessageId& messageId, proto::CommandAck_AckType ackType);
    void receivedMessage(const Message& message);


    inline friend std::ostream& operator<<(std::ostream& os, const BatchAcknowledgementTracker& batchAcknowledgementTracker);

    // Used for Cumulative acks only
    struct SendRemoveCriteria {
        private:
        const BatchMessageId& messageId_;

        public:
        SendRemoveCriteria(const BatchMessageId& messageId)
            : messageId_(messageId) {}

        bool operator()(const BatchMessageId &element) const {
            return (element <= messageId_);
        }
    };

    // Used for Cumulative acks only
    struct TrackerMapRemoveCriteria {
        private:
        const BatchMessageId& messageId_;

        public:
        TrackerMapRemoveCriteria(const BatchMessageId& messageId)
            : messageId_(messageId) {}

        bool operator()(std::pair<const pulsar::BatchMessageId, boost::dynamic_bitset<> > &element) const {
            return (element.first <= messageId_);
        }
    };

};

std::ostream& operator<<(std::ostream& os,
                         const BatchAcknowledgementTracker& batchAcknowledgementTracker) {
    os << "{ " <<  batchAcknowledgementTracker.name_
            << " [greatestCumulativeAckReceived_-"
            << batchAcknowledgementTracker.greatestCumulativeAckSent_ << "] [trackerMap size = "
            << batchAcknowledgementTracker.trackerMap_.size() << " ]}";
    return os;
}

}

#endif /* LIB_BATCHACKNOWLEDGEMENTTRACKER_H_ */

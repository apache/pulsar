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
#ifndef LIB_ACKGROUPINGTRACKER_H_
#define LIB_ACKGROUPINGTRACKER_H_

#include <cstdint>

#include <set>
#include <memory>

#include "PulsarApi.pb.h"
#include "ClientConnection.h"
#include <pulsar/MessageId.h>

namespace pulsar {

/**
 * @class AckGroupingTracker
 * Default ACK grouping tracker, it actually neither tracks ACK requests nor sends them to brokers.
 * It can be directly used by consumers for non-persistent topics.
 */
class AckGroupingTracker : public std::enable_shared_from_this<AckGroupingTracker> {
   public:
    AckGroupingTracker() = default;
    virtual ~AckGroupingTracker() = default;

    /**
     * Start tracking the ACK requests.
     */
    virtual void start() {}

    /**
     * Since ACK requests are grouped and delayed, we need to do some best-effort duplicate check to
     * discard messages that are being resent after a disconnection and for which the user has
     * already sent an acknowledgement.
     * @param[in] msgId message ID to be checked.
     * @return true if given message ID is grouped, otherwise false. If using cumulative ACK and the
     *  given message ID has been ACKed in previous cumulative ACK, it also returns true;
     */
    virtual bool isDuplicate(const MessageId& msgId) { return false; }

    /**
     * Adding message ID into ACK group for individual ACK.
     * @param[in] msgId ID of the message to be ACKed.
     */
    virtual void addAcknowledge(const MessageId& msgId) {}

    /**
     * Adding message ID into ACK group for cumulative ACK.
     * @param[in] msgId ID of the message to be ACKed.
     */
    virtual void addAcknowledgeCumulative(const MessageId& msgId) {}

    /**
     * Flush all the pending grouped ACKs (as flush() does), and stop period ACKs sending.
     */
    virtual void close() {}

    /**
     * Flush all the pending grouped ACKs and send them to the broker.
     */
    virtual void flush() {}

    /**
     * Flush all the pending grouped ACKs (as flush() does), and clean all records about ACKed
     * messages, such as last cumulative ACKed message ID.
     */
    virtual void flushAndClean() {}

   protected:
    /**
     * Immediately send ACK request to broker.
     * @param[in] connWeakPtr weak pointer of the client connection.
     * @param[in] consumerId ID of the consumer that performs this ACK.
     * @param[in] msgId message ID to be ACKed.
     * @param[in] ackType ACK type, e.g. cumulative.
     * @return true if the ACK is sent successfully, otherwise false.
     */
    bool doImmediateAck(ClientConnectionWeakPtr connWeakPtr, uint64_t consumerId, const MessageId& msgId,
                        proto::CommandAck_AckType ackType);

    /**
     * Immediately send a set of ACK requests one by one to the broker, it only supports individual
     * ACK.
     * @param[in] connWeakPtr weak pointer of the client connection.
     * @param[in] consumerId ID of the consumer that performs this ACK.
     * @param[in] msgIds message IDs to be ACKed.
     * @return true if the ACK is sent successfully, otherwise false.
     */
    bool doImmediateAck(ClientConnectionWeakPtr connWeakPtr, uint64_t consumerId,
                        const std::set<MessageId>& msgIds);
};  // class AckGroupingTracker

using AckGroupingTrackerPtr = std::shared_ptr<AckGroupingTracker>;

}  // namespace pulsar
#endif /* LIB_ACKGROUPINGTRACKER_H_ */

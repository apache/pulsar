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

#include "AckGroupingTracker.h"

#include <cstdint>

#include <set>

#include "Commands.h"
#include "LogUtils.h"
#include "PulsarApi.pb.h"
#include "ClientConnection.h"
#include <pulsar/MessageId.h>

namespace pulsar {

DECLARE_LOG_OBJECT();

inline void sendAck(ClientConnectionPtr cnx, uint64_t consumerId, const MessageId& msgId,
                    proto::CommandAck_AckType ackType) {
    proto::MessageIdData msgIdData;
    msgIdData.set_ledgerid(msgId.ledgerId());
    msgIdData.set_entryid(msgId.entryId());
    auto cmd = Commands::newAck(consumerId, msgIdData, ackType, -1);
    cnx->sendCommand(cmd);
    LOG_DEBUG("ACK request is sent for message - [" << msgIdData.ledgerid() << ", " << msgIdData.entryid()
                                                    << "]");
}

bool AckGroupingTracker::doImmediateAck(ClientConnectionWeakPtr connWeakPtr, uint64_t consumerId,
                                        const MessageId& msgId, proto::CommandAck_AckType ackType) {
    auto cnx = connWeakPtr.lock();
    if (cnx == nullptr) {
        LOG_DEBUG("Connection is not ready, ACK failed for message - [" << msgId.ledgerId() << ", "
                                                                        << msgId.entryId() << "]");
        return false;
    }
    sendAck(cnx, consumerId, msgId, ackType);
    return true;
}

bool AckGroupingTracker::doImmediateAck(ClientConnectionWeakPtr connWeakPtr, uint64_t consumerId,
                                        const std::set<MessageId>& msgIds) {
    auto cnx = connWeakPtr.lock();
    if (cnx == nullptr) {
        LOG_DEBUG("Connection is not ready, ACK failed.");
        return false;
    }

    for (const auto& msgId : msgIds) {
        sendAck(cnx, consumerId, msgId, proto::CommandAck::Individual);
    }
    return true;
}

}  // namespace pulsar

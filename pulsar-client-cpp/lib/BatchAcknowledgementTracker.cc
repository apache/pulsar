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
#include "BatchAcknowledgementTracker.h"

namespace pulsar {
DECLARE_LOG_OBJECT()

BatchAcknowledgementTracker::BatchAcknowledgementTracker(const std::string topic,
                                                         const std::string subscription,
                                                         const long consumerId)
    : greatestCumulativeAckSent_() {
    std::stringstream consumerStrStream;
    consumerStrStream << "BatchAcknowledgementTracker for [" << topic << ", " << subscription << ", "
                      << consumerId << "] ";
    name_ = consumerStrStream.str();
    LOG_DEBUG(name_ << "Constructed BatchAcknowledgementTracker");
}

void BatchAcknowledgementTracker::clear() {
    trackerMap_.clear();
    sendList_.clear();
}

void BatchAcknowledgementTracker::receivedMessage(const Message& message) {
    // ignore message if it is not a batch message
    if (!message.impl_->metadata.has_num_messages_in_batch()) {
        return;
    }
    Lock lock(mutex_);
    MessageId msgID = message.impl_->messageId;

    // ignore message if it is less than the last cumulative ack sent or messageID is already being tracked
    TrackerMap::iterator pos = trackerMap_.find(msgID);
    if (msgID < greatestCumulativeAckSent_ || pos != trackerMap_.end() ||
        std::find(sendList_.begin(), sendList_.end(), msgID) != sendList_.end()) {
        return;
    }
    LOG_DEBUG("Initializing the trackerMap_ with Message ID = "
              << msgID << " -- Map size: " << trackerMap_.size() << " -- List size: " << sendList_.size());

    // Since dynamic_set (this version) doesn't have all() function, initializing all bits with 1 and then
    // reseting them to 0 and using any()
    trackerMap_.insert(
        pos,
        TrackerPair(msgID, boost::dynamic_bitset<>(message.impl_->metadata.num_messages_in_batch()).set()));
}

void BatchAcknowledgementTracker::deleteAckedMessage(const MessageId& messageId,
                                                     proto::CommandAck_AckType ackType) {
    // Not a batch message and a individual ack
    if (messageId.batchIndex() == -1 && ackType == proto::CommandAck_AckType_Individual) {
        return;
    }

    MessageId batchMessageId =
        MessageId(messageId.partition(), messageId.ledgerId(), messageId.entryId(), -1 /* Batch index */);

    Lock lock(mutex_);
    if (ackType == proto::CommandAck_AckType_Cumulative) {
        // delete from trackerMap and sendList all messageIDs less than or equal to this one
        // equal to - since getGreatestCumulativeAckReady already gives us the exact message id to be acked

        TrackerMap::iterator it = trackerMap_.begin();
        TrackerMapRemoveCriteria criteria(messageId);
        while (it != trackerMap_.end()) {
            if (criteria(*it)) {
                trackerMap_.erase(it++);
            } else {
                ++it;
            }
        }

        // std::remove shifts all to be deleted items to the end of the vector and returns an iterator to the
        // first
        // instance of item, then we erase all elements from this iterator to the end of the list
        sendList_.erase(
            std::remove_if(sendList_.begin(), sendList_.end(), SendRemoveCriteria(batchMessageId)),
            sendList_.end());

        if (greatestCumulativeAckSent_ < messageId) {
            greatestCumulativeAckSent_ = messageId;
            LOG_DEBUG(*this << " The greatestCumulativeAckSent_ is now " << greatestCumulativeAckSent_);
        }
    } else {
        // Error - if it is a batch message and found in trackerMap_
        if (trackerMap_.find(messageId) != trackerMap_.end()) {
            LOG_ERROR(*this << " - This should not happened - Message should have been removed from "
                               "trakerMap_ and moved to sendList_ "
                            << messageId);
        }

        sendList_.erase(std::remove(sendList_.begin(), sendList_.end(), batchMessageId), sendList_.end());
    }
}

bool BatchAcknowledgementTracker::isBatchReady(const MessageId& msgID,
                                               const proto::CommandAck_AckType ackType) {
    Lock lock(mutex_);
    // Remove batch index
    MessageId batchMessageId =
        MessageId(msgID.partition(), msgID.ledgerId(), msgID.entryId(), -1 /* Batch index */);

    TrackerMap::iterator pos = trackerMap_.find(batchMessageId);
    if (pos == trackerMap_.end() ||
        std::find(sendList_.begin(), sendList_.end(), batchMessageId) != sendList_.end()) {
        LOG_DEBUG(
            "Batch is ready since message present in sendList_ or not present in trackerMap_ [message ID = "
            << batchMessageId << "]");
        return true;
    }

    int batchIndex = msgID.batchIndex();
    assert(batchIndex < pos->second.size());
    pos->second.set(batchIndex, false);

    if (ackType == proto::CommandAck_AckType_Cumulative) {
        for (int i = 0; i < batchIndex; i++) {
            pos->second.set(i, false);
        }
    }

    if (pos->second.any()) {
        return false;
    }
    sendList_.push_back(batchMessageId);
    trackerMap_.erase(pos);
    LOG_DEBUG("Batch is ready since message all bits are reset in trackerMap_ [message ID = " << msgID
                                                                                              << "]");
    return true;
}

// returns
// - a batch message id < messageId
// - same messageId if it is the last message in the batch
const MessageId BatchAcknowledgementTracker::getGreatestCumulativeAckReady(const MessageId& messageId) {
    Lock lock(mutex_);

    // Remove batch index
    MessageId batchMessageId =
        MessageId(messageId.partition(), messageId.ledgerId(), messageId.entryId(), -1 /* Batch index */);
    TrackerMap::iterator pos = trackerMap_.find(batchMessageId);

    // element not found
    if (pos == trackerMap_.end()) {
        return MessageId();
    }

    if (pos->second.size() - 1 != messageId.batchIndex()) {
        // Can't cumulatively ack this batch message
        if (pos == trackerMap_.begin()) {
            // This was the first message hence we can't decrement the iterator
            return MessageId();
        }
        pos--;
    }

    return pos->first;
}
}  // namespace pulsar

/*
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
package org.apache.pulsar.client.impl;

import java.util.BitSet;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;

public class MessageIdAdvUtils {

    static int hashCode(MessageIdAdv msgId) {
        return (int) (31 * (msgId.getLedgerId() + 31 * msgId.getEntryId())
                + (31 * (long) msgId.getPartitionIndex()) + msgId.getBatchIndex());
    }

    static boolean equals(MessageIdAdv lhs, Object o) {
        if (!(o instanceof MessageIdAdv)) {
            return false;
        }
        final MessageIdAdv rhs = (MessageIdAdv) o;
        return lhs.getLedgerId() == rhs.getLedgerId()
                && lhs.getEntryId() == rhs.getEntryId()
                && lhs.getPartitionIndex() == rhs.getPartitionIndex()
                && lhs.getBatchIndex() == rhs.getBatchIndex();
    }

    /**
     * Acknowledge batch message.
     *
     * @param msgId     the message id
     * @param individual whether to acknowledge the batch message individually
     * @return true if the batch message is fully acknowledged
     */
    static boolean acknowledge(MessageIdAdv msgId, boolean individual) {
        if (!isBatch(msgId)) {
            return true;
        }
        final BitSet ackSet = msgId.getAckSet();
        if (ackSet == null) {
            // The internal MessageId implementation should never reach here. If users have implemented their own
            // MessageId and getAckSet() is not override, return false to avoid acknowledge current entry.
            return false;
        }
        int batchIndex = msgId.getBatchIndex();
        synchronized (ackSet) {
            if (individual) {
                ackSet.clear(batchIndex);
            } else {
                ackSet.clear(0, batchIndex + 1);
            }
            return ackSet.isEmpty();
        }
    }

    static boolean isBatch(MessageIdAdv msgId) {
        return msgId.getBatchIndex() >= 0 && msgId.getBatchSize() > 0;
    }

    static MessageIdAdv discardBatch(MessageId messageId) {
        if (messageId instanceof ChunkMessageIdImpl) {
            return (MessageIdAdv) messageId;
        }
        MessageIdAdv msgId = (MessageIdAdv) messageId;
        return new MessageIdImpl(msgId.getLedgerId(), msgId.getEntryId(), msgId.getPartitionIndex());
    }

    static MessageIdAdv prevMessageId(MessageIdAdv msgId) {
        return new MessageIdImpl(msgId.getLedgerId(), msgId.getEntryId() - 1, msgId.getPartitionIndex());
    }
}

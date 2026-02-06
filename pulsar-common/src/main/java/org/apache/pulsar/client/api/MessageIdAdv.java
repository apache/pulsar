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
package org.apache.pulsar.client.api;

import java.util.BitSet;

/**
 * The {@link MessageId} interface provided for advanced users.
 * <p>
 * All built-in MessageId implementations should be able to be cast to MessageIdAdv.
 * </p>
 */
public interface MessageIdAdv extends MessageId {

    /**
     * Get the ledger ID.
     *
     * @return the ledger ID
     */
    long getLedgerId();

    /**
     * Get the entry ID.
     *
     * @return the entry ID
     */
    long getEntryId();

    /**
     * Get the partition index.
     *
     * @return -1 if the message is from a non-partitioned topic, otherwise the non-negative partition index
     */
    default int getPartitionIndex() {
        return -1;
    }

    /**
     * Get the batch index.
     *
     * @return -1 if the message is not in a batch
     */
    default int getBatchIndex() {
        return -1;
    }

    /**
     * Get the batch size.
     *
     * @return 0 if the message is not in a batch
     */
    default int getBatchSize() {
        return 0;
    }

    /**
     * Get the BitSet that indicates which messages in the batch.
     *
     * @implNote The message IDs of a batch should share a BitSet. For example, given 3 messages in the same batch whose
     * size is 3, all message IDs of them should return "111" (i.e. a BitSet whose size is 3 and all bits are 1). If the
     * 1st message has been acknowledged, the returned BitSet should become "011" (i.e. the 1st bit become 0).
     * If the caller performs any read or write operations on the return value of this method, they should do so with
     * lock protection.
     *
     * @return null if the message is a non-batched message
     */
    default BitSet getAckSet() {
        return null;
    }

    /**
     * Get the message ID of the first chunk if the current message ID represents the position of a chunked message.
     *
     * @implNote A chunked message is distributed across different BookKeeper entries. The message ID of a chunked
     * message is composed of two message IDs that represent positions of the first and the last chunk. The message ID
     * itself represents the position of the last chunk.
     *
     * @return null if the message is not a chunked message
     */
    default MessageIdAdv getFirstChunkMessageId() {
        return null;
    }

    /**
     * The default implementation of {@link Comparable#compareTo(Object)}.
     */
    default int compareTo(MessageId o) {
        if (!(o instanceof MessageIdAdv)) {
            throw new UnsupportedOperationException("Unknown MessageId type: "
                    + ((o != null) ? o.getClass().getName() : "null"));
        }
        final MessageIdAdv other = (MessageIdAdv) o;
        int result = Long.compare(this.getLedgerId(), other.getLedgerId());
        if (result != 0) {
            return result;
        }
        result = Long.compare(this.getEntryId(), other.getEntryId());
        if (result != 0) {
            return result;
        }
        // TODO: Correct the following compare logics, see https://github.com/apache/pulsar/pull/18981
        result = Integer.compare(this.getPartitionIndex(), other.getPartitionIndex());
        if (result != 0) {
            return result;
        }
        return Integer.compare(this.getBatchIndex(), other.getBatchIndex());
    }
}

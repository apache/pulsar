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

import com.google.common.collect.ComparisonChain;
import java.util.BitSet;
import javax.annotation.Nullable;

/**
 * The interface to retrieve any field of {@link org.apache.pulsar.common.api.proto.MessageIdData}.
 * <p>
 * See the MessageIdData defined in `PulsarApi.proto`.
 */
public interface PulsarApiMessageId extends MessageId {

    long getLedgerId();

    long getEntryId();

    default int getPartition() {
        return -1;
    }

    default int getBatchIndex() {
        return -1;
    }

    default @Nullable BitSet getAckSet() {
        return null;
    }

    default int getBatchSize() {
        return 0;
    }

    default @Nullable PulsarApiMessageId getFirstChunkMessageId() {
        return null;
    }

    default boolean isBatch() {
        return getBatchIndex() >= 0 && getBatchSize() > 0;
    }

    @Override
    default int compareTo(MessageId o) {
        if (!(o instanceof PulsarApiMessageId)) {
            throw new UnsupportedOperationException("Unknown MessageId type: "
                    + ((o != null) ? o.getClass().getName() : "null"));
        }
        return legacyCompare(this, (PulsarApiMessageId) o);
    }

    // The legacy compare method, which treats the non-batched message id as preceding the batched message id.
    // However, this behavior is wrong because a non-batched message id represents an entry, while a batched message
    // represents a single message in the entry, which should precedes the message id.
    // Keep this implementation just for backward compatibility when users compare two message ids.
    static int legacyCompare(PulsarApiMessageId lhs, PulsarApiMessageId rhs) {
        return ComparisonChain.start()
                .compare(lhs.getLedgerId(), rhs.getLedgerId())
                .compare(lhs.getEntryId(), rhs.getEntryId())
                .compare(lhs.getPartition(), rhs.getPartition())
                .compare(lhs.getBatchIndex(), rhs.getBatchIndex())
                .result();
    }

    static int compare(PulsarApiMessageId lhs, PulsarApiMessageId rhs) {
        return ComparisonChain.start()
                .compare(lhs.getLedgerId(), rhs.getLedgerId())
                .compare(lhs.getEntryId(), rhs.getEntryId())
                .compare(lhs.getPartition(), rhs.getPartition())
                .compare(
                        (lhs.getBatchIndex() < 0) ? Integer.MAX_VALUE : lhs.getBatchIndex(),
                        (rhs.getBatchIndex() < 0) ? Integer.MAX_VALUE : rhs.getBatchIndex())
                .result();
    }

    static boolean equals(PulsarApiMessageId lhs, PulsarApiMessageId rhs) {
        return legacyCompare(lhs, rhs) == 0;
    }

    static int hashCode(PulsarApiMessageId id) {
        return (int) (31 * (id.getLedgerId() + 31 * id.getEntryId()) + (31 * (long) id.getPartition())
                + id.getBatchIndex());
    }
}

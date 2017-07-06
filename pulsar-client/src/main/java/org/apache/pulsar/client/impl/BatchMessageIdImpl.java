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
package org.apache.pulsar.client.impl;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 */
public class BatchMessageIdImpl extends MessageIdImpl implements Comparable<MessageIdImpl> {
    private final int batchIndex;

    public BatchMessageIdImpl(long ledgerId, long entryId, int partitionIndex, int batchIndex) {
        super(ledgerId, entryId, partitionIndex);
        this.batchIndex = batchIndex;
    }

    int getBatchIndex() {
        return batchIndex;
    }

    @Override
    public int compareTo(MessageIdImpl o) {
        if (!(o instanceof BatchMessageIdImpl)) {
            throw new IllegalArgumentException(
                    "expected BatchMessageIdImpl object. Got instance of " + o.getClass().getName());
        }

        BatchMessageIdImpl other = (BatchMessageIdImpl) o;
        return ComparisonChain.start().compare(this.ledgerId, other.ledgerId).compare(this.entryId, other.entryId)
                .compare(this.batchIndex, other.batchIndex).compare(this.getPartitionIndex(), other.getPartitionIndex())
                .result();
    }

    @Override
    public int hashCode() {
        return (int) (31 * (ledgerId + 31 * entryId) + (31 * partitionIndex) + batchIndex);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl other = (BatchMessageIdImpl) obj;
            return ledgerId == other.ledgerId && entryId == other.entryId && partitionIndex == other.partitionIndex
                    && batchIndex == other.batchIndex;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("%d:%d:%d:%d", ledgerId, entryId, partitionIndex, batchIndex);
    }

    // Serialization

    @Override
    public byte[] toByteArray() {
        return toByteArray(batchIndex);
    }
}

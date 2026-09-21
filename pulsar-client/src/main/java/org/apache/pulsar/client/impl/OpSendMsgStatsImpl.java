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

import lombok.Builder;

@Builder
public class OpSendMsgStatsImpl implements OpSendMsgStats {
    private long uncompressedSize;
    private long sequenceId;
    private int retryCount;
    private long batchSizeByte;
    private int numMessagesInBatch;
    private long highestSequenceId;
    private int totalChunks;
    private int chunkId;

    @Override
    public long getUncompressedSize() {
        return uncompressedSize;
    }

    @Override
    public long getSequenceId() {
        return sequenceId;
    }

    @Override
    public int getRetryCount() {
        return retryCount;
    }

    @Override
    public long getBatchSizeByte() {
        return batchSizeByte;
    }

    @Override
    public int getNumMessagesInBatch() {
        return numMessagesInBatch;
    }

    @Override
    public long getHighestSequenceId() {
        return highestSequenceId;
    }

    @Override
    public int getTotalChunks() {
        return totalChunks;
    }

    @Override
    public int getChunkId() {
        return chunkId;
    }
}

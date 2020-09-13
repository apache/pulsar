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

import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;

/**
 * A no-op acknowledgment grouping tracker.
 */
public class NonPersistentAcknowledgmentGroupingTracker implements AcknowledgmentsGroupingTracker {

    public static NonPersistentAcknowledgmentGroupingTracker of() {
        return INSTANCE;
    }

    private static final NonPersistentAcknowledgmentGroupingTracker INSTANCE = new NonPersistentAcknowledgmentGroupingTracker();

    private NonPersistentAcknowledgmentGroupingTracker() {}

    @Override
    public boolean isDuplicate(MessageId messageId) {
        return false;
    }

    @Override
    public void addAcknowledgment(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties,
                                  long txnidMostBits, long txnidLeastBits) {
        // no-op
    }

    @Override
    public void addListAcknowledgment(List<MessageIdImpl> messageIds, AckType ackType, Map<String, Long> properties) {
        // no-op
    }

    @Override
    public void addBatchIndexAcknowledgment(BatchMessageIdImpl msgId, int batchIndex, int BatchSize, AckType ackType,
                                            Map<String, Long> properties, long txnidMostSets, long txnidLeastSets) {
        // no-op
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public void flushAndClean() {
        // no-op
    }
}

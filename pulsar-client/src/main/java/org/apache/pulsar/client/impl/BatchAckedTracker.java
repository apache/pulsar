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

import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;

/**
 * Tracks any partial acked batches and its acked messages
 * This will prevent acked message redelivery to the client only at the client API level.
 * This class does not track batch with all acked message. We trust broker won't deliver again.
 */
class BatchAckedTracker {

    // a map of partial acked batch and its messages already acked
    // Key is the string Id for batch, the value is bit set for message index whether ack-ed or not 
    @VisibleForTesting
    Map<String, BitSet> ackedBatches = Collections.synchronizedMap(new HashMap<String, BitSet>());

    public BatchAckedTracker() {
    }

    // If this message should be delivered and tracks this message if it is a batch message
    public boolean deliver(MessageId messageId) {
        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl id = (BatchMessageIdImpl) messageId;
            String batchId = getBatchId(id);
            if (ackedBatches.containsKey(batchId)) {
                return ackedBatches.get(batchId).get(id.getBatchIndex());
            }
        }
        // deliver non batch message and any other cases
        return true;
    }

    private BitSet initBatchSet(int size) {
        BitSet set = new BitSet(size);
        set.set(0, size);
        return set;
    }
    /**
     * 
     * @param messageId batchMessageIdImpl
     * @return boolean isAllMsgAcked for the batch
     */
    public boolean ack (BatchMessageIdImpl messageId, AckType ackType) {
        String batchId = getBatchId(messageId);
        int batchSize = messageId.getBatchSize();
        BitSet batch = ackedBatches.getOrDefault(batchId, initBatchSet(batchSize));
        if (ackType == AckType.Individual) {
            batch.clear(messageId.getBatchIndex());
        } else {
            batch.clear(0, messageId.getBatchIndex() + 1);
        }

        if (batch.isEmpty()) {
            //we ack complete batch now so delete it from the tracker
            ackedBatches.remove(batchId);
            return true;
        } else {
            ackedBatches.put(batchId, batch);
            return false;
        }
    }

    private static String getBatchId(BatchMessageIdImpl id) {
        return id.ledgerId + "-" + id.entryId + "-" + id.partitionIndex;
    }
}

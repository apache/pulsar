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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.pulsar.client.api.MessageId;

/**
 * Tracks any partial acked batches and its acked messages
 * This will prevent acked message redelivery to the client only at the client API level.
 * This class does not track batch with all acked message. We trust broker won't deliver again.
 */
class BatchAckedTracker {

    // a map of partial acked batch and its messages already acked
    @VisibleForTesting
    Map<String, Set<BatchMessageIdImpl>> ackedBatches = new HashMap<String, Set<BatchMessageIdImpl>>();

    public BatchAckedTracker() {
    }

    // we start to track 
    public void negativeAcked(BatchMessageIdImpl messageId) {
        
    }

    // If this message should be delivered and tracks this message if it is a batch message
    public boolean deliver(MessageId messageId) {
        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl id = (BatchMessageIdImpl) messageId;
            String batchId = getBatchId(id);
            if (ackedBatches.containsKey(batchId)){
                Set<BatchMessageIdImpl> batch = ackedBatches.get(batchId);
                if (batch.contains(id)) {
                    return false;
                }
            }
        }
        // deliver non batch message and any other cases
        return true;
    }

    /**
     * 
     * @param messageId batchMessageIdImpl
     * @return boolean isAllMsgAcked for the batch
     */
    public boolean ack (BatchMessageIdImpl messageId) {
        String batchId = getBatchId(messageId);
        Set<BatchMessageIdImpl> batches = ackedBatches.getOrDefault(batchId, new HashSet<BatchMessageIdImpl>());
        if (messageId.getBatchSize() == (batches.size() + 1)) {
            //we ack complete batch now so delete it from the tracker
            ackedBatches.remove(batchId);
            return true;
        } else {
            batches.add(messageId);
            ackedBatches.put(batchId, batches);
        }
        return false;
    }

    private static String getBatchId(BatchMessageIdImpl id) {
        return id.ledgerId + "-" + id.entryId + "-" + id.partitionIndex;
    }
}

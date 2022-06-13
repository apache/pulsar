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

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key based batch message container.
 *
 * incoming single messages:
 * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
 *
 * batched into multiple batch messages:
 * [(k1, v1), (k1, v2), (k1, v3)], [(k2, v1), (k2, v2), (k2, v3)], [(k3, v1), (k3, v2), (k3, v3)]
 */
class BatchMessageKeyBasedContainer extends AbstractBatchMessageContainer {

    private final Map<String, BatchMessageContainerImpl> batches = new HashMap<>();

    @Override
    public boolean add(MessageImpl<?> msg, SendCallback callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] add message to batch, num messages in batch so far is {}", topicName, producerName,
                    numMessagesInBatch);
        }
        String key = getKey(msg);
        final BatchMessageContainerImpl batchMessageContainer = batches.computeIfAbsent(key,
                __ -> new BatchMessageContainerImpl(producer));
        batchMessageContainer.add(msg, callback);
        // The `add` method fails iff the container is empty, i.e. the `msg` is the first message to add, while `msg`
        // was failed to add. In this case, `clear` method will be called and the batch container is empty and there is
        // no need to update the stats.
        if (!batchMessageContainer.isEmpty()) {
            numMessagesInBatch++;
            currentBatchSizeBytes += msg.getDataBuffer().readableBytes();
        }
        return isBatchFull();
    }

    @Override
    public void clear() {
        numMessagesInBatch = 0;
        currentBatchSizeBytes = 0;
        batches.clear();
        currentTxnidMostBits = -1L;
        currentTxnidLeastBits = -1L;
    }

    @Override
    public boolean isEmpty() {
        return batches.isEmpty();
    }

    @Override
    public void discard(Exception ex) {
        batches.forEach((k, v) -> v.discard(ex));
        clear();
    }

    @Override
    public boolean isMultiBatches() {
        return true;
    }

    @Override
    public List<ProducerImpl.OpSendMsg> createOpSendMsgs() throws IOException {
        try {
            // In key based batching, the sequence ids might not be ordered, for example,
            // | key | sequence id list |
            // | :-- | :--------------- |
            // | A | 0, 3, 4 |
            // | B | 1, 2 |
            // The message order should be 1, 2, 0, 3, 4 so that a message with a sequence id <= 4 should be dropped.
            // However, for a MessageMetadata with both `sequence_id` and `highest_sequence_id` fields, the broker will
            // expect a strict order so that the batch of key "A" (0, 3, 4) will be dropped.
            // Therefore, we should update the `sequence_id` field to the highest sequence id and remove the
            // `highest_sequence_id` field to allow the weak order.
            batches.values().forEach(batchMessageContainer -> {
                batchMessageContainer.setLowestSequenceId(batchMessageContainer.getHighestSequenceId());
            });
            return batches.values().stream().sorted((o1, o2) ->
                    (int) (o1.getLowestSequenceId() - o2.getLowestSequenceId())
            ).map(batchMessageContainer -> {
                try {
                    return batchMessageContainer.createOpSendMsg();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }).collect(Collectors.toList());
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Override
    public boolean hasSameSchema(MessageImpl<?> msg) {
        String key = getKey(msg);
        BatchMessageContainerImpl batchMessageContainer = batches.get(key);
        return batchMessageContainer == null || batchMessageContainer.hasSameSchema(msg);
    }

    private String getKey(MessageImpl<?> msg) {
        if (msg.hasOrderingKey()) {
            return Base64.getEncoder().encodeToString(msg.getOrderingKey());
        }
        return msg.getKey();
    }

    private static final Logger log = LoggerFactory.getLogger(BatchMessageKeyBasedContainer.class);

}

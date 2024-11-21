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
package org.apache.pulsar.compaction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.impl.RawBatchConverter;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PublishingOrderCompactor extends AbstractTwoPhaseCompactor<MessageId> {

    private static final Logger log = LoggerFactory.getLogger(PublishingOrderCompactor.class);

    public PublishingOrderCompactor(ServiceConfiguration conf,
        PulsarClient pulsar,
        BookKeeper bk,
        ScheduledExecutorService scheduler) {
        super(conf, pulsar, bk, scheduler);
    }

    @Override
    protected Map<String, MessageId> toLatestMessageIdForKey(Map<String, MessageId> latestForKey) {
        return latestForKey;
    }

    @Override
    protected boolean compactMessage(String topic, Map<String, MessageId> latestForKey,
        RawMessage m, MessageMetadata metadata, MessageId id) {
        boolean deletedMessage = false;
        boolean replaceMessage = false;
        Pair<String, Integer> keyAndSize = extractKeyAndSize(m, metadata);
        if (keyAndSize != null) {
            if (keyAndSize.getRight() > 0) {
                MessageId old = latestForKey.put(keyAndSize.getLeft(), id);
                replaceMessage = old != null;
            } else {
                deletedMessage = true;
                latestForKey.remove(keyAndSize.getLeft());
            }
        } else {
            if (!topicCompactionRetainNullKey) {
                deletedMessage = true;
            }
        }
        if (replaceMessage || deletedMessage) {
            mxBean.addCompactionRemovedEvent(topic);
        }
        return deletedMessage;
    }

    @Override
    protected boolean compactBatchMessage(String topic, Map<String, MessageId> latestForKey,
        RawMessage m, MessageMetadata metadata, MessageId id) {
        boolean deletedMessage = false;
        try {
            int numMessagesInBatch = metadata.getNumMessagesInBatch();
            int deleteCnt = 0;
            for (ImmutableTriple<MessageId, String, Integer> e : extractIdsAndKeysAndSizeFromBatch(
                m, metadata)) {
                if (e != null) {
                    if (e.getMiddle() == null) {
                        if (!topicCompactionRetainNullKey) {
                            // record delete null-key message event
                            deleteCnt++;
                            mxBean.addCompactionRemovedEvent(topic);
                        }
                        continue;
                    }
                    if (e.getRight() > 0) {
                        MessageId old = latestForKey.put(e.getMiddle(), e.getLeft());
                        if (old != null) {
                            mxBean.addCompactionRemovedEvent(topic);
                        }
                    } else {
                        latestForKey.remove(e.getMiddle());
                        deleteCnt++;
                        mxBean.addCompactionRemovedEvent(topic);
                    }
                }
            }
            if (deleteCnt == numMessagesInBatch) {
                deletedMessage = true;
            }
        } catch (IOException ioe) {
            log.info(
                "Error decoding batch for message {}. Whole batch will be included in output",
                id, ioe);
        }

        return deletedMessage;
    }

    protected List<ImmutableTriple<MessageId, String, Integer>> extractIdsAndKeysAndSizeFromBatch(
        RawMessage msg, MessageMetadata metadata)
        throws IOException {
        return RawBatchConverter.extractIdsAndKeysAndSize(msg, metadata);
    }

}
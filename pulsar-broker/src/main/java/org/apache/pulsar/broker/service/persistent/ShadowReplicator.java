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
package org.apache.pulsar.broker.service.persistent;


import io.netty.buffer.ByteBuf;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.Codec;

/**
 *  Replicate messages to shadow topic.
 */
@Slf4j
public class ShadowReplicator extends PersistentReplicator {

    public ShadowReplicator(String shadowTopic, PersistentTopic sourceTopic, ManagedCursor cursor,
                            BrokerService brokerService, PulsarClientImpl replicationClient)
            throws PulsarServerException {
        super(brokerService.pulsar().getConfiguration().getClusterName(), sourceTopic, cursor,
                brokerService.pulsar().getConfiguration().getClusterName(), shadowTopic, brokerService,
                replicationClient);
    }

    /**
     * @return Producer name format : replicatorPrefix-localTopic-->remoteTopic
     */
    @Override
    protected String getProducerName() {
        return replicatorPrefix + "-" + localTopicName + REPL_PRODUCER_NAME_DELIMITER + remoteTopicName;
    }

    @Override
    protected boolean replicateEntries(List<Entry> entries) {
        boolean atLeastOneMessageSentForReplication = false;

        try {
            // This flag is set to true when we skip at least one local message,
            // in order to skip remaining local messages.
            boolean isLocalMessageSkippedOnce = false;
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                int length = entry.getLength();
                ByteBuf headersAndPayload = entry.getDataBuffer();
                MessageImpl msg;
                try {
                    msg = MessageImpl.deserializeSkipBrokerEntryMetaData(headersAndPayload);
                } catch (Throwable t) {
                    log.error("[{}] Failed to deserialize message at {} (buffer size: {}): {}", replicatorId,
                            entry.getPosition(), length, t.getMessage(), t);
                    cursor.asyncDelete(entry.getPosition(), this, entry.getPosition());
                    entry.release();
                    continue;
                }

                if (msg.isExpired(messageTTLInSeconds)) {
                    msgExpired.recordEvent(0 /* no value stat */);
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Discarding expired message at position {}, replicateTo {}",
                                replicatorId, entry.getPosition(), msg.getReplicateTo());
                    }
                    cursor.asyncDelete(entry.getPosition(), this, entry.getPosition());
                    entry.release();
                    msg.recycle();
                    continue;
                }

                if (STATE_UPDATER.get(this) != State.Started || isLocalMessageSkippedOnce) {
                    // The producer is not ready yet after having stopped/restarted. Drop the message because it will
                    // recovered when the producer is ready
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Dropping read message at {} because producer is not ready",
                                replicatorId, entry.getPosition());
                    }
                    isLocalMessageSkippedOnce = true;
                    entry.release();
                    msg.recycle();
                    continue;
                }

                dispatchRateLimiter.ifPresent(rateLimiter -> rateLimiter.tryDispatchPermit(1, entry.getLength()));

                // Increment pending messages for messages produced locally
                PENDING_MESSAGES_UPDATER.incrementAndGet(this);

                msgOut.recordEvent(headersAndPayload.readableBytes());

                msg.setReplicatedFrom(localCluster);

                msg.setMessageId(new MessageIdImpl(entry.getLedgerId(), entry.getEntryId(), -1));

                headersAndPayload.retain();

                producer.sendAsync(msg, ProducerSendCallback.create(this, entry, msg));
                atLeastOneMessageSentForReplication = true;
            }
        } catch (Exception e) {
            log.error("[{}] Unexpected exception: {}", replicatorId, e.getMessage(), e);
        }
        return atLeastOneMessageSentForReplication;
    }

    /**
     * Cursor name fot this shadow replicator.
     * @param replicatorPrefix
     * @param shadowTopic
     * @return
     */
    public static String getShadowReplicatorName(String replicatorPrefix, String shadowTopic) {
        return replicatorPrefix + "-" + Codec.encode(shadowTopic);
    }
}

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

import io.netty.util.ReferenceCountUtil;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Markers;

@Slf4j
public class GeoReplicationProducerImpl extends ProducerImpl{

    public static final String MSG_PROP_REPL_SEQUENCE_LID = "__MSG_PROP_REPL_SEQUENCE_LID";
    public static final String MSG_PROP_REPL_SEQUENCE_EID = "__MSG_PROP_REPL_SEQUENCE_EID";
    public static final String MSG_PROP_IS_REPL_MARKER = "__MSG_PROP_IS_REPL_MARKER";

    private long lastPersistedSourceLedgerId;
    private long lastPersistedSourceEntryId;

    private final boolean isPersistentTopic;

    public GeoReplicationProducerImpl(PulsarClientImpl client, String topic,
                                      ProducerConfigurationData conf,
                                      CompletableFuture producerCreatedFuture, int partitionIndex,
                                      Schema schema, ProducerInterceptors interceptors,
                                      Optional overrideProducerName) {
        super(client, topic, conf, producerCreatedFuture, partitionIndex, schema, interceptors, overrideProducerName);
        isPersistentTopic = TopicName.get(topic).isPersistent();
    }

    private boolean isBrokerSupportsDedupReplV2(ClientCnx cnx) {
        // Non-Persistent topic does not have ledger id or entry id, so it does not support.
        return cnx.isBrokerSupportsDedupReplV2() && isPersistentTopic;
    }

    @Override
    protected void ackReceived(ClientCnx cnx, long seq, long highSeq, long ledgerId, long entryId) {
        if (!isBrokerSupportsDedupReplV2(cnx)) {
            // Repl V1 is the same as normal for this handling.
            super.ackReceived(cnx, seq, highSeq, ledgerId, entryId);
            return;
        }
        synchronized (this) {
            OpSendMsg op = pendingMessages.peek();
            if (op == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg {}:{}", topic, producerName, seq, highSeq);
                }
                return;
            }
            // Replicator send markers also, use sequenceId to check the marker send-receipt.
            if (isReplicationMarker(entryId)) {
                ackReceivedReplMarker(cnx, op, seq, highSeq, ledgerId, entryId);
                return;
            }
            ackReceivedReplicatedMsg(cnx, op, seq, highSeq, ledgerId, entryId);
        }
    }
    private void ackReceivedReplicatedMsg(ClientCnx cnx, OpSendMsg op, long sourceLId, long sourceEId,
                                          long targetLId, long targetEid) {
        // Case-1: repeatedly publish.
        if (sourceLId < lastPersistedSourceLedgerId
                || (sourceLId == lastPersistedSourceLedgerId  && sourceEId < lastPersistedSourceEntryId)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Dropped a repl marker SendReceipt. Got entry {}:{}, last persisted source entry:"
                        + " {}:{}",
                        topic, producerName, sourceLId, sourceEId,
                        lastPersistedSourceLedgerId, lastPersistedSourceEntryId);
            }
            return;
        }

        // Parse source cluster's entry position.
        Long pendingLId = null;
        Long pendingEId = null;
        List<KeyValue> kvPairList =  op.msg.getMessageBuilder().getPropertiesList();
        for (KeyValue kvPair : kvPairList) {
            if (kvPair.getKey().equals(MSG_PROP_REPL_SEQUENCE_LID)) {
                if (StringUtils.isNumeric(kvPair.getValue())) {
                    pendingLId = Long.valueOf(kvPair.getValue());
                } else {
                    break;
                }
            }
            if (kvPair.getKey().equals(MSG_PROP_REPL_SEQUENCE_EID)) {
                if (StringUtils.isNumeric(kvPair.getValue())) {
                    pendingEId = Long.valueOf(kvPair.getValue());
                } else {
                    break;
                }
            }
            if (pendingLId != null && pendingEId != null) {
                break;
            }
        }

        // Case-2, which is expected.
        if (pendingLId != null && pendingEId != null && sourceLId == pendingLId && sourceEId == pendingEId) {
            // Case-3, which is expected.
            // Q: After a reconnect, maybe we have lost the response of Send-Receipt, then how can we remove
            //    pending messages from the queue?
            // A: if both @param-ledgerId and @param-entry-id are "-1", it means the message has been sent
            //    successfully.
            //    PS: broker will respond "-1" only when it confirms the message has been persisted, broker will
            //        respond a "MessageDeduplication.MessageDupUnknownException" if the message is sending
            //        in-progress.
            //    Notice: if send messages outs of oder, may lost messages.
            // Conclusion: So whether @param-ledgerId and @param-entry-id are "-1" or not, we can remove pending
            //    message.
            lastPersistedSourceLedgerId = sourceLId;
            lastPersistedSourceEntryId = sourceEId;
            removeAndApplyCallback(op, sourceLId, sourceEId, targetLId, targetEid, false);
            return;
        }

        // Case-3: got null source cluster's entry position, which is unexpected.
        if (pendingLId == null || pendingEId == null) {
            log.error("[{}] [{}] can not found v2 sequence-id {}:{}, ackReceived: {}:{} {}:{} - queue-size: {}",
                    topic, producerName, pendingLId, pendingEId, sourceLId,
                    sourceEId, targetLId, targetEid, pendingMessages.messagesCount());
            cnx.channel().close();
            return;
        }

        // Case-4: unknown error, which is unexpected.
        log.warn("[{}] [{}] Got ack for msg. expecting {}:{}, but got: {}:{} - queue-size: {}",
                topic, producerName, pendingLId, pendingEId, sourceLId,
                sourceEId, pendingMessages.messagesCount());
        // Force connection closing so that messages can be re-transmitted in a new connection
        cnx.channel().close();
    }

    protected void ackReceivedReplMarker(ClientCnx cnx, OpSendMsg op, long req, long highReq,
                                         long ledgerId, long entryId) {
        // Case-1: repeatedly publish repl marker.
        long lastSeqReceivedAck = LAST_SEQ_ID_PUBLISHED_UPDATER.get(this);
        if (req <= lastSeqReceivedAck) {
            // Ignoring the ack since it's referring to a message that has already timed out.
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Dropped a repl marker SendReceipt. sequenceId: {},"
                                + " sequenceIdPersisted: {},"
                                + " highReq: {}, position: {}:{}",
                        topic, producerName, req, lastSeqReceivedAck, highReq, ledgerId, entryId);
            }
            return;
        }

        // Case-2, which is expected:
        // 1. Broker responds SendReceipt who is a repl marker.
        // 2. The current pending msg is also a marker.
        if (isReplicationMarker(op) && req == op.sequenceId) {
            long calculatedSeq = getHighestSequenceId(op);
            LAST_SEQ_ID_PUBLISHED_UPDATER.getAndUpdate(this, last -> Math.max(last, calculatedSeq));
            removeAndApplyCallback(op, req, highReq, ledgerId, entryId, true);
            return;
        }

        // Case-3, which is unexpected.
        // Case-3-1: expected a SendError if "seq <= lastInProgressSend".
        // Case-3-2: something went wrong.
        long lastInProgressSend = LAST_SEQ_ID_PUSHED_UPDATER.get(this);
        String logText = String.format("[%s] [%s] Got ack for a repl marker msg. expecting %s, but got: %s."
                + " sequenceIdPersisted: %s, lastInProgressSend: %s,"
                + " HighReq: %s, position: %s:%s, queue-size: %s",
                topic, producerName, lastSeqReceivedAck - 1, req,
                lastSeqReceivedAck, lastInProgressSend,
                highReq, ledgerId, entryId, pendingMessages.messagesCount()
        );
        if (req < lastInProgressSend) {
            log.warn(logText);
        } else {
            log.error(logText);
        }
        // Force connection closing so that messages can be re-transmitted in a new connection.
        cnx.channel().close();
    }

    private void removeAndApplyCallback(OpSendMsg op, long lIdSent, long eIdSent, long ledgerId, long entryId,
                                        boolean isMarker) {
        if (log.isDebugEnabled()) {
            log.debug("Got receipt for producer: [{}] -- source-message: {}:{} -- target-msg: {}:{} -- isMarker: {}",
                    getProducerName(), lIdSent, eIdSent, ledgerId, entryId, isMarker);
        }
        pendingMessages.remove();
        releaseSemaphoreForSendOp(op);
        // Since Geo-Replicator will not send batched message, skip to update the field
        // "LAST_SEQ_ID_PUBLISHED_UPDATER".
        op.setMessageId(ledgerId, entryId, partitionIndex);
        try {
            // Need to protect ourselves from any exception being thrown in the future handler from the
            // application
            op.sendComplete(null);
        } catch (Throwable t) {
            log.warn("[{}] [{}] Got exception while completing the callback for -- source-message: {}:{} --"
                            + " target-msg: {}:{} -- isMarker: {}",
                    topic, producerName, lIdSent, eIdSent, ledgerId, entryId, isMarker, t);
        }
        ReferenceCountUtil.safeRelease(op.cmd);
        op.recycle();
    }

    private boolean isReplicationMarker(OpSendMsg op) {
        return op.msg != null && op.msg.getMessageBuilder().hasMarkerType()
                && Markers.isReplicationMarker(op.msg.getMessageBuilder().getMarkerType());
    }

    private boolean isReplicationMarker(long highestSeq) {
        return Long.MIN_VALUE == highestSeq;
    }

    @Override
    protected void updateLastSeqPushed(OpSendMsg op) {
        // Only update the value for repl marker.
        if (isReplicationMarker(op)) {
            super.updateLastSeqPushed(op);
        }
    }
}

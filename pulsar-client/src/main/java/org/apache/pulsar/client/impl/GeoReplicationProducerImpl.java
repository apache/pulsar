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

    public static final String MSG_PROP_REPL_SOURCE_POSITION = "__MSG_PROP_REPL_SOURCE_POSITION";
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

    private boolean isBrokerSupportsReplDedupByLidAndEid(ClientCnx cnx) {
        // Non-Persistent topic does not have ledger id or entry id, so it does not support.
        return cnx.isBrokerSupportsReplDedupByLidAndEid() && isPersistentTopic;
    }

    @Override
    protected void ackReceived(ClientCnx cnx, long seq, long highSeq, long ledgerId, long entryId) {
        if (!isBrokerSupportsReplDedupByLidAndEid(cnx)) {
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
            if (isReplicationMarker(highSeq)) {
                ackReceivedReplMarker(cnx, op, seq, highSeq, ledgerId, entryId);
                return;
            }
            ackReceivedReplicatedMsg(cnx, op, seq, highSeq, ledgerId, entryId);
        }
    }

    private void ackReceivedReplicatedMsg(ClientCnx cnx, OpSendMsg op, long sourceLId, long sourceEId,
                                          long targetLId, long targetEid) {
        // Parse source cluster's entry position.
        Long pendingLId = null;
        Long pendingEId = null;
        List<KeyValue> kvPairList =  op.msg.getMessageBuilder().getPropertiesList();
        for (KeyValue kvPair : kvPairList) {
            if (kvPair.getKey().equals(MSG_PROP_REPL_SOURCE_POSITION)) {
                if (!kvPair.getValue().contains(":")) {
                    break;
                }
                String[] ledgerIdAndEntryId = kvPair.getValue().split(":");
                if (ledgerIdAndEntryId.length != 2 || !StringUtils.isNumeric(ledgerIdAndEntryId[0])
                        || !StringUtils.isNumeric(ledgerIdAndEntryId[1])) {
                    break;
                }
                pendingLId = Long.valueOf(ledgerIdAndEntryId[0]);
                pendingEId = Long.valueOf(ledgerIdAndEntryId[1]);
                break;
            }
        }

        // Case-1: repeatedly publish. Source message was exactly resend by the Replicator after a cursor rewind.
        //   - The first time: Replicator --M1--> producer --> ...
        //   - Cursor rewind.
        //   - The second time: Replicator --M1--> producer --> ...
        if (pendingLId != null && pendingEId != null
                && (pendingLId < lastPersistedSourceLedgerId || (pendingLId.longValue() == lastPersistedSourceLedgerId
                  && pendingEId.longValue() <= lastPersistedSourceEntryId))) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received an msg send receipt[pending send is repeated due to repl cursor rewind]:"
                                + " source entry {}:{}, pending send: {}:{}, latest persisted: {}:{}",
                        topic, producerName, sourceLId, sourceEId, pendingLId, pendingEId,
                        lastPersistedSourceLedgerId, lastPersistedSourceEntryId);
            }
            removeAndApplyCallback(op, sourceLId, sourceEId, targetLId, targetEid, false);
            ackReceived(cnx, sourceLId, sourceEId, targetLId, targetEid);
            return;
        }

        // Case-2: repeatedly publish. Send command was executed again by the producer after a reconnect.
        //  - Replicator --M1--> producer --> ...
        //  - The first time: producer call Send-Command-1.
        //  - Producer reconnect.
        //  - The second time: producer call Send-Command-1.
        if (sourceLId < lastPersistedSourceLedgerId
                || (sourceLId == lastPersistedSourceLedgerId  && sourceEId <= lastPersistedSourceEntryId)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received an msg send receipt[repeated]: source entry {}:{}, latest persisted:"
                                + " {}:{}",
                        topic, producerName, sourceLId, sourceEId,
                        lastPersistedSourceLedgerId, lastPersistedSourceEntryId);
            }
            return;
        }

        // Case-3, which is expected.
        if (pendingLId != null && pendingEId != null && sourceLId == pendingLId.longValue()
                && sourceEId == pendingEId.longValue()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received an msg send receipt[expected]: source entry {}:{}, target entry:"
                                + " {}:{}",
                        topic, producerName, sourceLId, sourceEId,
                        targetLId, targetEid);
            }
            lastPersistedSourceLedgerId = sourceLId;
            lastPersistedSourceEntryId = sourceEId;
            removeAndApplyCallback(op, sourceLId, sourceEId, targetLId, targetEid, false);
            return;
        }

        // Case-4: Unexpected
        //   4-1: got null source cluster's entry position, which is unexpected.
        //   4-2: unknown error, which is unexpected.
        log.error("[{}] [{}] Received an msg send receipt[error]: source entry {}:{}, target entry: {}:{},"
                + " pending send: {}:{}, latest persisted: {}:{}, queue-size: {}",
                topic, producerName, sourceLId, sourceEId, targetLId, targetEid, pendingLId, pendingEId,
                lastPersistedSourceLedgerId, lastPersistedSourceEntryId, pendingMessages.messagesCount());
        cnx.channel().close();
    }

    protected void ackReceivedReplMarker(ClientCnx cnx, OpSendMsg op, long seq, long isSourceMarker,
                                         long ledgerId, long entryId) {
        // Case-1: repeatedly publish repl marker.
        long lastSeqPersisted = LAST_SEQ_ID_PUBLISHED_UPDATER.get(this);
        if (lastSeqPersisted != 0 && seq <= lastSeqPersisted) {
            // Ignoring the ack since it's referring to a message that has already timed out.
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received an repl marker send receipt[repeated]. seq: {}, seqPersisted: {},"
                                + " isSourceMarker: {}, target entry: {}:{}",
                        topic, producerName, seq, lastSeqPersisted, isSourceMarker, ledgerId, entryId);
            }
            return;
        }

        // Case-2, which is expected:
        //   condition: broker responds SendReceipt who is a repl marker.
        //   and condition: the current pending msg is also a marker.
        boolean pendingMsgIsReplMarker = isReplicationMarker(op);
        if (pendingMsgIsReplMarker && seq == op.sequenceId) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received an repl marker send receipt[expected]. seq: {}, seqPersisted: {},"
                                + " isReplMarker: {}, target entry: {}:{}",
                        topic, producerName, seq, lastSeqPersisted, isSourceMarker, ledgerId, entryId);
            }
            long calculatedSeq = getHighestSequenceId(op);
            LAST_SEQ_ID_PUBLISHED_UPDATER.getAndUpdate(this, last -> Math.max(last, calculatedSeq));
            removeAndApplyCallback(op, seq, isSourceMarker, ledgerId, entryId, true);
            return;
        }

        // Case-3, unexpected.
        //   3-1: if "lastSeqPersisted < seq <= lastInProgressSend", rather than going here, it should be a SendError.
        //   3-2: unknown error.
        long lastInProgressSend = LAST_SEQ_ID_PUSHED_UPDATER.get(this);
        String logText = String.format("[%s] [%s] Received an repl marker send receipt[error]. seq: %s, seqPending: %s."
                + " sequenceIdPersisted: %s, lastInProgressSend: %s,"
                + " isSourceMarker: %s, target entry: %s:%s, queue-size: %s",
                topic, producerName, seq, pendingMsgIsReplMarker ? op.sequenceId : "unknown",
                lastSeqPersisted, lastInProgressSend,
                isSourceMarker, ledgerId, entryId, pendingMessages.messagesCount()
        );
        if (seq < lastInProgressSend) {
            log.warn(logText);
        } else {
            log.error(logText);
        }
        // Force connection closing so that messages can be re-transmitted in a new connection.
        cnx.channel().close();
    }

    private void removeAndApplyCallback(OpSendMsg op, long lIdSent, long eIdSent, long ledgerId, long entryId,
                                        boolean isMarker) {
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

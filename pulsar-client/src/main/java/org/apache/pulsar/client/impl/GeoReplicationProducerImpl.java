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

import io.github.merlimat.slog.Logger;
import io.netty.channel.Channel;
import io.netty.util.ReferenceCountUtil;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Markers;

@SuppressWarnings("unchecked")
public class GeoReplicationProducerImpl extends ProducerImpl{

    private static final Logger LOG = Logger.get(GeoReplicationProducerImpl.class);
    private final Logger log;

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
        this.log = LOG.with()
                .attr("topic", topic)
                .attr("producerName", () -> producerName)
                .build();
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
                    log.debug()
                            .attr("seq", seq)
                            .attr("highSeq", highSeq)
                            .log("Got ack for timed out msg");
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
            if (MessageImpl.SchemaState.Broken.equals(op.msg.getSchemaState())) {
                log.error()
                        .attr("sourceLedgerId", sourceLId).attr("sourceEntryId", sourceEId)
                        .attr("pendingLedgerId", pendingLId).attr("pendingEntryId", pendingEId)
                        .attr("persistedSourceLedgerId", lastPersistedSourceLedgerId)
                        .attr("persistedSourceEntryId", lastPersistedSourceEntryId)
                        .attr("pendingQueueSize", pendingMessages.messagesCount())
                        .log("Replication is paused because the schema is incompatible with the remote"
                                + " cluster, please modify the schema compatibility for the remote cluster");
                return;
            }
                log.debug()
                        .attr("sourceLedgerId", sourceLId).attr("sourceEntryId", sourceEId)
                        .attr("pendingLedgerId", pendingLId).attr("pendingEntryId", pendingEId)
                        .attr("persistedSourceLedgerId", lastPersistedSourceLedgerId)
                        .attr("persistedSourceEntryId", lastPersistedSourceEntryId)
                        .log("Received msg send receipt, pending send is repeated due to repl cursor rewind");
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
                log.debug()
                        .attr("entry", sourceLId)
                        .attr("sourceEId", sourceEId)
                        .attr("lastPersistedSourceLedgerId", lastPersistedSourceLedgerId)
                        .attr("lastPersistedSourceEntryId", lastPersistedSourceEntryId)
                        .log("Received repeated msg send receipt");
            return;
        }

        // Case-3, which is expected.
        if (pendingLId != null && pendingEId != null && sourceLId == pendingLId.longValue()
                && sourceEId == pendingEId.longValue()) {
                log.debug()
                        .attr("entry", sourceLId)
                        .attr("sourceEId", sourceEId)
                        .attr("targetLId", targetLId)
                        .attr("targetEid", targetEid)
                        .log("Received expected msg send receipt");
            lastPersistedSourceLedgerId = sourceLId;
            lastPersistedSourceEntryId = sourceEId;
            removeAndApplyCallback(op, sourceLId, sourceEId, targetLId, targetEid, false);
            return;
        }

        // Case-4: Received an out-of-order msg send receipt, and the first item in pending queue is a marker.
        if (op.msg.getMessageBuilder().hasMarkerType()
                && Markers.isReplicationMarker(op.msg.getMessageBuilder().getMarkerType())) {
            log.warn()
                    .attr("sourceLedgerId", sourceLId).attr("sourceEntryId", sourceEId)
                    .attr("markerType", MarkerType.valueOf(op.msg.getMessageBuilder().getMarkerType()))
                    .attr("pendingLedgerId", pendingLId).attr("pendingEntryId", pendingEId)
                    .attr("persistedSourceLedgerId", lastPersistedSourceLedgerId)
                    .attr("persistedSourceEntryId", lastPersistedSourceEntryId)
                    .log("Received out-of-order msg send receipt due to replicated subscription,"
                            + " dropping pending marker command");
            // Drop pending marker. The next ack receipt of this marker message will be dropped after it come in.
            ackReceivedReplMarker(cnx, op, op.sequenceId, -1 /*non-batch message*/, -1, -1);
            // Handle the current send receipt.
            ackReceived(cnx, sourceLId, sourceEId, targetLId, targetEid);
            return;
        }

        // Case-5: Unexpected
        //   5-1: got null source cluster's entry position, which is unexpected.
        //   5-2: unknown error, which is unexpected.
        log.error()
                .attr("sourceLedgerId", sourceLId).attr("sourceEntryId", sourceEId)
                .attr("targetLedgerId", targetLId).attr("targetEntryId", targetEid)
                .attr("pendingLedgerId", pendingLId).attr("pendingEntryId", pendingEId)
                .attr("persistedSourceLedgerId", lastPersistedSourceLedgerId)
                .attr("persistedSourceEntryId", lastPersistedSourceEntryId)
                .attr("queueSize", pendingMessages.messagesCount())
                .log("Received unexpected msg send receipt");
        cnx.channel().close();
    }

    protected void ackReceivedReplMarker(ClientCnx cnx, OpSendMsg op, long seq, long isSourceMarker,
                                         long ledgerId, long entryId) {
        // Case-1: repeatedly publish repl marker.
        long lastSeqPersisted = LAST_SEQ_ID_PUBLISHED_UPDATER.get(this);
        if (lastSeqPersisted != 0 && seq <= lastSeqPersisted) {
            // Ignoring the ack since it's referring to a message that has already timed out.
                log.debug()
                        .attr("seq", seq)
                        .attr("lastSeqPersisted", lastSeqPersisted)
                        .attr("issourcemarker", isSourceMarker)
                        .attr("entry", ledgerId)
                        .attr("entryId", entryId)
                        .log("Received repeated repl marker send receipt");
            return;
        }

        // Case-2, which is expected:
        //   condition: broker responds SendReceipt who is a repl marker.
        //   and condition: the current pending msg is also a marker.
        boolean pendingMsgIsReplMarker = isReplicationMarker(op);
        if (pendingMsgIsReplMarker && seq == op.sequenceId) {
                log.debug()
                        .attr("seq", seq)
                        .attr("lastSeqPersisted", lastSeqPersisted)
                        .attr("isreplmarker", isSourceMarker)
                        .attr("entry", ledgerId)
                        .attr("entryId", entryId)
                        .log("Received expected repl marker send receipt");
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
            log.warn()
                    .attr("sourceMessage", lIdSent)
                    .attr("eIdSent", eIdSent)
                    .attr("targetMsg", ledgerId)
                    .attr("entryId", entryId)
                    .attr("ismarker", isMarker)
                    .exception(t)
                    .log("Got exception while completing the callback");
        }
        ReferenceCountUtil.safeRelease(op.cmd);
        op.recycle();
    }

    private boolean isReplicationMarker(OpSendMsg op) {
        return op.msg != null && op.msg.getMessageBuilder().hasMarkerType()
                && Markers.isReplicationMarker(op.msg.getMessageBuilder().getMarkerType());
    }

    @Override
    public void printWarnLogWhenCanNotDetermineDeduplication(Channel channel, long sourceLId, long sourceEId) {
        log.warn()
                .attr("producerId", producerId)
                .attr("name", producerName)
                .attr("channel", channel)
                .attr("entry", sourceLId)
                .attr("sourceEId", sourceEId)
                .log("Message published by producer has been dropped"
                        + " because broker cannot determine whether"
                        + " it is duplicate");
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

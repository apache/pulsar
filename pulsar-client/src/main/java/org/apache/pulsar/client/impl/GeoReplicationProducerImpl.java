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

@Slf4j
public class GeoReplicationProducerImpl extends ProducerImpl{

    public static String MSG_PROP_REPL_SEQUENCE_LID = "__MSG_PROP_REPL_SEQUENCE_LID";
    public static String MSG_PROP_REPL_SEQUENCE_EID = "__MSG_PROP_REPL_SEQUENCE_EID";

    public GeoReplicationProducerImpl(PulsarClientImpl client, String topic,
                                      ProducerConfigurationData conf,
                                      CompletableFuture producerCreatedFuture, int partitionIndex,
                                      Schema schema, ProducerInterceptors interceptors,
                                      Optional overrideProducerName) {
        super(client, topic, conf, producerCreatedFuture, partitionIndex, schema, interceptors, overrideProducerName);
    }

    @Override
    protected void ackReceived(ClientCnx cnx, long lIdSent, long eIdSent, long ledgerId, long entryId) {
        if (!cnx.isBrokerSupportsDedupReplV2()) {
            super.ackReceived(cnx, lIdSent, eIdSent, ledgerId, entryId);
            return;
        }

        OpSendMsg op = null;
        synchronized (this) {
            op = pendingMessages.peek();
            if (op == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg {}:{}", topic, producerName, lIdSent, eIdSent);
                }
                return;
            }
            Long lIdPendingRes = null;
            Long eIdPendingRes = null;
            List<KeyValue> kvPairList =  op.msg.getMessageBuilder().getPropertiesList();
            for (KeyValue kvPair : kvPairList) {
                if (kvPair.getKey().equals(MSG_PROP_REPL_SEQUENCE_LID)) {
                    if (StringUtils.isNumeric(kvPair.getValue())) {
                        lIdPendingRes = Long.valueOf(kvPair.getValue());
                    } else {
                        break;
                    }
                }
                if (kvPair.getKey().equals(MSG_PROP_REPL_SEQUENCE_EID)) {
                    if (StringUtils.isNumeric(kvPair.getValue())) {
                        eIdPendingRes = Long.valueOf(kvPair.getValue());
                    } else {
                        break;
                    }
                }
                if (lIdPendingRes != null && eIdPendingRes != null) {
                    break;
                }
            }
            if (lIdPendingRes == null || eIdPendingRes == null) {
                // Rollback to the original implementation.
                log.error("[{}] [{}] can not found v2 sequence-id {}:{}, ackReceived: {}:{} {}:{} - queue-size: {}",
                        topic, producerName, lIdPendingRes, eIdPendingRes, lIdSent,
                        eIdSent, ledgerId, entryId, pendingMessages.messagesCount());
                cnx.channel().close();
                return;
            }

            if (lIdSent == lIdPendingRes && eIdSent == eIdPendingRes) {
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
                if (log.isInfoEnabled()) {
                    log.info("Got receipt for producer: [{}] -- source-message: {}:{} -- target-msg: {}:{}",
                            getProducerName(), lIdSent, eIdSent, ledgerId, entryId);
                }
                pendingMessages.remove();
                releaseSemaphoreForSendOp(op);
                // TODO LAST_SEQ_ID_PUBLISHED_UPDATER.getAndUpdate(this, last -> Math.max(last, getHighestSequenceId(finalOp)));
                op.setMessageId(ledgerId, entryId, partitionIndex);
                try {
                    // Need to protect ourselves from any exception being thrown in the future handler from the
                    // application
                    op.sendComplete(null);
                } catch (Throwable t) {
                    log.warn("[{}] [{}] Got exception while completing the callback for -- source-message: {}:{} --"
                                    + " target-msg: {}:{}",
                            topic, producerName, lIdSent, eIdSent, ledgerId, entryId, t);
                }
                ReferenceCountUtil.safeRelease(op.cmd);
                op.recycle();
            } else if (lIdSent < lIdPendingRes || (lIdSent == lIdPendingRes  && eIdSent < eIdPendingRes)) {
                // Ignoring the ack since it's referring to a message that has already timed out.
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg. expecting less or equals {}:{}, but got: {}:{}",
                            topic, producerName, lIdPendingRes, eIdPendingRes, lIdSent,
                            eIdSent);
                }
            } else {
                log.warn("[{}] [{}] Got ack for msg. expecting less or equals {}:{}, but got: {}:{} - queue-size: {}",
                        topic, producerName, lIdPendingRes, eIdPendingRes, lIdSent,
                        eIdSent, pendingMessages.messagesCount());
                // Force connection closing so that messages can be re-transmitted in a new connection
                cnx.channel().close();
            }
        }
    }
}

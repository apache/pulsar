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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

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

    void ackReceived(ClientCnx cnx, long replSequenceLIdSent, long replSequenceEIdSent, long ledgerId, long entryId) {
        // Since parse message metadata cost much, we only check sequence id when a duplication occurs.
        // Why not adding attributes that indicates sequence ids into "OpSendMsg" to avoid parsing the message metadata?
        // - Since the send order conflict is a small probability event, we avoid to add two fields to "OpSendMsg" to
        //   save the cost of high frequency events(publishing messages).
        if (ledgerId >=0 && entryId >=0) {
            return;
        }

        OpSendMsg op = null;
        synchronized (this) {
            op = pendingMessages.peek();

            // Message is coming from replication, we need to use the replication's producer name, ledger id and entry id
            // for the purpose of deduplication.
            int readerIndex = op.cmd.getSecond().readerIndex();
            MessageMetadata md = Commands.parseMessageMetadata(op.cmd.getSecond());
            op.cmd.getSecond().readerIndex(readerIndex);

            Long replSequenceLIdNext = null;
            Long replSequenceEIdNext = null;
            List<KeyValue> kvPairList = md.getPropertiesList();
            for (KeyValue kvPair : kvPairList) {
                if (kvPair.getKey().equals(MSG_PROP_REPL_SEQUENCE_LID)) {
                    if (StringUtils.isNumeric(kvPair.getValue())) {
                        replSequenceLIdNext = Long.valueOf(kvPair.getValue());
                    } else {
                        break;
                    }
                }
                if (kvPair.getKey().equals(MSG_PROP_REPL_SEQUENCE_EID)) {
                    if (StringUtils.isNumeric(kvPair.getValue())) {
                        replSequenceEIdNext = Long.valueOf(kvPair.getValue());
                    } else {
                        break;
                    }
                }
                if (replSequenceLIdNext != null && replSequenceEIdNext != null) {
                    break;
                }
            }
            if (replSequenceLIdNext == null || replSequenceEIdNext == null) {
                // Rollback to the original implementation.
                super.ackReceived(cnx, replSequenceLIdSent, replSequenceEIdSent, ledgerId, entryId);
                return;
            }

            if (replSequenceLIdSent <= replSequenceLIdNext && replSequenceEIdSent < replSequenceEIdNext) {
                // Ignoring the ack since it's referring to a message that has already timed out.
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg. expecting: {} - {} - got: {} - {}",
                            topic, producerName, replSequenceLIdNext, replSequenceEIdNext, replSequenceLIdSent,
                            replSequenceEIdSent);
                }
            } else {
                log.warn("[{}] [{}] Got ack for msg. expecting: {} - {} - got: {} - {} - queue-size: {}",
                        topic, producerName, replSequenceLIdNext, replSequenceEIdNext, replSequenceLIdSent,
                        replSequenceEIdSent, pendingMessages.messagesCount());
                // Force connection closing so that messages can be re-transmitted in a new connection
                cnx.channel().close();
            }
        }
    }
}

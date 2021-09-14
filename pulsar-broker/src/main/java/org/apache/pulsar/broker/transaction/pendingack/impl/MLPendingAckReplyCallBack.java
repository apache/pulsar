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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckReplyCallBack;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckMetadata;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckMetadataEntry;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MLPendingAckStore reply call back.
 */
public class MLPendingAckReplyCallBack implements PendingAckReplyCallBack {

    private final PendingAckHandleImpl pendingAckHandle;

    public MLPendingAckReplyCallBack(PendingAckHandleImpl pendingAckHandle) {
        this.pendingAckHandle = pendingAckHandle;
    }

    @Override
    public void replayComplete() {
        synchronized (pendingAckHandle) {
            log.info("Topic name : [{}], SubName : [{}] pending ack state reply success!",
                    pendingAckHandle.getTopicName(), pendingAckHandle.getSubName());

            if (pendingAckHandle.changeToReadyState()) {
                pendingAckHandle.completeHandleFuture();
                log.info("Topic name : [{}], SubName : [{}] pending ack handle cache request success!",
                        pendingAckHandle.getTopicName(), pendingAckHandle.getSubName());
            } else {
                log.error("Topic name : [{}], SubName : [{}] pending ack state reply fail!",
                        pendingAckHandle.getTopicName(), pendingAckHandle.getSubName());
            }
        }
        pendingAckHandle.handleCacheRequest();
    }

    @Override
    public void handleMetadataEntry(PendingAckMetadataEntry pendingAckMetadataEntry) {
        TxnID txnID = new TxnID(pendingAckMetadataEntry.getTxnidMostBits(),
                pendingAckMetadataEntry.getTxnidLeastBits());
        AckType ackType = pendingAckMetadataEntry.getAckType();
        switch (pendingAckMetadataEntry.getPendingAckOp()) {
            case ABORT:
                pendingAckHandle.handleAbort(txnID, ackType);
                break;
            case COMMIT:
                pendingAckHandle.handleCommit(txnID, ackType, Collections.emptyMap());
                break;
            case ACK:
                if (ackType == AckType.Cumulative) {
                    PendingAckMetadata pendingAckMetadata =
                            pendingAckMetadataEntry.getPendingAckMetadatasList().get(0);
                    pendingAckHandle.handleCumulativeAckRecover(txnID,
                            PositionImpl.get(pendingAckMetadata.getLedgerId(), pendingAckMetadata.getEntryId()));
                } else {
                    List<MutablePair<PositionImpl, Integer>> positions = new ArrayList<>();
                    pendingAckMetadataEntry.getPendingAckMetadatasList().forEach(pendingAckMetadata -> {
                        if (pendingAckMetadata.getAckSetsCount() == 0) {
                            positions.add(new MutablePair<>(PositionImpl.get(pendingAckMetadata.getLedgerId(),
                                    pendingAckMetadata.getEntryId()), pendingAckMetadata.getBatchSize()));
                        } else {
                            PositionImpl position =
                                    PositionImpl.get(pendingAckMetadata.getLedgerId(), pendingAckMetadata.getEntryId());
                            if (pendingAckMetadata.getAckSetsCount() > 0) {
                                long[] ackSets = new long[pendingAckMetadata.getAckSetsCount()];
                                for (int i = 0; i < pendingAckMetadata.getAckSetsCount(); i++) {
                                    ackSets[i] = pendingAckMetadata.getAckSetAt(i);
                                }
                                position.setAckSet(ackSets);
                            }
                            positions.add(new MutablePair<>(position, pendingAckMetadata.getBatchSize()));
                        }
                    });
                    pendingAckHandle.handleIndividualAckRecover(txnID, positions);
                }
                break;
            default:
                throw new IllegalStateException("Transaction pending ack replay "
                        + "error with illegal state : " + pendingAckMetadataEntry.getPendingAckOp());

        }
    }

    private static final Logger log = LoggerFactory.getLogger(MLPendingAckReplyCallBack.class);
}
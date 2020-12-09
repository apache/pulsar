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

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckReplyCallBack;
import org.apache.pulsar.broker.transaction.proto.TransactionPendingAck.PendingAckMetadata;
import org.apache.pulsar.broker.transaction.proto.TransactionPendingAck.PendingAckMetadataEntry;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * MLPendingAckStore reply call back.
 */
public class MLPendingAckReplyCallBack implements PendingAckReplyCallBack {

    private final MLPendingAckStore mlPendingAckStore;

    private final PendingAckHandleImpl pendingAckHandle;

    public MLPendingAckReplyCallBack(MLPendingAckStore mlPendingAckStore,
                                     PendingAckHandleImpl pendingAckHandle) {
        this.mlPendingAckStore = mlPendingAckStore;
        this.pendingAckHandle = pendingAckHandle;
    }

    @Override
    public void replayComplete() {
        log.info("Topic name : [{}], SubName : [{}] pending ack state reply success!",
                pendingAckHandle.getTopicName(), pendingAckHandle.getSubName());

        if (pendingAckHandle.changeToReadyState()) {
            log.info("Topic name : [{}], SubName : [{}] pending ack state reply success!",
                    pendingAckHandle.getTopicName(), pendingAckHandle.getSubName());
            mlPendingAckStore.startTimerTask();
        } else {
            log.error("Topic name : [{}], SubName : [{}] pending ack state reply fail!",
                    pendingAckHandle.getTopicName(), pendingAckHandle.getSubName());
        }
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
                pendingAckHandle.handleCommit(txnID, ackType, null);
                break;
            case ACK:
                if (ackType == AckType.Cumulative) {
                    PendingAckMetadata pendingAckMetadata =
                            pendingAckMetadataEntry.getPendingAckMetadataList().get(0);
                    pendingAckHandle.handleCumulativeAck(txnID,
                            PositionImpl.get(pendingAckMetadata.getLedgerId(), pendingAckMetadata.getEntryId()));
                } else {
                    List<MutablePair<PositionImpl, Integer>> positions = new ArrayList<>();
                    pendingAckMetadataEntry.getPendingAckMetadataList().forEach(pendingAckMetadata -> {
                        if (pendingAckMetadata.getAckSetCount() == 0) {
                            positions.add(new MutablePair<>(PositionImpl.get(pendingAckMetadata.getLedgerId(),
                                    pendingAckMetadata.getEntryId()), pendingAckMetadata.getBatchSize()));
                        } else {
                            PositionImpl position =
                                    PositionImpl.get(pendingAckMetadata.getLedgerId(), pendingAckMetadata.getEntryId());
                            position.setAckSet(SafeCollectionUtils.longListToArray(pendingAckMetadata.getAckSetList()));
                            positions.add(new MutablePair<>(position, pendingAckMetadata.getBatchSize()));
                        }
                    });
                    pendingAckHandle.handleIndividualAck(txnID, positions);
                }
                break;
            default:
                throw new IllegalStateException("Transaction pending ack replay " +
                        "error with illegal state : " + pendingAckMetadataEntry.getPendingAckOp());

        }
    }

    private static final Logger log = LoggerFactory.getLogger(MLPendingAckReplyCallBack.class);
}
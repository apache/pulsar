/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.transaction.buffer.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredTxn;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredPosition;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredStatus;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredTxnMeta;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredEntryInfo;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredTxnID;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredTxnStatus;

class DataFormat {

    static StoredTxn parseStoredTxn(byte[] data) {
        try {
            return StoredTxn.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    static StoredTxn startStore(Position bufferPosition) {
        return StoredTxn.newBuilder().setBufferPosition(buildPosition(bufferPosition)).setStoredStatus(StoredStatus.START)
                        .build();
    }
    static StoredTxn middleStore(Position startPosition, TransactionMetaImpl meta) {
        return StoredTxn.newBuilder().setStoredStatus(StoredStatus.MIDDLE)
                        .setPosition(buildPosition(startPosition)).setTxnMeta(buildStoredTxnMeta(meta)).build();
    }

    static StoredTxn endStore() {
        return StoredTxn.newBuilder().setStoredStatus(StoredStatus.END).build();
    }

    static TransactionMeta parseToTransactionMeta(byte[] data) {
        StoredTxn storedTxn = parseStoredTxn(data);
        return recoverMeta(storedTxn.getTxnMeta());
    }

    private static StoredPosition buildPosition(Position position) {
        PositionImpl newPosition = (PositionImpl) position;
        return StoredPosition.newBuilder().setLedgerId(newPosition.getLedgerId()).setEntryId(newPosition.getEntryId())
                             .build();
    }

    private static Position recoverPosition(StoredPosition position) {
        return new PositionImpl(position.getLedgerId(), position.getEntryId());
    }

    private static StoredTxnMeta buildStoredTxnMeta(TransactionMetaImpl meta) {
        return StoredTxnMeta.newBuilder().setTxnId(buildTxnId(meta.getTxnID()))
                            .setStatus(buildTxnStatus(meta.getTxnStatus()))
                            .addAllEntryInfo(buildEntryInfoList(meta.getEntries()))
                            .setCommittedLedger(meta.getCommittedAtLedgerId())
                            .setCommittedEntry(meta.getCommittedAtEntryId()).build();
    }

    private static TransactionMeta recoverMeta(StoredTxnMeta meta) {
        return TransactionMetaImpl.builder().committedAtLedgerId(meta.getCommittedLedger())
                                  .committedAtEntryId(meta.getCommittedEntry()).txnID(recoverTxnId(meta.getTxnId()))
                                  .entries(recoverEntryInfo(meta.getEntryInfoList()))
                                  .txnStatus(recoverStatus(meta.getStatus())).build();
    }

    private static StoredTxnID buildTxnId(TxnID txnID) {
        return StoredTxnID.newBuilder().setLeastSigBits(txnID.getLeastSigBits()).setMostSigBits(txnID.getMostSigBits())
                          .build();
    }

    private static TxnID recoverTxnId(StoredTxnID txnID) {
        return new TxnID(txnID.getMostSigBits(), txnID.getLeastSigBits());
    }

    private static List<StoredEntryInfo> buildEntryInfoList(SortedMap<Long, Position> entryInfo) {
        return entryInfo.entrySet().stream()
                        .map(info -> buildEntryInfo(info.getKey(), info.getValue()))
                        .collect(Collectors.toList()); }

    private static SortedMap<Long, Position> recoverEntryInfo(List<StoredEntryInfo> entryInfos) {
        SortedMap<Long, Position> map = new TreeMap<>();
        entryInfos.forEach(storedEntryInfo -> map.put(storedEntryInfo.getSequenceId(),
                                                      recoverPosition(storedEntryInfo.getPosition())));
        return map;
    }

    private static StoredEntryInfo buildEntryInfo(Long sequenceId, Position position) {
        return StoredEntryInfo.newBuilder().setSequenceId(sequenceId).setPosition(buildPosition(position)).build();
    }

    private static StoredTxnStatus buildTxnStatus(TxnStatus status) {
        return StoredTxnStatus.valueOf(status.name());
    }

    private static TxnStatus recoverStatus(StoredTxnStatus storedStatus) {
        return TxnStatus.valueOf(storedStatus.name());
    }

}

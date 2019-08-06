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
package org.apache.pulsar.transaction.buffer.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredEntryInfo;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredPosition;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredSnapshotStatus;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredTxnID;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredTxnIndexEntry;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredTxnMeta;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredTxnStatus;

/**
 * Data format used to format index snapshot entry.
 */
public class DataFormat {
    static StoredTxnIndexEntry parseStoredTxn(byte[] data) {
        try {
            return StoredTxnIndexEntry.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    static StoredTxnIndexEntry newSnapshotStartEntry(Position bufferPosition) {
        return StoredTxnIndexEntry.newBuilder().setPosition(buildPosition(bufferPosition))
                                  .setStoredStatus(StoredSnapshotStatus.START).build();
    }
    static StoredTxnIndexEntry newSnapshotMiddleEntry(Position startPosition, TransactionMetaImpl meta) {
        return StoredTxnIndexEntry.newBuilder().setStoredStatus(StoredSnapshotStatus.MIDDLE)
                                  .setPosition(buildPosition(startPosition)).setTxnMeta(buildStoredTxnMeta(meta))
                                  .build();
    }

    static StoredTxnIndexEntry newSnapshotEndEntry(Position startPosition) {
        return StoredTxnIndexEntry.newBuilder().setStoredStatus(StoredSnapshotStatus.END)
                                  .setPosition(buildPosition(startPosition)).build();
    }

    static TransactionMeta parseToTransactionMeta(byte[] data) {
        StoredTxnIndexEntry storedTxn = parseStoredTxn(data);
        return recoverMeta(storedTxn.getTxnMeta());
    }

    private static StoredPosition buildPosition(Position position) {
        PositionImpl newPosition = (PositionImpl) position;
        return StoredPosition.newBuilder().setLedgerId(newPosition.getLedgerId()).setEntryId(newPosition.getEntryId())
                             .build();
    }

    static Position recoverPosition(StoredPosition position) {
        return new PositionImpl(position.getLedgerId(), position.getEntryId());
    }

    private static StoredTxnMeta buildStoredTxnMeta(TransactionMetaImpl meta) {
        return StoredTxnMeta.newBuilder().setTxnId(buildTxnId(meta.getTxnID()))
                            .setStatus(buildTxnStatus(meta.getTxnStatus()))
                            .addAllEntryInfo(buildEntryInfoList(meta.getEntries()))
                            .setCommittedLedger(meta.getCommittedAtLedgerId())
                            .setCommittedEntry(meta.getCommittedAtEntryId()).build();
    }

    static TransactionMetaImpl recoverMeta(StoredTxnMeta meta) {
        return new TransactionMetaImpl(recoverTxnId(meta.getTxnId()), recoverEntries(meta.getEntryInfoList()),
                                       recoverStatus(meta.getStatus()), meta.getCommittedLedger(),
                                       meta.getCommittedEntry());
    }

    private static StoredTxnID buildTxnId(TxnID txnID) {
        return StoredTxnID.newBuilder().setLeastSigBits(txnID.getLeastSigBits()).setMostSigBits(txnID.getMostSigBits())
                          .build();
    }

    private static TxnID recoverTxnId(StoredTxnID txnID) {
        return new TxnID(txnID.getMostSigBits(), txnID.getLeastSigBits());
    }

    private static List<StoredEntryInfo> buildEntryInfoList(SortedMap<Long, Position> entries) {
        return entries.entrySet().stream()
                      .map(entry -> StoredEntryInfo.newBuilder()
                                                   .setPosition(buildPosition(entry.getValue()))
                                                   .setSequenceId(entry.getKey())
                                                   .build())
                      .collect(Collectors.toList());
    }
    @Builder
    @Getter
    final static class EntryInfo {
        long sequenceId;
        Position position;
    }

    private static SortedMap<Long, Position> recoverEntries(List<StoredEntryInfo> entries) {
        Map<Long, Position> index = entries.stream().map(
            storedEntryInfo -> EntryInfo.builder().sequenceId(storedEntryInfo.getSequenceId())
                                        .position(recoverPosition(storedEntryInfo.getPosition())).build())
                                           .collect(Collectors.toMap(EntryInfo::getSequenceId, EntryInfo::getPosition));

        return new TreeMap<>(index);
    }

    private static StoredTxnStatus buildTxnStatus(TxnStatus status) {
        return StoredTxnStatus.valueOf(status.name());
    }

    private static TxnStatus recoverStatus(StoredTxnStatus storedStatus) {
        return TxnStatus.valueOf(storedStatus.name());
    }
}

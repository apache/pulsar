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
package org.apache.bookkeeper.mledger.offload;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.KeyValue;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.OffloadContext;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.OffloadDriverMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.DataFormats;

@Slf4j
public final class OffloadUtils {

    private OffloadUtils() {}

    public static Map<String, String> getOffloadDriverMetadata(LedgerInfo ledgerInfo) {
        Map<String, String> metadata = Maps.newHashMap();
        if (ledgerInfo.hasOffloadContext()) {
            OffloadContext ctx = ledgerInfo.getOffloadContext();
            if (ctx.hasDriverMetadata()) {
                OffloadDriverMetadata driverMetadata = ctx.getDriverMetadata();
                if (driverMetadata.getPropertiesCount() > 0) {
                    driverMetadata.getPropertiesList().forEach(kv -> metadata.put(kv.getKey(), kv.getValue()));
                }
            }
        }
        return metadata;
    }

    public static Map<String, String> getOffloadDriverMetadata(LedgerInfo ledgerInfo,
                                                               Map<String, String> defaultOffloadDriverMetadata) {
        if (ledgerInfo.hasOffloadContext()) {
            OffloadContext ctx = ledgerInfo.getOffloadContext();
            if (ctx.hasDriverMetadata()) {
                OffloadDriverMetadata driverMetadata = ctx.getDriverMetadata();
                if (driverMetadata.getPropertiesCount() > 0) {
                    Map<String, String> metadata = Maps.newHashMap();
                    driverMetadata.getPropertiesList().forEach(kv -> metadata.put(kv.getKey(), kv.getValue()));
                    return metadata;
                }
            }
        }
        return defaultOffloadDriverMetadata;
    }

    public static String getOffloadDriverName(LedgerInfo ledgerInfo, String defaultDriverName) {
        if (ledgerInfo.hasOffloadContext()) {
            OffloadContext ctx = ledgerInfo.getOffloadContext();
            if (ctx.hasDriverMetadata()) {
                OffloadDriverMetadata driverMetadata = ctx.getDriverMetadata();
                if (driverMetadata.hasName()) {
                    return driverMetadata.getName();
                }
            }
        }
        return defaultDriverName;
    }

    public static void setOffloadDriverMetadata(LedgerInfo.Builder infoBuilder,
                                                String driverName,
                                                Map<String, String> offloadDriverMetadata) {
        infoBuilder.getOffloadContextBuilder()
            .getDriverMetadataBuilder()
            .setName(driverName);
        infoBuilder.getOffloadContextBuilder().getDriverMetadataBuilder().clearProperties();
        offloadDriverMetadata.forEach((k, v) -> infoBuilder
                .getOffloadContextBuilder()
                .getDriverMetadataBuilder()
                .addProperties(KeyValue.newBuilder()
                        .setKey(k)
                        .setValue(v)
                        .build()));
    }

    public static byte[] buildLedgerMetadataFormat(LedgerMetadata metadata) {
        DataFormats.LedgerMetadataFormat.Builder builder = DataFormats.LedgerMetadataFormat.newBuilder();
        builder.setQuorumSize(metadata.getWriteQuorumSize())
                .setAckQuorumSize(metadata.getAckQuorumSize())
                .setEnsembleSize(metadata.getEnsembleSize())
                .setLength(metadata.getLength())
                .setState(metadata.isClosed() ? DataFormats.LedgerMetadataFormat.State.CLOSED : DataFormats.LedgerMetadataFormat.State.OPEN)
                .setLastEntryId(metadata.getLastEntryId())
                .setCtime(metadata.getCtime())
                .setDigestType(BookKeeper.DigestType.toProtoDigestType(
                        BookKeeper.DigestType.fromApiDigestType(metadata.getDigestType())));

        for (Map.Entry<String, byte[]> e : metadata.getCustomMetadata().entrySet()) {
            builder.addCustomMetadataBuilder()
                    .setKey(e.getKey()).setValue(ByteString.copyFrom(e.getValue()));
        }

        for (Map.Entry<Long, ? extends List<BookieId>> e : metadata.getAllEnsembles().entrySet()) {
            builder.addSegmentBuilder()
                    .setFirstEntryId(e.getKey())
                    .addAllEnsembleMember(e.getValue().stream().map(BookieId::toString).collect(Collectors.toList()));
        }

        return builder.build().toByteArray();
    }

    public static LedgerMetadata parseLedgerMetadata(long id, byte[] bytes) throws IOException {
        DataFormats.LedgerMetadataFormat ledgerMetadataFormat = DataFormats.LedgerMetadataFormat.newBuilder().mergeFrom(bytes).build();
        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create()
                .withLastEntryId(ledgerMetadataFormat.getLastEntryId())
                .withPassword(ledgerMetadataFormat.getPassword().toByteArray())
                .withClosedState()
                .withId(id)
                .withMetadataFormatVersion(2)
                .withLength(ledgerMetadataFormat.getLength())
                .withAckQuorumSize(ledgerMetadataFormat.getAckQuorumSize())
                .withCreationTime(ledgerMetadataFormat.getCtime())
                .withWriteQuorumSize(ledgerMetadataFormat.getQuorumSize())
                .withEnsembleSize(ledgerMetadataFormat.getEnsembleSize());
        ledgerMetadataFormat.getSegmentList().forEach(segment -> {
            ArrayList<BookieId> addressArrayList = new ArrayList<>();
            segment.getEnsembleMemberList().forEach(address -> {
                try {
                    addressArrayList.add(BookieId.parse(address));
                } catch (IllegalArgumentException e) {
                    log.error("Exception when create BookieId {}. ", address, e);
                }
            });
            builder.newEnsembleEntry(segment.getFirstEntryId(), addressArrayList);
        });

        if (ledgerMetadataFormat.getCustomMetadataCount() > 0) {
            Map<String, byte[]> customMetadata = Maps.newHashMap();
            ledgerMetadataFormat.getCustomMetadataList().forEach(
                    entry -> customMetadata.put(entry.getKey(), entry.getValue().toByteArray()));
            builder.withCustomMetadata(customMetadata);
        }

        switch (ledgerMetadataFormat.getDigestType()) {
            case HMAC:
                builder.withDigestType(DigestType.MAC);
                break;
            case CRC32:
                builder.withDigestType(DigestType.CRC32);
                break;
            case CRC32C:
                builder.withDigestType(DigestType.CRC32C);
                break;
            case DUMMY:
                builder.withDigestType(DigestType.DUMMY);
                break;
            default:
                throw new IllegalArgumentException("Unable to convert digest type " + ledgerMetadataFormat.getDigestType());
        }

        return builder.build();
    }
}

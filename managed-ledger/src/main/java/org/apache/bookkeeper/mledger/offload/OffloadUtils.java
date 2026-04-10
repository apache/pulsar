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
package org.apache.bookkeeper.mledger.offload;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.common.util.Retries;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.proto.KeyValue;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.OffloadContext;
import org.apache.bookkeeper.mledger.proto.OffloadDriverMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.DataFormats;

@CustomLog
public final class OffloadUtils {

    private OffloadUtils() {}

    public static Map<String, String> getOffloadDriverMetadata(LedgerInfo ledgerInfo) {
        Map<String, String> metadata = new HashMap<>();
        if (ledgerInfo.hasOffloadContext()) {
            OffloadContext ctx = ledgerInfo.getOffloadContext();
            if (ctx.hasDriverMetadata()) {
                OffloadDriverMetadata driverMetadata = ctx.getDriverMetadata();
                if (driverMetadata.getPropertiesCount() > 0) {
                    for (int i = 0; i < driverMetadata.getPropertiesCount(); i++) {
                        KeyValue kv = driverMetadata.getPropertyAt(i);
                        metadata.put(kv.getKey(), kv.getValue());
                    }
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
                    Map<String, String> metadata = new HashMap<>();
                    for (int i = 0; i < driverMetadata.getPropertiesCount(); i++) {
                        KeyValue kv = driverMetadata.getPropertyAt(i);
                        metadata.put(kv.getKey(), kv.getValue());
                    }
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

    public static void setOffloadDriverMetadata(LedgerInfo infoBuilder,
                                                String driverName,
                                                Map<String, String> offloadDriverMetadata) {
        OffloadDriverMetadata driverMeta = infoBuilder.setOffloadContext().setDriverMetadata();
        driverMeta.setName(driverName);
        driverMeta.clearProperties();
        offloadDriverMetadata.forEach((k, v) -> driverMeta.addProperty().setKey(k).setValue(v));
    }

    public static byte[] buildLedgerMetadataFormat(LedgerMetadata metadata) {
        DataFormats.LedgerMetadataFormat.Builder builder = DataFormats.LedgerMetadataFormat.newBuilder();
        builder.setQuorumSize(metadata.getWriteQuorumSize())
                .setAckQuorumSize(metadata.getAckQuorumSize())
                .setEnsembleSize(metadata.getEnsembleSize())
                .setLength(metadata.getLength())
                .setState(metadata.isClosed() ? DataFormats.LedgerMetadataFormat.State.CLOSED :
                        DataFormats.LedgerMetadataFormat.State.OPEN)
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
        DataFormats.LedgerMetadataFormat ledgerMetadataFormat = DataFormats.LedgerMetadataFormat.newBuilder()
                .mergeFrom(bytes).build();
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
                    log.error().attr("address", address).exception(e).log("Exception when create BookieId");
                }
            });
            builder.newEnsembleEntry(segment.getFirstEntryId(), addressArrayList);
        });

        if (ledgerMetadataFormat.getCustomMetadataCount() > 0) {
            Map<String, byte[]> customMetadata = new HashMap<>();
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
                throw new IllegalArgumentException("Unable to convert digest type "
                        + ledgerMetadataFormat.getDigestType());
        }

        return builder.build();
    }

    public static CompletableFuture<Void> cleanupOffloaded(long ledgerId, UUID uuid, ManagedLedgerConfig mlConfig,
                                     Map<String, String> offloadDriverMetadata, String cleanupReason,
                                     String name, org.apache.bookkeeper.common.util.OrderedScheduler executor) {
        log.info().attr("name", name)
                .attr("ledgerId", ledgerId)
                .attr("uuid", uuid.toString())
                .attr("cleanupReason", cleanupReason)
                .log("Cleanup offload");
        Map<String, String> metadataMap = new HashMap<>(offloadDriverMetadata);
        metadataMap.put("ManagedLedgerName", name);

        return Retries.run(Backoff.exponentialJittered(TimeUnit.SECONDS.toMillis(1),
                        TimeUnit.HOURS.toMillis(1)).limit(10),
                Retries.NonFatalPredicate,
                () -> mlConfig.getLedgerOffloader().deleteOffloaded(ledgerId, uuid, metadataMap),
                executor, name).whenComplete((ignored, exception) -> {
            if (exception != null) {
                log.warn().attr("name", name)
                        .attr("ledgerId", ledgerId)
                        .attr("cleanupReason", cleanupReason)
                        .exception(exception)
                        .log("Error cleaning up offload");
            }
        });
    }

}

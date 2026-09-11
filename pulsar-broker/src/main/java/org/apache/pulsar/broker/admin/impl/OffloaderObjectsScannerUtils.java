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
package org.apache.pulsar.broker.admin.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;

@CustomLog
@UtilityClass
public class OffloaderObjectsScannerUtils {

    private static final String STATUS_OK = "OK";
    private static final String STATUS_UNKNOWN = "UNKNOWN";
    private static final String STATUS_BAD_UUID = "BAD-UUID";
    private static final String STATUS_NOT_FOUND = "NOT-FOUND";
    private static final String STATUS_NOT_OFFLOADED = "NOT-OFFLOADED";

    public interface ScannerResultSink {

        void object(Map<String, Object> data) throws Exception;
        void finished(int total, int errors, int unknown) throws Exception;
    }

    static void scanOffloadedLedgers(LedgerOffloader managedLedgerOffloader,
                                                              String localClusterName,
                                                              ManagedLedgerFactory managedLedgerFactory,
                                                              ScannerResultSink sink) throws Exception {
        AtomicInteger totalCount = new AtomicInteger();
        AtomicInteger totalErrors = new AtomicInteger();
        AtomicInteger totalUnknown = new AtomicInteger();
        managedLedgerOffloader.scanLedgers((md -> {
            log.info().attr("metadata", md).log("Found ledger");
            Map<String, Object> objectInfo = new HashMap<>();
            objectInfo.put("ledger", md.getLedgerId());
            objectInfo.put("name", md.getName());
            objectInfo.put("uri", md.getUri());
            objectInfo.put("uuid", md.getUuid());
            objectInfo.put("size", md.getSize());
            objectInfo.put("lastModified", md.getLastModified());
            objectInfo.put("userMetadata", md.getUserMetadata());

            String status = STATUS_UNKNOWN;

            if (md.getUserMetadata() != null) {
                // non case sensitive
                TreeMap<String, String> userMetadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                userMetadata.putAll(md.getUserMetadata());
                String clusterName = userMetadata.get(LedgerOffloader.METADATA_PULSAR_CLUSTER_NAME);
                if (localClusterName.equals(clusterName)) {
                    String managedLedgerName = userMetadata.get("managedledgername");
                    if (managedLedgerName != null) {
                        objectInfo.put("managedLedgerName", managedLedgerName);
                        try {
                            status = checkLedgerShouldBeOnTieredStorage(md.getLedgerId(), md.getUuid(),
                                    managedLedgerName, objectInfo, managedLedgerFactory);
                        } catch (InterruptedException err) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(err);
                        } catch (ManagedLedgerException err) {
                            log.error().attr("managedLedger", managedLedgerName)
                                    .log("Error while checking managed ledger");
                            throw new RuntimeException(err);
                        }
                    }
                }
            }
            totalCount.incrementAndGet();
            objectInfo.put("status", status);
            switch (status) {
                case STATUS_OK:
                    break;
                case STATUS_UNKNOWN:
                    totalUnknown.incrementAndGet();
                    break;
                default:
                    totalErrors.incrementAndGet();
                    break;
            }

            sink.object(objectInfo);
            return true;
        }), managedLedgerOffloader.getOffloadDriverMetadata());

        sink.finished(totalCount.get(), totalErrors.get(), totalUnknown.get());
    }

    private static String checkLedgerShouldBeOnTieredStorage(long ledgerId, String uuid,
            String managedLedgerName,
            Map<String, Object> data,
            ManagedLedgerFactory managedLedgerFactory)
        throws InterruptedException, ManagedLedgerException  {
        try {
            ManagedLedgerInfo managedLedgerInfo = managedLedgerFactory.getManagedLedgerInfo(managedLedgerName);
            ManagedLedgerInfo.LedgerInfo ledgerInfo = managedLedgerInfo
                    .ledgers.stream().filter(l -> l.ledgerId == ledgerId).findAny().orElse(null);
            if (ledgerInfo == null) {
                log.info()
                        .attr("managedLedger", managedLedgerName)
                        .attr("ledgerId", ledgerId)
                        .log("Managed ledger does not contain ledger");
                return STATUS_NOT_FOUND;
            }
            data.put("numEntries", ledgerInfo.entries);
            data.put("offloaded", ledgerInfo.isOffloaded);
            if (!ledgerInfo.isOffloaded) {
                log.info()
                        .attr("ledgerId", ledgerId)
                        .attr("managedLedger", managedLedgerName)
                        .log("Ledger is not marked as OFFLOADED");
                return STATUS_NOT_OFFLOADED;
            }
            String uuidOnMetadata = ledgerInfo.offloadedContextUuid;
            if (!Objects.equals(uuidOnMetadata, uuid)) {
                log.info()
                        .attr("ledgerId", ledgerId)
                        .attr("metadataUuid", uuidOnMetadata)
                        .attr("uuid", uuid)
                        .log("Ledger uuid does not match name uuid");
                return STATUS_BAD_UUID;
            }
            return "OK";
        }  catch (ManagedLedgerException.ManagedLedgerNotFoundException
                | ManagedLedgerException.MetadataNotFoundException notFound) {
            log.info()
                    .attr("managedLedger", managedLedgerName)
                    .log("Managed ledger does not exist (maybe the topic has been deleted)");
            return STATUS_NOT_FOUND;
        }
    }

}

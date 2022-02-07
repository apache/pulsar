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
package org.apache.pulsar.compaction;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffloadCompactedTopicImpl extends CompactedTopicImpl {
    public static final String UUID_MSB_NAME = "CompactedTopicOffloaderMsb";
    public static final String UUID_LSB_NAME = "CompactedTopicOffloaderLsb";
    private final LedgerOffloader ledgerOffloader;
    private final UUID uid;
    private final Map<String, String> offloadDriverMetadata;

    private CompletableFuture<CompactedTopicContext> compactedTopicContext = null;

    public OffloadCompactedTopicImpl(LedgerOffloader ledgerOffloader,
            UUID uid, Map<String, String> offloadDriverMetadata) {
        super(null);
        this.ledgerOffloader = ledgerOffloader;
        this.uid = uid;
        this.offloadDriverMetadata = offloadDriverMetadata;
    }

    @Override
    public CompletableFuture<CompactedTopicContext> newCompactedLedger(Position p, long compactedLedgerId) {
        synchronized (this) {
            compactionHorizon = (PositionImpl) p;

            CompletableFuture<CompactedTopicContext> previousContext = compactedTopicContext;
            compactedTopicContext = openCompactedLedger(ledgerOffloader, uid, offloadDriverMetadata,
                    compactedLedgerId);

            // delete the ledger from the old context once the new one is open
            return compactedTopicContext.thenCompose(__ ->
                    previousContext != null ? previousContext : CompletableFuture.completedFuture(null));
        }
    }

    @Override
    public CompletableFuture<Void> deleteCompactedLedger(long compactedLedgerId) {
        return tryDeleteCompactedLedger(ledgerOffloader, uid, offloadDriverMetadata, compactedLedgerId);
    }

    private CompletableFuture<CompactedTopicContext> openCompactedLedger(LedgerOffloader lo,
            UUID uid, Map<String, String> offloadDriverMetadata, long id) {

        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();

        lo.readOffloaded(id, uid, offloadDriverMetadata)
                .whenComplete((readHandle, ex) -> {
                    if (ex != null) {
                        promise.completeExceptionally(ex);
                    } else {
                        promise.complete(readHandle);
                    }
                });

        return promise.thenApply((readHandle) -> new CompactedTopicContext(
                readHandle, createCache(readHandle, DEFAULT_STARTPOINT_CACHE_SIZE)));
    }

    static CompletableFuture<Void> tryDeleteCompactedLedger(LedgerOffloader lo, UUID uid,
            Map<String, String> offloadDriverMetadata, long id) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        lo.deleteOffloaded(id, uid, offloadDriverMetadata)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        log.warn("Error deleting offload compacted topic ledger {}",
                                id, ex);
                    } else {
                        log.debug("Offload compacted topic ledger deleted successfully");
                    }
                });
        return promise;
    }

    public Optional<CompactedTopicContext> getCompactedTopicContext() {
        if (compactedTopicContext != null) {
            try {
                return Optional.of(compactedTopicContext.get());
            } catch (InterruptedException | ExecutionException e) {
                log.warn("Error fetch compacted topic context failed", e);
            }
        }
        return Optional.empty();
    }

    private static final Logger log = LoggerFactory.getLogger(OffloadCompactedTopicImpl.class);
}


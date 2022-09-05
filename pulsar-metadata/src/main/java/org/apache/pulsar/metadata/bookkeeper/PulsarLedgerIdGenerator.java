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
package org.apache.pulsar.metadata.bookkeeper;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;

@Slf4j
public class PulsarLedgerIdGenerator implements LedgerIdGenerator {

    private final MetadataStoreExtended store;
    private final String ledgerIdGenPath;
    private final String shortIdGenPath;

    private static final String IDGEN_NODE = "idgen-long";
    private static final String IDGEN_SHORT_NODE = "idgen";

    private static final String SHORT_ID_PREFIX = "ID-";

    public PulsarLedgerIdGenerator(MetadataStoreExtended store, String ledgersRoot) {
        this.store = store;
        this.ledgerIdGenPath = ledgersRoot + "/" + IDGEN_NODE;
        this.shortIdGenPath = ledgersRoot + "/" + IDGEN_SHORT_NODE;
    }

    @Override
    public void generateLedgerId(BookkeeperInternalCallbacks.GenericCallback<Long> genericCallback) {
        ledgerIdGenPathPresent()
                .thenCompose(isIdGenPathPresent -> {
                    if (isIdGenPathPresent) {
                        // We've already started generating 63-bit ledger IDs.
                        // Keep doing that.
                        return generateLongLedgerId();
                    } else {
                        // We've not moved onto 63-bit ledgers yet.
                        return generateShortLedgerId();
                    }
                }).thenAccept(ledgerId ->
                genericCallback.operationComplete(BKException.Code.OK, ledgerId)
        ).exceptionally(ex -> {
            log.error("Error generating ledger id: {}", ex.getMessage());
            genericCallback.operationComplete(BKException.Code.MetaStoreException, -1L);
            return null;
        });

    }

    private CompletableFuture<Long> generateShortLedgerId() {
        // Make sure the short-id gen path exists as a persistent node
        return store.exists(shortIdGenPath)
                .thenCompose(exists -> {
                    if (exists) {
                        // Proceed
                        return internalGenerateShortLedgerId();
                    } else {
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        store.put(shortIdGenPath, new byte[0], Optional.of(-1L))
                                .whenComplete((stat, throwable) -> {
                                    Throwable cause = FutureUtil.unwrapCompletionException(throwable);
                                    if (cause == null
                                            || cause instanceof MetadataStoreException.BadVersionException) {
                                        // creat shortIdGenPath success or it already created by others.
                                        future.complete(null);
                                    } else {
                                        future.completeExceptionally(throwable);
                                    }
                                });
                        return future.thenCompose(__ -> internalGenerateShortLedgerId());
                    }
                });
    }

    private CompletableFuture<Long> internalGenerateShortLedgerId() {
        final String ledgerPrefix = this.shortIdGenPath + "/" + SHORT_ID_PREFIX;

        return store.put(ledgerPrefix, new byte[0], Optional.of(-1L),
                EnumSet.of(CreateOption.Ephemeral, CreateOption.Sequential))
                .thenCompose(stat -> {
                    // delete the znode for id generation
                    store.delete(handleTheDeletePath(stat.getPath()), Optional.empty()).
                            exceptionally(ex -> {
                                log.warn("Exception during deleting node for id generation: ", ex);
                                return null;
                            });

                    // Extract ledger id from generated path
                    long ledgerId;
                    try {
                        ledgerId = getLedgerIdFromGenPath(stat.getPath(), ledgerPrefix);
                        if (ledgerId < 0 || ledgerId >= Integer.MAX_VALUE) {
                            // 31-bit IDs overflowed. Start using 63-bit ids.
                            return store.put(ledgerIdGenPath, new byte[0], Optional.empty())
                                    .thenCompose(__ -> generateLongLedgerId());
                        } else {
                            return CompletableFuture.completedFuture(ledgerId);
                        }
                    } catch (IOException e) {
                        log.error("Could not extract ledger-id from id gen path:" + stat.getPath(), e);
                        return FutureUtil.failedFuture(e);
                    }
                });
    }

    private CompletableFuture<Long> generateLongLedgerId() {
        final String hobPrefix = "HOB-";
        final String ledgerPrefix = this.ledgerIdGenPath + "/" + hobPrefix;

        return store.getChildren(ledgerIdGenPath).thenCompose(highOrderDirectories -> {
            Optional<Long> largest = highOrderDirectories.stream().map((t) -> {
                try {
                    return Long.parseLong(t.replace(hobPrefix, ""));
                } catch (NumberFormatException e) {
                    return null;
                }
            }).filter((t) -> t != null)
                    .reduce(Math::max);

            // If we didn't get any valid IDs from the directory...
            if (!largest.isPresent()) {
                // else, Start at HOB-0000000001;
                return createHOBPathAndGenerateId(ledgerPrefix, 1);
            }

            // Found the largest.
            // Get the low-order bits.
            final Long highBits = largest.get();
            return generateLongLedgerIdLowBits(ledgerPrefix, highBits)
                    .thenApply(ledgerId -> {
                        // Perform garbage collection on HOB- directories.
                        // Keeping 3 should be plenty to prevent races
                        if (highOrderDirectories.size() > 3) {
                            Object[] highOrderDirs = highOrderDirectories.stream()
                                    .map((t) -> {
                                        try {
                                            return Long.parseLong(t.replace(hobPrefix, ""));
                                        } catch (NumberFormatException e) {
                                            return null;
                                        }
                                    })
                                    .filter((t) -> t != null)
                                    .sorted()
                                    .toArray();

                            for (int i = 0; i < highOrderDirs.length - 3; i++) {
                                String path = ledgerPrefix + formatHalfId(((Long) highOrderDirs[i]).intValue());
                                if (log.isDebugEnabled()) {
                                    log.debug("DELETING HIGH ORDER DIR: {}", path);
                                }
                                store.delete(path, Optional.of(0L));
                            }
                        }
                        return ledgerId;
                    });
        });
    }

    /**
     * Formats half an ID as 10-character 0-padded string.
     * @param i - 32 bits of the ID to format
     * @return a 10-character 0-padded string.
     */
    private String formatHalfId(int i) {
        return String.format("%010d", i);
    }

    private CompletableFuture<Long> createHOBPathAndGenerateId(String ledgerPrefix, int hob) {
        if (log.isDebugEnabled()) {
            log.debug("Creating HOB path: {}", ledgerPrefix + formatHalfId(hob));
        }

        CompletableFuture<Long> future = new CompletableFuture<>();
        store.put(ledgerPrefix + formatHalfId(hob), new byte[0], Optional.empty())
                .whenComplete((__, ex) -> {
                    ex = FutureUtil.unwrapCompletionException(ex);
                    if (ex != null && !(ex instanceof MetadataStoreException.BadVersionException)) {
                        // BadVersion is OK here because we can have multiple threads (or nodes) trying to create the
                        // new HOB path
                        future.completeExceptionally(ex);
                    } else {
                        // We just created a new HOB directory, try again
                        generateLongLedgerId().thenAccept(future::complete)
                                .exceptionally(e -> {
                                    future.completeExceptionally(e);
                                    return null;
                                });
                    }
                });

        return future;
    }

    private CompletableFuture<Long> generateLongLedgerIdLowBits(final String ledgerPrefix, long highBits) {
        String highPath = ledgerPrefix + formatHalfId((int) highBits);
        return generateLedgerIdImpl(createLedgerPrefix(highPath, null))
                .thenCompose(result -> {
                    if (result >= 0L && result < 2147483647L) {
                        return CompletableFuture.completedFuture((highBits << 32) | result);
                    } else {
                        // Lower bits are full. Need to expand and create another HOB node.
                        Long newHighBits = highBits + 1;
                        return createHOBPathAndGenerateId(ledgerPrefix, newHighBits.intValue());
                    }
                });
    }

    public CompletableFuture<Long> generateLedgerIdImpl(final String prefix) {
        return store
                .put(prefix, new byte[0], Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral, CreateOption.Sequential))
                .thenCompose(stat -> {
                    // delete the znode for id generation
                    store.delete(handleTheDeletePath(stat.getPath()), Optional.empty()).
                            exceptionally(ex -> {
                                log.warn("Exception during deleting node for id generation: ", ex);
                                return null;
                            });

                    try {
                        long ledgerId = getLedgerIdFromGenPath(stat.getPath(), prefix);
                        return CompletableFuture.completedFuture(ledgerId);
                    } catch (IOException e) {
                        log.error("Could not extract ledger-id from id gen path:" + stat.getPath(), e);
                        return FutureUtil.failedFuture(e);
                    }
                });
    }

    @Override
    public void close() throws IOException {

    }

    /**
     * Checks the existence of the long ledger id gen path. Existence indicates we have switched from the legacy
     * algorithm to the new method of generating 63-bit ids. If the existence is UNKNOWN, it looks in zk to
     * find out. If it previously checked in zk, it returns that value. This value changes when we run out
     * of ids < Integer.MAX_VALUE, and try to create the long ledger id gen path.
     */
    public CompletableFuture<Boolean> ledgerIdGenPathPresent() {
        return store.exists(ledgerIdGenPath);
    }

    private static long getLedgerIdFromGenPath(String nodeName, String ledgerPrefix) throws IOException {
        try {
            String[] parts = nodeName.split(ledgerPrefix);
            long ledgerId = Long.parseLong(parts[parts.length - 1]);
            return ledgerId;
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
    }

    private static String createLedgerPrefix(String ledgersPath, String idGenZnodeName) {
        String ledgerIdGenPath = null;
        if (StringUtils.isBlank(idGenZnodeName)) {
            ledgerIdGenPath = ledgersPath;
        } else {
            ledgerIdGenPath = ledgersPath + "/" + idGenZnodeName;
        }

        return ledgerIdGenPath + "/" + "ID-";
    }

    //If the config rootPath when use zk metadata store, it will append rootPath as the prefix of the path.
    //So when we get the path from the stat, we should truncate the rootPath.
    private String handleTheDeletePath(String path) {
        if (store instanceof ZKMetadataStore) {
            String rootPath = ((ZKMetadataStore) store).getRootPath();
            if (rootPath == null) {
                return path;
            }
            return path.replaceFirst(rootPath, "");
        }
        return path;
    }
}

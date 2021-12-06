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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.zookeeper.AsyncCallback;

@Slf4j
public class PulsarLedgerManager implements LedgerManager {

    private final String ledgerRootPath;
    private final MetadataStore store;
    private final MetadataCache<LedgerMetadata> cache;
    private final LedgerMetadataSerDe serde;

    private final LegacyHierarchicalLedgerManager legacyLedgerManager;
    private final LongHierarchicalLedgerManager longLedgerManager;

    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-bk-ledger-manager-scheduler"));

    // ledger metadata listeners
    protected final ConcurrentMap<Long, Set<BookkeeperInternalCallbacks.LedgerMetadataListener>> listeners =
            new ConcurrentHashMap<>();

    PulsarLedgerManager(MetadataStore store, String ledgersRootPath) {
        this.ledgerRootPath = ledgersRootPath;
        this.store = store;
        this.legacyLedgerManager = new LegacyHierarchicalLedgerManager(store, scheduler, ledgerRootPath);
        this.longLedgerManager = new LongHierarchicalLedgerManager(store, scheduler, ledgerRootPath);
        this.serde = new LedgerMetadataSerDe();
        this.cache = store.getMetadataCache(new MetadataSerde<LedgerMetadata>() {
            @Override
            public byte[] serialize(String path, LedgerMetadata value) throws IOException {
                return serde.serialize(value);
            }

            @Override
            public LedgerMetadata deserialize(String path, byte[] content, Stat stat) throws IOException {
                return serde.parseConfig(content, getLedgerId(path), Optional.of(stat.getCreationTimestamp()));
            }
        });

        store.registerListener(this::handleDataNotification);
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long ledgerId,
                                                                             LedgerMetadata inputMetadata) {
        final LedgerMetadata metadata;
        if (inputMetadata.getMetadataFormatVersion() > LedgerMetadataSerDe.METADATA_FORMAT_VERSION_2) {
            metadata = LedgerMetadataBuilder.from(inputMetadata).withId(ledgerId).build();
        } else {
            metadata = inputMetadata;
        }

        final byte[] data;
        try {
            data = serde.serialize(metadata);
        } catch (IOException ioe) {
            return FutureUtil.failedFuture(new BKException.BKMetadataSerializationException(ioe));
        }

        CompletableFuture<Versioned<LedgerMetadata>> future = store.put(getLedgerPath(ledgerId), data, Optional.of(-1L))
                .thenApply(stat -> new Versioned(metadata, new LongVersion(stat.getVersion())));
        future.exceptionally(ex -> {
            log.error("Failed to create ledger {}: {}", ledgerId, ex.getMessage());
            return null;
        });

        return future;
    }

    @Override
    public CompletableFuture<Void> removeLedgerMetadata(long ledgerId, Version version) {
        Optional<Long> existingVersion = Optional.empty();
        if (Version.NEW == version) {
            log.error("Request to delete ledger {} metadata with version set to the initial one", ledgerId);
            return FutureUtil.failedFuture(new BKException.BKMetadataVersionException());
        } else if (Version.ANY != version) {
            if (!(version instanceof LongVersion)) {
                log.info("Not an instance of ZKVersion: {}", ledgerId);
                return FutureUtil.failedFuture(new BKException.BKMetadataVersionException());
            } else {
                existingVersion = Optional.of(((LongVersion) version).getLongVersion());
            }
        }

        return store.delete(getLedgerPath(ledgerId), existingVersion)
                .thenRun(() -> {
                    // removed listener on ledgerId
                    Set<BookkeeperInternalCallbacks.LedgerMetadataListener> listenerSet = listeners.remove(ledgerId);
                    if (null != listenerSet) {
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "Remove registered ledger metadata listeners on ledger {} after ledger is deleted.",
                                    ledgerId);
                        }
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("No ledger metadata listeners to remove from ledger {} when it's being deleted.",
                                    ledgerId);
                        }
                    }
                });
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        String ledgerPath = getLedgerPath(ledgerId);
        cache.getWithStats(ledgerPath)
                .thenAccept(optRes -> {
                    if (!optRes.isPresent()) {
                        if (log.isDebugEnabled()) {
                            log.debug("No such ledger: {} at path {}", ledgerId, ledgerPath);
                        }
                        promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                    }

                    Stat stat = optRes.get().getStat();
                    LongVersion version = new LongVersion(stat.getVersion());
                    LedgerMetadata metadata = optRes.get().getValue();
                    promise.complete(new Versioned<>(metadata, version));
                }).exceptionally(ex -> {
                    log.error("Could not read metadata for ledger: {}: {}", ledgerId, ex.getMessage());
                    promise.completeExceptionally(new BKException.ZKException(ex.getCause()));
                    return null;
                });
        return promise;
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                                                                            Version currentVersion) {

        if (!(currentVersion instanceof LongVersion)) {
            return FutureUtil.failedFuture(new BKException.BKMetadataVersionException());
        }
        final LongVersion zv = (LongVersion) currentVersion;

        final byte[] data;
        try {
            data = serde.serialize(metadata);
        } catch (IOException ioe) {
            return FutureUtil.failedFuture(new BKException.BKMetadataSerializationException(ioe));
        }

        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();

        store.put(getLedgerPath(ledgerId),
                        data, Optional.of(zv.getLongVersion()))
                .thenAccept(stat -> {
                    promise.complete(new Versioned<>(metadata, new LongVersion(stat.getVersion())));
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof MetadataStoreException.BadVersionException) {
                        promise.completeExceptionally(new BKException.BKMetadataVersionException());
                    } else {
                        log.warn("Conditional update ledger metadata failed: {}", ex.getMessage());
                        promise.completeExceptionally(new BKException.ZKException(ex.getCause()));
                    }
                    return null;
                });
        return promise;
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId,
                                               BookkeeperInternalCallbacks.LedgerMetadataListener listener) {
        if (listener == null) {
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Registered ledger metadata listener {} on ledger {}.", listener, ledgerId);
        }
        Set<BookkeeperInternalCallbacks.LedgerMetadataListener> listenerSet =
                listeners.computeIfAbsent(ledgerId, k -> new HashSet<>());
        synchronized (listenerSet) {
            listenerSet.add(listener);
        }
        new ReadLedgerMetadataTask(ledgerId).run();
    }

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId,
                                                 BookkeeperInternalCallbacks.LedgerMetadataListener listener) {
        Set<BookkeeperInternalCallbacks.LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
        if (listenerSet == null) {
            return;
        }
        synchronized (listenerSet) {
            if (listenerSet.remove(listener)) {
                if (log.isDebugEnabled()) {
                    log.debug("Unregistered ledger metadata listener {} on ledger {}.", listener, ledgerId);
                }
            }
            if (listenerSet.isEmpty()) {
                listeners.remove(ledgerId, listenerSet);
            }
        }
    }

    private static final Pattern ledgerPathRegex = Pattern.compile("/L[0-9]+$");

    private void handleDataNotification(Notification n) {
        if (!n.getPath().startsWith(ledgerRootPath)
                || !ledgerPathRegex.matcher(n.getPath()).matches()) {
            return;
        }

        final long ledgerId;
        try {
            ledgerId = getLedgerId(n.getPath());
        } catch (IOException ioe) {
            log.warn("Received invalid ledger path {} : ", n.getPath(), ioe);
            return;
        }

        switch (n.getType()) {
            case Modified:
                new ReadLedgerMetadataTask(ledgerId).run();
                break;

            case Deleted:
                Set<BookkeeperInternalCallbacks.LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
                if (listenerSet != null) {
                    synchronized (listenerSet) {
                        if (log.isDebugEnabled()) {
                            log.debug("Removed ledger metadata listeners on ledger {} : {}",
                                    ledgerId, listenerSet);
                        }
                        for (BookkeeperInternalCallbacks.LedgerMetadataListener l : listenerSet) {
                            l.onChanged(ledgerId, null);
                        }
                        listeners.remove(ledgerId, listenerSet);
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("No ledger metadata listeners to remove from ledger {} after it's deleted.",
                                ledgerId);
                    }
                }
                break;

            case Created:
            case ChildrenChanged:
            default:
                if (log.isDebugEnabled()) {
                    log.debug("Received event {} on {}.", n.getType(), n.getPath());
                }
                break;
        }
    }

    @Override
    public void asyncProcessLedgers(BookkeeperInternalCallbacks.Processor<Long> processor,
                                    AsyncCallback.VoidCallback finalCb, Object context, int successRc, int failureRc) {
        // Process the old 31-bit id ledgers first.
        legacyLedgerManager.asyncProcessLedgers(processor, (rc, path, ctx) -> {
            if (rc == failureRc) {
                // If it fails, return the failure code to the callback
                finalCb.processResult(rc, path, ctx);
            } else {
                // If it succeeds, proceed with our own recursive ledger processing for the 63-bit id ledgers
                longLedgerManager.asyncProcessLedgers(processor, finalCb, context, successRc, failureRc);
            }
        }, context, successRc, failureRc);
    }

    @Override
    public LedgerRangeIterator getLedgerRanges(long ledgerId) {
        LedgerRangeIterator iteratorA = new LegacyHierarchicalLedgerRangeIterator(store, ledgerRootPath);
        LedgerRangeIterator iteratorB = new LongHierarchicalLedgerRangeIterator(store, ledgerRootPath);
        return new CombinedLedgerRangeIterator(iteratorA, iteratorB);
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException(ie);
        }
    }

    private String getLedgerPath(long ledgerId) {
        return this.ledgerRootPath + StringUtils.getHybridHierarchicalLedgerPath(ledgerId);
    }

    private long getLedgerId(String ledgerPath) throws IOException {
        if (!ledgerPath.startsWith(ledgerRootPath)) {
            throw new IOException("it is not a valid hashed path name : " + ledgerPath);
        }
        String hierarchicalPath = ledgerPath.substring(ledgerRootPath.length() + 1);
        return StringUtils.stringToLongHierarchicalLedgerId(hierarchicalPath);
    }


    /**
     * ReadLedgerMetadataTask class.
     */
    private class ReadLedgerMetadataTask implements Runnable {

        final long ledgerId;

        ReadLedgerMetadataTask(long ledgerId) {
            this.ledgerId = ledgerId;
        }

        @Override
        public void run() {
            if (null != listeners.get(ledgerId)) {
                if (log.isDebugEnabled()) {
                    log.debug("Re-read ledger metadata for {}.", ledgerId);
                }
                readLedgerMetadata(ledgerId)
                        .whenComplete((metadata, exception) -> handleMetadata(metadata, exception));
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Ledger metadata listener for ledger {} is already removed.", ledgerId);
                }
            }
        }

        private void handleMetadata(Versioned<LedgerMetadata> result, Throwable exception) {
            if (exception == null) {
                final Set<BookkeeperInternalCallbacks.LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
                if (null != listenerSet) {
                    if (log.isDebugEnabled()) {
                        log.debug("Ledger metadata is changed for {} : {}.", ledgerId, result);
                    }
                    scheduler.submit(() -> {
                        synchronized (listenerSet) {
                            for (BookkeeperInternalCallbacks.LedgerMetadataListener listener : listenerSet) {
                                listener.onChanged(ledgerId, result);
                            }
                        }
                    });
                }
            } else if (BKException.getExceptionCode(exception)
                    == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                // the ledger is removed, do nothing
                Set<BookkeeperInternalCallbacks.LedgerMetadataListener> listenerSet = listeners.remove(ledgerId);
                if (null != listenerSet) {
                    if (log.isDebugEnabled()) {
                        log.debug("Removed ledger metadata listener set on ledger {} as its ledger is deleted : {}",
                                ledgerId, listenerSet.size());
                    }
                    // notify `null` as indicator that a ledger is deleted
                    // make this behavior consistent with `NodeDeleted` watched event.
                    synchronized (listenerSet) {
                        for (BookkeeperInternalCallbacks.LedgerMetadataListener listener : listenerSet) {
                            listener.onChanged(ledgerId, null);
                        }
                    }
                }
            } else {
                log.warn("Failed on read ledger metadata of ledger {}: {}",
                        ledgerId, BKException.getExceptionCode(exception));
                scheduler.schedule(this, 10, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * whether the child of ledgersRootPath is a top level parent znode for
     * ledgers (in HierarchicalLedgerManager) or znode of a ledger (in
     * FlatLedgerManager).
     */
    public boolean isLedgerParentNode(String path) {
        return path.matches(StringUtils.HIERARCHICAL_LEDGER_PARENT_NODE_REGEX);
    }
}

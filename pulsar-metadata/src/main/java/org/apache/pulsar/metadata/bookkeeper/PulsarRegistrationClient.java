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
package org.apache.pulsar.metadata.bookkeeper;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.COOKIE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.apache.pulsar.common.util.FutureUtil.Sequencer;
import static org.apache.pulsar.common.util.FutureUtil.waitForAll;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

@CustomLog
public class PulsarRegistrationClient implements RegistrationClient {

    private final MetadataStoreExtended store;
    private final String ledgersRootPath;
    // registration paths
    private final String bookieRegistrationPath;
    private final String bookieAllRegistrationPath;
    private final String bookieReadonlyRegistrationPath;
    private final Set<RegistrationListener> writableBookiesWatchers = new CopyOnWriteArraySet<>();
    private final Set<RegistrationListener> readOnlyBookiesWatchers = new CopyOnWriteArraySet<>();
    private final Set<String> pendingBookieRefreshes = ConcurrentHashMap.newKeySet();
    private final MetadataCache<BookieServiceInfo> bookieServiceInfoMetadataCache;
    private final ScheduledExecutorService executor;
    private final Map<BookieId, Versioned<BookieServiceInfo>> writableBookieInfo;
    private final Map<BookieId, Versioned<BookieServiceInfo>> readOnlyBookieInfo;
    private final FutureUtil.Sequencer<Void> sequencer;
    private final Runnable cancelListener;
    private final Runnable cancelSessionListener;
    private SessionEvent lastMetadataSessionEvent;

    public PulsarRegistrationClient(MetadataStore store,
                                    String ledgersRootPath) {
        this.store = (MetadataStoreExtended) store;
        this.ledgersRootPath = ledgersRootPath;
        this.bookieServiceInfoMetadataCache = store.getMetadataCache(BookieServiceInfoSerde.INSTANCE);
        this.sequencer = Sequencer.create();
        this.writableBookieInfo = new ConcurrentHashMap<>();
        this.readOnlyBookieInfo = new ConcurrentHashMap<>();
        // Following Bookie Network Address Changes is an expensive operation
        // as it requires additional ZooKeeper watches
        // we can disable this feature, in case the BK cluster has only
        // static addresses
        this.bookieRegistrationPath = ledgersRootPath + "/" + AVAILABLE_NODE;
        this.bookieAllRegistrationPath = ledgersRootPath + "/" + COOKIE_NODE;
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;
        this.executor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-registration-client"));

        cancelListener = store.registerCancellableListener(this::updatedBookies);
        cancelSessionListener = this.store.registerCancellableSessionListener(this::refreshBookies);
    }

    @Override
    public void close() {
        cancelListener.run();
        cancelSessionListener.run();
        executor.shutdownNow();
    }

    private void refreshBookies(SessionEvent sessionEvent) {
        lastMetadataSessionEvent = sessionEvent;
        if (!SessionEvent.Reconnected.equals(sessionEvent) && !SessionEvent.SessionReestablished.equals(sessionEvent)){
            return;
        }
        // Clean caches.
        store.invalidateCaches(bookieRegistrationPath, bookieAllRegistrationPath, bookieReadonlyRegistrationPath);
        bookieServiceInfoMetadataCache.invalidateAll();
        // Refresh caches of the listeners.
        getReadOnlyBookies().thenAccept(bookies ->
                readOnlyBookiesWatchers.forEach(w -> executor.execute(() -> w.onBookiesChanged(bookies))));
        getWritableBookies().thenAccept(bookies ->
                writableBookiesWatchers.forEach(w -> executor.execute(() -> w.onBookiesChanged(bookies))));
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getWritableBookies() {
        return getBookiesThenFreshCache(bookieRegistrationPath);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getAllBookies() {
        // this method is meant to return all the known bookies, even the bookies
        // that are not in a running state
        return getBookiesThenFreshCache(bookieAllRegistrationPath);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getReadOnlyBookies() {
        return getBookiesThenFreshCache(bookieReadonlyRegistrationPath);
    }

    /**
     * @throws IllegalArgumentException if parameter path is null or empty.
     */
    private CompletableFuture<Versioned<Set<BookieId>>> getBookiesThenFreshCache(String path) {
        if (path == null || path.isEmpty()) {
            return failedFuture(
                    new IllegalArgumentException("parameter [path] can not be null or empty."));
        }
        return store.getChildren(path)
                .thenComposeAsync(children -> {
                    final Set<BookieId> bookieIds = PulsarRegistrationClient.convertToBookieAddresses(children);
                    final List<CompletableFuture<?>> bookieInfoUpdated = new ArrayList<>(bookieIds.size());
                    for (BookieId id : bookieIds) {
                        // update the cache for new bookies
                        if (path.equals(bookieReadonlyRegistrationPath) && readOnlyBookieInfo.get(id) == null) {
                            bookieInfoUpdated.add(readBookieInfoAsReadonlyBookie(id));
                            continue;
                        }
                        if (path.equals(bookieRegistrationPath) && writableBookieInfo.get(id) == null) {
                            bookieInfoUpdated.add(readBookieInfoAsWritableBookie(id));
                            continue;
                        }
                        if (path.equals(bookieAllRegistrationPath)) {
                            if (writableBookieInfo.get(id) != null || readOnlyBookieInfo.get(id) != null) {
                                // jump to next bookie id
                                continue;
                            }
                            // check writable first
                            final CompletableFuture<?> revalidateAllBookiesFuture = readBookieInfoAsWritableBookie(id)
                                    .thenCompose(writableBookieInfo -> writableBookieInfo
                                                .<CompletableFuture<Optional<CacheGetResult<BookieServiceInfo>>>>map(
                                                        bookieServiceInfo -> completedFuture(null))
                                                // check read-only then
                                                .orElseGet(() -> readBookieInfoAsReadonlyBookie(id)));
                            bookieInfoUpdated.add(revalidateAllBookiesFuture);
                        }
                    }
                    if (bookieInfoUpdated.isEmpty()) {
                        return completedFuture(bookieIds);
                    } else {
                        return waitForAll(bookieInfoUpdated)
                                .thenApply(___ -> bookieIds);
                    }
                })
                .thenApply(s -> new Versioned<>(s, Version.NEW));
    }

    @Override
    public CompletableFuture<Void> watchWritableBookies(RegistrationListener registrationListener) {
        writableBookiesWatchers.add(registrationListener);
        // trigger all listeners in writableBookiesWatchers one by one. It aims to keep a sync way
        // to make sure the previous listener has finished when a new listener is register.
        // Though it would bring duplicate trigger listener problem, but since watchWritableBookies
        // is only executed when bookieClient construct, the duplicate problem is acceptable.
        return getWritableBookies()
                .thenAcceptAsync(bookies ->
                        writableBookiesWatchers.forEach(w -> w.onBookiesChanged(bookies)), executor);
    }

    @Override
    public void unwatchWritableBookies(RegistrationListener registrationListener) {
        writableBookiesWatchers.remove(registrationListener);
    }

    @Override
    public CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener registrationListener) {
        readOnlyBookiesWatchers.add(registrationListener);
        // trigger all listeners in readOnlyBookiesWatchers one by one. It aims to keep a sync way
        // to make sure the previous listener has finished when a new listener is register.
        // Though it would bring duplicate trigger listener problem, but since watchReadOnlyBookies
        // is only executed when bookieClient construct, the duplicate problem is acceptable.
        return getReadOnlyBookies()
                .thenAcceptAsync(bookies ->
                        readOnlyBookiesWatchers.forEach(w -> w.onBookiesChanged(bookies)), executor);
    }

    @Override
    public void unwatchReadOnlyBookies(RegistrationListener registrationListener) {
        readOnlyBookiesWatchers.remove(registrationListener);
    }

    private CompletableFuture<Void> refreshBookiesAndNotifyWatchers(String path) {
        boolean readOnly = path.equals(bookieReadonlyRegistrationPath);
        Set<RegistrationListener> watchers = readOnly ? readOnlyBookiesWatchers : writableBookiesWatchers;
        CompletableFuture<Versioned<Set<BookieId>>> bookiesFuture =
                readOnly ? getReadOnlyBookies() : getWritableBookies();
        return bookiesFuture.whenComplete((bookies, ex) -> {
            if (ex != null) {
                log.debug().attr("path", path).exceptionMessage(ex).log("Failed to refresh bookies; retrying");
                scheduleBookieRefresh(path);
            }
        }).thenAccept(bookies -> watchers.forEach(w -> executor.execute(() -> w.onBookiesChanged(bookies))));
    }

    private void scheduleBookieRefresh(String path) {
        if (executor.isShutdown() || !pendingBookieRefreshes.add(path)) {
            return;
        }
        try {
            // Keep at most one retry per registration type. Re-read current membership rather than replaying
            // the failed notification, and release the sequencer between attempts so deletes can still proceed.
            executor.schedule(() -> {
                sequencer.sequential(() -> {
                    pendingBookieRefreshes.remove(path);
                    if (executor.isShutdown()) {
                        return completedFuture(null);
                    }
                    return refreshBookiesAndNotifyWatchers(path);
                });
            }, 1, TimeUnit.SECONDS);
        } catch (RejectedExecutionException ignored) {
            // The client can be closed after the shutdown check.
            pendingBookieRefreshes.remove(path);
        }
    }

    /**
     * This method will receive metadata store notifications and then update the
     * local cache in background sequentially.
     */
    private void updatedBookies(Notification n) {
        // make the notification callback run sequential in background.
        final String path = n.getPath();
        if (!path.startsWith(bookieReadonlyRegistrationPath) && !path.startsWith(bookieRegistrationPath)) {
            // ignore unknown path
            return;
        }
        if (path.equals(bookieReadonlyRegistrationPath) || path.equals(bookieRegistrationPath)) {
            // ignore root path
            return;
        }
        final BookieId bookieId = stripBookieIdFromPath(n.getPath());
        sequencer.sequential(() -> {
            switch (n.getType()) {
                case Created:
                    log.info().attr("bookieId", bookieId).attr("path", n.getPath()).log("Bookie created");
                    if (path.startsWith(bookieReadonlyRegistrationPath)) {
                        return refreshBookiesAndNotifyWatchers(bookieReadonlyRegistrationPath);
                    }
                    return refreshBookiesAndNotifyWatchers(bookieRegistrationPath);
                case Modified:
                    if (bookieId == null) {
                        return completedFuture(null);
                    }
                    log.info().attr("bookieId", bookieId).attr("path", n.getPath()).log("Bookie modified");
                    if (path.startsWith(bookieReadonlyRegistrationPath)) {
                        return readBookieInfoAsReadonlyBookie(bookieId).thenApply(__ -> null);
                    }
                    return readBookieInfoAsWritableBookie(bookieId).thenApply(__ -> null);
                case Deleted:
                    if (bookieId == null) {
                        return completedFuture(null);
                    }
                    log.info().attr("bookieId", bookieId).attr("path", n.getPath()).log("Bookie deleted");
                    if (path.startsWith(bookieReadonlyRegistrationPath)) {
                        readOnlyBookieInfo.remove(bookieId);
                        return refreshBookiesAndNotifyWatchers(bookieReadonlyRegistrationPath);
                    }
                    if (path.startsWith(bookieRegistrationPath)) {
                        writableBookieInfo.remove(bookieId);
                        return refreshBookiesAndNotifyWatchers(bookieRegistrationPath);
                    }
                    return completedFuture(null);
                default:
                    return completedFuture(null);
            }
        });
    }

    private static BookieId stripBookieIdFromPath(String path) {
        if (path == null) {
            return null;
        }
        final int slash = path.lastIndexOf('/');
        if (slash >= 0) {
            try {
                return BookieId.parse(path.substring(slash + 1));
            } catch (IllegalArgumentException e) {
                log.warn().attr("path", path).exceptionMessage(e).log("Cannot decode bookieId");
            }
        }
        return null;
    }


    private static Set<BookieId> convertToBookieAddresses(List<String> children) {
        // Read the bookie addresses into a set for efficient lookup
        HashSet<BookieId> newBookieAddrs = new HashSet<>();
        for (String bookieAddrString : children) {
            if (READONLY.equals(bookieAddrString)) {
                continue;
            }
            BookieId bookieAddr = BookieId.parse(bookieAddrString);
            newBookieAddrs.add(bookieAddr);
        }
        return newBookieAddrs;
    }

    @Override
    public CompletableFuture<Versioned<BookieServiceInfo>> getBookieServiceInfo(BookieId bookieId) {
        // this method cannot perform blocking calls to the MetadataStore
        // or return a CompletableFuture that is completed on the MetadataStore main thread
        // this is because there are a few cases in which some operations on the main thread
        // wait for the result. This is due to the fact that resolving the address of a bookie
        // is needed in many code paths.
        Versioned<BookieServiceInfo> info;
        if ((info = writableBookieInfo.get(bookieId)) == null) {
            info = readOnlyBookieInfo.get(bookieId);
        }
        log.debug().attr("bookieId", bookieId).attr("info", info).log("getBookieServiceInfo");
        if (info != null) {
            return completedFuture(info);
        } else {
            // The bookie can be registered and still missing here, if reading its info failed when it
            // registered. Reload it in background, without waiting, so that a retry can find it.
            // Use the notification sequencer so a late reload cannot refill the cache after a deletion.
            sequencer.sequential(() -> readBookieInfoAsWritableBookie(bookieId)
                    .thenCompose(writable -> writable.isPresent()
                            ? completedFuture(writable)
                            : readBookieInfoAsReadonlyBookie(bookieId))
                    .thenApply(__ -> null))
                    .exceptionally(ex -> {
                        log.debug().attr("bookieId", bookieId).exceptionMessage(ex)
                                .log("Failed to reload the bookie info");
                        return null;
                    });
            return FutureUtils.exception(new BKException.BKBookieHandleNotAvailableException());
        }
    }

    public CompletableFuture<Optional<CacheGetResult<BookieServiceInfo>>> readBookieInfoAsWritableBookie(
            BookieId bookieId) {
        final String asWritable = bookieRegistrationPath + "/" + bookieId;
        return bookieServiceInfoMetadataCache.getWithStats(asWritable)
                .thenApply((Optional<CacheGetResult<BookieServiceInfo>> bkInfoWithStats) -> {
                            if (bkInfoWithStats.isPresent()) {
                                final CacheGetResult<BookieServiceInfo> r = bkInfoWithStats.get();
                                log.info().attr("bookieId", bookieId).attr("info", r.getValue())
                                        .log("Update BookieInfoCache (writable bookie)");
                                writableBookieInfo.put(bookieId,
                                        new Versioned<>(r.getValue(), new LongVersion(r.getStat().getVersion())));
                            }
                            return bkInfoWithStats;
                        }
                );
    }

    final CompletableFuture<Optional<CacheGetResult<BookieServiceInfo>>> readBookieInfoAsReadonlyBookie(
            BookieId bookieId) {
        final String asReadonly = bookieReadonlyRegistrationPath + "/" + bookieId;
        return bookieServiceInfoMetadataCache.getWithStats(asReadonly)
                .thenApply((Optional<CacheGetResult<BookieServiceInfo>> bkInfoWithStats) -> {
                    if (bkInfoWithStats.isPresent()) {
                        final CacheGetResult<BookieServiceInfo> r = bkInfoWithStats.get();
                        log.info().attr("bookieId", bookieId).attr("info", r.getValue())
                                .log("Update BookieInfoCache (readonly bookie)");
                        readOnlyBookieInfo.put(bookieId,
                                new Versioned<>(r.getValue(), new LongVersion(r.getStat().getVersion())));
                    }
                    return bkInfoWithStats;
                });
    }
}

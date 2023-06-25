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
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;

@Slf4j
public class PulsarRegistrationClient implements RegistrationClient {

    private final MetadataStore store;
    private final String ledgersRootPath;
    // registration paths
    private final String bookieRegistrationPath;
    private final String bookieAllRegistrationPath;
    private final String bookieReadonlyRegistrationPath;
    private final Set<RegistrationListener> writableBookiesWatchers = new CopyOnWriteArraySet<>();
    private final Set<RegistrationListener> readOnlyBookiesWatchers = new CopyOnWriteArraySet<>();
    private final MetadataCache<BookieServiceInfo> bookieServiceInfoMetadataCache;
    private final ScheduledExecutorService executor;
    private final Map<BookieMode, Map<BookieId, Versioned<BookieServiceInfo>>> bookieInfoCache =
            new ConcurrentHashMap<>();
    private final FutureUtil.Sequencer<Void> sequencer;


    enum BookieMode {
        READ_ONLY,
        READ_WRITE,
    }

    public PulsarRegistrationClient(MetadataStore store,
                                    String ledgersRootPath) {
        this.store = store;
        this.ledgersRootPath = ledgersRootPath;
        this.bookieServiceInfoMetadataCache = store.getMetadataCache(BookieServiceInfoSerde.INSTANCE);
        this.sequencer = Sequencer.create();

        // Following Bookie Network Address Changes is an expensive operation
        // as it requires additional ZooKeeper watches
        // we can disable this feature, in case the BK cluster has only
        // static addresses
        this.bookieRegistrationPath = ledgersRootPath + "/" + AVAILABLE_NODE;
        this.bookieAllRegistrationPath = ledgersRootPath + "/" + COOKIE_NODE;
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;

        this.executor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-registration-client"));

        store.registerListener(this::updatedBookies);
    }

    @Override
    public void close() {
        executor.shutdownNow();
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
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException(String.format("parameter [path] can not be null or empty.")));
        }
        return store.getChildren(path)
                .thenComposeAsync(children -> {
                    final Set<BookieId> bookieIds = PulsarRegistrationClient.convertToBookieAddresses(children);
                    final List<CompletableFuture<?>> bookieInfoUpdated = new ArrayList<>(bookieIds.size());
                    for (BookieId id : bookieIds) {
                        // update the cache for new bookies
                        if (path.equals(bookieRegistrationPath) &&
                                (bookieInfoCache.computeIfAbsent(BookieMode.READ_WRITE,
                                        __ -> new ConcurrentHashMap<>()).get(id) == null)) {
                            bookieInfoUpdated.add(readBookieServiceInfoAsync(id));
                        }
                        if (path.equals(bookieReadonlyRegistrationPath) &&
                                (bookieInfoCache.computeIfAbsent(BookieMode.READ_ONLY,
                                        __ -> new ConcurrentHashMap<>()).get(id) == null)) {
                            bookieInfoUpdated.add(readBookieInfoAsReadonlyBookie(id));
                        }
                        if (path.equals(bookieAllRegistrationPath)) {
                            // do nothing
                        }
                    }
                    if (bookieInfoUpdated.isEmpty()) {
                        return CompletableFuture.completedFuture(bookieIds);
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
        return getWritableBookies()
                .thenAcceptAsync(registrationListener::onBookiesChanged, executor);
    }

    @Override
    public void unwatchWritableBookies(RegistrationListener registrationListener) {
        writableBookiesWatchers.remove(registrationListener);
    }

    @Override
    public CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener registrationListener) {
        readOnlyBookiesWatchers.add(registrationListener);
        return getReadOnlyBookies()
                .thenAcceptAsync(registrationListener::onBookiesChanged, executor);
    }

    @Override
    public void unwatchReadOnlyBookies(RegistrationListener registrationListener) {
        readOnlyBookiesWatchers.remove(registrationListener);
    }

    /**
     * This method will receive metadata store notifications and then update the
     * local cache in background sequentially.
     */
    private void updatedBookies(Notification n) {
        final BookieMode mode;
        if (n.getPath().startsWith(bookieReadonlyRegistrationPath)) {
            mode = BookieMode.READ_ONLY;
        } else if (n.getPath().startsWith(bookieRegistrationPath)) {
            mode = BookieMode.READ_WRITE;
        } else {
            // unknown notification, should not happen.
            return;
        }
        // make the notification callback run sequential in background.
        sequencer.sequential(() -> {
            final BookieId bookieId = stripBookieIdFromPath(n.getPath());
            switch (n.getType()) {
                case Created:
                    log.info("Bookie {} created. mode: {}", bookieId, mode);
                    return handleBookieCreated(mode);
                case Modified:
                    if (bookieId != null) {
                        log.info("Bookie {} modified. mode: {}", bookieId, mode);
                        return handleBookieModified(mode, bookieId);
                    }
                    break;
                case Deleted:
                    if (bookieId != null) {
                        log.info("Bookie {} disappeared. mode: {}", bookieId, mode);
                        bookieInfoCache.computeIfAbsent(mode, __ -> new ConcurrentHashMap<>())
                                .remove(bookieId);
                    }
                    break;
                default:
            }
            // fallback to completed future
            return CompletableFuture.completedFuture(null);
        });
    }

    private CompletableFuture<Void> handleBookieModified(BookieMode mode, BookieId id) {
        if (mode == BookieMode.READ_ONLY) {
            return readBookieInfoAsReadonlyBookie(id);
        }
        return readBookieServiceInfoAsync(id);
    }

    private CompletableFuture<Void> handleBookieCreated(BookieMode mode) {
        if (mode == BookieMode.READ_ONLY) {
            return getReadOnlyBookies().thenAccept(bookies ->
                    readOnlyBookiesWatchers.forEach(w -> executor.execute(() -> w.onBookiesChanged(bookies))));
        }
        return getWritableBookies().thenAccept(bookies ->
                writableBookiesWatchers.forEach(w -> executor.execute(() -> w.onBookiesChanged(bookies))));
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
                log.warn("Cannot decode bookieId from {}", path, e);
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
        Versioned<BookieServiceInfo> info = null;
        for (BookieMode bookieMode : bookieInfoCache.keySet()) {
            if ((info = bookieInfoCache.computeIfAbsent(bookieMode,
                    __ -> new ConcurrentHashMap<>()).get(bookieId)) != null) {
                break;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("getBookieServiceInfo {} -> {}", bookieId, info);
        }
        if (info != null) {
            return CompletableFuture.completedFuture(info);
        } else {
            return FutureUtils.exception(new BKException.BKBookieHandleNotAvailableException());
        }
    }

    public CompletableFuture<Void> readBookieServiceInfoAsync(BookieId bookieId) {
        String asWritable = bookieRegistrationPath + "/" + bookieId;
        return bookieServiceInfoMetadataCache.get(asWritable)
                .thenAccept((Optional<BookieServiceInfo> getResult) -> {
                            if (getResult.isPresent()) {
                                Versioned<BookieServiceInfo> res =
                                        new Versioned<>(getResult.get(), new LongVersion(-1));
                                log.info("Update BookieInfoCache (writable bookie) {} -> {}",
                                        bookieId, getResult.get());
                                bookieInfoCache.computeIfAbsent(BookieMode.READ_WRITE,
                                        __ -> new ConcurrentHashMap<>()).put(bookieId, res);
                            }
                        }
                );
    }

    final CompletableFuture<Void> readBookieInfoAsReadonlyBookie(BookieId bookieId) {
        String asReadonly = bookieReadonlyRegistrationPath + "/" + bookieId;
        return bookieServiceInfoMetadataCache.get(asReadonly)
                .thenAccept((Optional<BookieServiceInfo> getResultAsReadOnly) -> {
                    if (getResultAsReadOnly.isPresent()) {
                        Versioned<BookieServiceInfo> res =
                                new Versioned<>(getResultAsReadOnly.get(), new LongVersion(-1));
                        log.info("Update BookieInfoCache (readonly bookie) {} -> {}", bookieId,
                                getResultAsReadOnly.get());
                        bookieInfoCache.computeIfAbsent(BookieMode.READ_ONLY, __ -> new ConcurrentHashMap<>())
                                .put(bookieId, res);
                    }
                });
    }
}

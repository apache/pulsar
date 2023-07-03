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
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.apache.pulsar.metadata.api.NotificationType;

@Slf4j
public class PulsarRegistrationClient implements RegistrationClient {

    private final MetadataStore store;
    private final String ledgersRootPath;
    // registration paths
    private final String bookieRegistrationPath;
    private final String bookieAllRegistrationPath;
    private final String bookieReadonlyRegistrationPath;

    private final ConcurrentHashMap<BookieId, Versioned<BookieServiceInfo>> bookieServiceInfoCache =
                                                                                    new ConcurrentHashMap();
    private final Set<RegistrationListener> writableBookiesWatchers = new CopyOnWriteArraySet<>();
    private final Set<RegistrationListener> readOnlyBookiesWatchers = new CopyOnWriteArraySet<>();
    private final MetadataCache<BookieServiceInfo> bookieServiceInfoMetadataCache;
    private final ScheduledExecutorService executor;

    public PulsarRegistrationClient(MetadataStore store,
                                    String ledgersRootPath) {
        this.store = store;
        this.ledgersRootPath = ledgersRootPath;
        this.bookieServiceInfoMetadataCache = store.getMetadataCache(BookieServiceInfoSerde.INSTANCE);

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
        return getChildren(bookieRegistrationPath);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getAllBookies() {
        // this method is meant to return all the known bookies, even the bookies
        // that are not in a running state
        return getChildren(bookieAllRegistrationPath);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getReadOnlyBookies() {
        return getChildren(bookieReadonlyRegistrationPath);
    }

    private CompletableFuture<Versioned<Set<BookieId>>> getChildren(String path) {
        return store.getChildren(path)
                .thenComposeAsync(children -> {
                    Set<BookieId> bookieIds = PulsarRegistrationClient.convertToBookieAddresses(children);
                    List<CompletableFuture<?>> bookieInfoUpdated =
                            new ArrayList<>(bookieIds.size());
                    for (BookieId id : bookieIds) {
                        // update the cache for new bookies
                        if (!bookieServiceInfoCache.containsKey(id)) {
                            bookieInfoUpdated.add(readBookieServiceInfoAsync(id));
                        }
                    }
                    if (bookieInfoUpdated.isEmpty()) {
                        return CompletableFuture.completedFuture(bookieIds);
                    } else {
                        return FutureUtil
                                .waitForAll(bookieInfoUpdated)
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

    private void handleDeletedBookieNode(Notification n) {
        if (n.getType() == NotificationType.Deleted) {
            BookieId bookieId = stripBookieIdFromPath(n.getPath());
            if (bookieId != null) {
                log.info("Bookie {} disappeared", bookieId);
                bookieServiceInfoCache.remove(bookieId);
            }
        }
    }

    private void handleUpdatedBookieNode(Notification n) {
        BookieId bookieId = stripBookieIdFromPath(n.getPath());
        if (bookieId != null) {
            log.info("Bookie {} info updated", bookieId);
            readBookieServiceInfoAsync(bookieId);
        }
    }

    private void updatedBookies(Notification n) {
        if (n.getType() == NotificationType.Created || n.getType() == NotificationType.Deleted) {
            if (n.getPath().startsWith(bookieReadonlyRegistrationPath)) {
                getReadOnlyBookies().thenAccept(bookies -> {
                    readOnlyBookiesWatchers.forEach(w -> executor.execute(() -> w.onBookiesChanged(bookies)));
                });
                handleDeletedBookieNode(n);
            } else if (n.getPath().startsWith(bookieRegistrationPath)) {
                  getWritableBookies().thenAccept(bookies ->
                        writableBookiesWatchers.forEach(w -> executor.execute(() -> w.onBookiesChanged(bookies))));
                handleDeletedBookieNode(n);
            }
        } else if (n.getType() == NotificationType.Modified) {
            if (n.getPath().startsWith(bookieReadonlyRegistrationPath)
                || n.getPath().startsWith(bookieRegistrationPath)) {
                handleUpdatedBookieNode(n);
            }
        }
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
        Versioned<BookieServiceInfo> resultFromCache = bookieServiceInfoCache.get(bookieId);
        if (log.isDebugEnabled()) {
            log.debug("getBookieServiceInfo {} -> {}", bookieId, resultFromCache);
        }
        if (resultFromCache != null) {
            return CompletableFuture.completedFuture(resultFromCache);
        } else {
            return FutureUtils.exception(new BKException.BKBookieHandleNotAvailableException());
        }
    }

    public CompletableFuture<Void> readBookieServiceInfoAsync(BookieId bookieId) {
        String asWritable = bookieRegistrationPath + "/" + bookieId;
        return bookieServiceInfoMetadataCache.get(asWritable)
                .thenCompose((Optional<BookieServiceInfo> getResult) -> {
                    if (getResult.isPresent()) {
                        Versioned<BookieServiceInfo> res =
                                new Versioned<>(getResult.get(), new LongVersion(-1));
                        log.info("Update BookieInfoCache (writable bookie) {} -> {}", bookieId, getResult.get());
                        bookieServiceInfoCache.put(bookieId, res);
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return readBookieInfoAsReadonlyBookie(bookieId);
                    }
                }
        );
    }

    final CompletableFuture<Void> readBookieInfoAsReadonlyBookie(BookieId bookieId) {
        String asReadonly = bookieReadonlyRegistrationPath + "/" + bookieId;
        return bookieServiceInfoMetadataCache.get(asReadonly)
                .thenApply((Optional<BookieServiceInfo> getResultAsReadOnly) -> {
                    if (getResultAsReadOnly.isPresent()) {
                        Versioned<BookieServiceInfo> res =
                                new Versioned<>(getResultAsReadOnly.get(), new LongVersion(-1));
                        log.info("Update BookieInfoCache (readonly bookie) {} -> {}", bookieId,
                                getResultAsReadOnly.get());
                        bookieServiceInfoCache.put(bookieId, res);
                    }
                    return null;
                });
    }
}

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

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.COOKIE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;

public class PulsarRegistrationClient implements RegistrationClient {

    private final MetadataStore store;
    private final String ledgersRootPath;
    // registration paths
    private final String bookieRegistrationPath;
    private final String bookieAllRegistrationPath;
    private final String bookieReadonlyRegistrationPath;

    private final Map<RegistrationListener, Boolean> writableBookiesWatchers = new ConcurrentHashMap<>();
    private final Map<RegistrationListener, Boolean> readOnlyBookiesWatchers = new ConcurrentHashMap<>();

    public PulsarRegistrationClient(MetadataStore store,
                                    String ledgersRootPath) {
        this.store = store;
        this.ledgersRootPath = ledgersRootPath;

        // Following Bookie Network Address Changes is an expensive operation
        // as it requires additional ZooKeeper watches
        // we can disable this feature, in case the BK cluster has only
        // static addresses
        this.bookieRegistrationPath = ledgersRootPath + "/" + AVAILABLE_NODE;
        this.bookieAllRegistrationPath = ledgersRootPath + "/" + COOKIE_NODE;
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;

        store.registerListener(this::updatedBookies);
    }

    @Override
    public void close() {
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getWritableBookies() {
        return getChildren(bookieRegistrationPath);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getAllBookies() {
        CompletableFuture<Versioned<Set<BookieId>>> wb = getWritableBookies();
        CompletableFuture<Versioned<Set<BookieId>>> rb = getReadOnlyBookies();
        return wb.thenCombine(rb, (rw, ro) -> {
            Set<BookieId> res = new HashSet<>();
            res.addAll(rw.getValue());
            res.addAll(ro.getValue());
            return new Versioned<>(res, Version.NEW);
        });
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getReadOnlyBookies() {
        return getChildren(bookieReadonlyRegistrationPath);
    }

    private CompletableFuture<Versioned<Set<BookieId>>> getChildren(String path) {
        return store.getChildren(path)
                .thenApply(PulsarRegistrationClient::convertToBookieAddresses)
                .thenApply(s -> new Versioned<>(s, Version.NEW));
    }

    @Override
    public CompletableFuture<Void> watchWritableBookies(RegistrationListener registrationListener) {
        writableBookiesWatchers.put(registrationListener, Boolean.TRUE);
        return getWritableBookies()
                .thenAccept(registrationListener::onBookiesChanged);
    }

    @Override
    public void unwatchWritableBookies(RegistrationListener registrationListener) {
        writableBookiesWatchers.remove(registrationListener);
    }

    @Override
    public CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener registrationListener) {
        readOnlyBookiesWatchers.put(registrationListener, Boolean.TRUE);
        return getReadOnlyBookies()
                .thenAccept(registrationListener::onBookiesChanged);
    }

    @Override
    public void unwatchReadOnlyBookies(RegistrationListener registrationListener) {
        readOnlyBookiesWatchers.remove(registrationListener);
    }

    private void updatedBookies(Notification n) {
        if (n.getType() == NotificationType.Created || n.getType() == NotificationType.Deleted) {
            if (n.getPath().startsWith(bookieReadonlyRegistrationPath)) {
                getReadOnlyBookies().thenAccept(bookies ->
                        readOnlyBookiesWatchers.keySet()
                                .forEach(w -> w.onBookiesChanged(bookies)));
            } else if (n.getPath().startsWith(bookieRegistrationPath)) {
                getWritableBookies().thenAccept(bookies ->
                        writableBookiesWatchers.keySet()
                                .forEach(w -> w.onBookiesChanged(bookies)));
            }
        }
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
}

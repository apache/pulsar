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

import io.opentelemetry.api.OpenTelemetry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.OptionsHelper;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;

/**
 * Minimal in-memory metadata store that models the metadata store semantics the session-loss
 * driven bookie re-registration depends on:
 * <ul>
 * <li>the record identity is a store-level UUID, independent of the session: a record
 * created by an earlier session of the same store instance still reports
 * {@code createdBySelf == true} (cross-session adopt);</li>
 * <li>ephemeral puts lazily create a session and bind the record to it; a session loss
 * purges (or, in a server-side grace window, keeps) the records of that session;</li>
 * <li>expected-version checked puts and deletes with BadVersion/NotFound semantics;</li>
 * <li>the first session established after an expiry is reported as
 * {@link SessionEvent#SessionReestablished}, mirroring the session watcher mapping.</li>
 * </ul>
 */
class FakeOxiaSessionMetadataStore extends AbstractMetadataStore {

    /** Session id used for foreign records: never matches a session of this store. */
    private static final long FOREIGN_SESSION_ID = Long.MIN_VALUE;

    @Data
    static class Record {
        final long version;
        final byte[] data;
        final long createdTimestamp;
        final long modifiedTimestamp;
        final boolean ephemeral;
        final String creatorIdentity;
        final long ownerSessionId;
    }

    private final String identity;
    private final NavigableMap<String, Record> records = new TreeMap<>();

    // guarded by this
    private long nextSessionId = 1;
    private Long currentSessionId;
    private boolean seenExpired;

    private final ScheduledExecutorService sessionEventDispatcher = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "fake-oxia-session-events"));

    private volatile boolean unavailable = false;
    /**
     * When set, models the zk ownership semantics: an update (setData) neither transfers
     * the ephemeral ownership nor changes the node type, and {@code createdBySelf} is
     * session-scoped ({@code ephemeralOwner == current session}) instead of identity-scoped.
     */
    private volatile boolean zkOwnershipSemantics = false;
    private final AtomicInteger deletesToFailWithBadVersion = new AtomicInteger();

    FakeOxiaSessionMetadataStore(String identity) {
        super("fake-oxia-session-store", OpenTelemetry.noop(), null, 1);
        this.identity = identity;
    }

    // ---------------------------------------------------------------- store operations

    @Override
    protected synchronized CompletableFuture<Optional<GetResult>> storeGet(String path, Set<Option> opts) {
        if (unavailable) {
            return failed(new MetadataStoreException("injected store unavailability"));
        }
        Record record = records.get(path);
        if (record == null) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return CompletableFuture.completedFuture(Optional.of(new GetResult(record.data, stat(path, record))));
    }

    @Override
    protected synchronized CompletableFuture<Boolean> existsFromStore(String path, Set<Option> opts) {
        if (unavailable) {
            return failed(new MetadataStoreException("injected store unavailability"));
        }
        return CompletableFuture.completedFuture(records.containsKey(path));
    }

    @Override
    public synchronized CompletableFuture<List<String>> getChildrenFromStore(String path, Set<Option> opts) {
        List<String> children = new ArrayList<>();
        String prefix = path.equals("/") ? "/" : path + "/";
        for (String key : records.tailMap(prefix).keySet()) {
            if (!key.startsWith(prefix)) {
                break;
            }
            String relative = key.substring(prefix.length());
            if (relative.isEmpty()) {
                continue;
            }
            children.add(relative.split("/")[0]);
        }
        return CompletableFuture.completedFuture(children);
    }

    @Override
    protected synchronized CompletableFuture<Stat> storePut(String path, byte[] data,
            Optional<Long> optExpectedVersion, Set<Option> opts) {
        if (unavailable) {
            return failed(new MetadataStoreException("injected store unavailability"));
        }
        Record existing = records.get(path);
        if (optExpectedVersion.isPresent()) {
            long expected = optExpectedVersion.get();
            long current = existing != null ? existing.version : -1L;
            if (expected != current) {
                return failed(new MetadataStoreException.BadVersionException(
                        "expected version " + expected + " but was " + current));
            }
        }

        long now = System.currentTimeMillis();
        long newVersion = existing != null ? existing.version + 1 : 0;
        boolean ephemeral = OptionsHelper.isEphemeral(opts);
        long ownerSessionId = -1L;
        if (ephemeral) {
            // Sessions are created lazily by successful ephemeral puts, like in the oxia client.
            ownerSessionId = ensureSession();
        }
        Record record;
        if (zkOwnershipSemantics && existing != null) {
            // zk semantics: an update (setData) neither transfers the ephemeral ownership
            // nor changes the node type.
            record = new Record(newVersion, data.clone(), existing.createdTimestamp, now,
                    existing.ephemeral, existing.creatorIdentity, existing.ownerSessionId);
        } else {
            record = new Record(newVersion, data.clone(),
                    existing != null ? existing.createdTimestamp : now, now,
                    ephemeral, identity, ownerSessionId);
        }
        records.put(path, record);
        return CompletableFuture.completedFuture(stat(path, record));
    }

    @Override
    protected synchronized CompletableFuture<Void> storeDelete(String path, Optional<Long> optExpectedVersion,
            Set<Option> opts) {
        if (unavailable) {
            return failed(new MetadataStoreException("injected store unavailability"));
        }
        if (deletesToFailWithBadVersion.getAndUpdate(d -> d > 0 ? d - 1 : 0) > 0) {
            return failed(new MetadataStoreException.BadVersionException("injected delete race"));
        }
        Record record = records.get(path);
        if (record == null) {
            return failed(new MetadataStoreException.NotFoundException(path));
        }
        if (optExpectedVersion.isPresent() && optExpectedVersion.get() != record.version) {
            return failed(new MetadataStoreException.BadVersionException(
                    "expected version " + optExpectedVersion.get() + " but was " + record.version));
        }
        records.remove(path);
        receivedNotification(new Notification(NotificationType.Deleted, path));
        notifyParentChildrenChanged(path);
        return CompletableFuture.completedFuture(null);
    }

    private Stat stat(String path, Record record) {
        boolean createdBySelf = zkOwnershipSemantics
                ? record.ephemeral && currentSessionId != null && record.ownerSessionId == currentSessionId
                : Objects.equals(record.creatorIdentity, identity);
        return new Stat(path, record.version, record.createdTimestamp, record.modifiedTimestamp,
                record.ephemeral, createdBySelf);
    }

    private long ensureSession() {
        if (currentSessionId == null) {
            currentSessionId = nextSessionId++;
            if (seenExpired) {
                seenExpired = false;
                // First session established after an expiry: report it like a session
                // watcher does.
                fireSessionEvent(SessionEvent.SessionReestablished);
            }
        }
        return currentSessionId;
    }

    private void fireSessionEvent(SessionEvent event) {
        try {
            sessionEventDispatcher.execute(() -> receivedSessionEvent(event));
        } catch (RejectedExecutionException ignore) {
            // store closed
        }
    }

    private static <T> CompletableFuture<T> failed(MetadataStoreException exception) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(exception);
        return future;
    }

    // ---------------------------------------------------------------- test knobs

    /**
     * Kills the current session: fires {@link SessionEvent#SessionLost} and, when
     * {@code purgeEphemeralRecords} is set, removes the ephemeral records owned by the dead
     * session, like the server-side session expiry purge. When the purge is skipped, the
     * records stay in the server-side grace window after the session death.
     */
    synchronized void expireSession(boolean purgeEphemeralRecords) {
        if (currentSessionId == null) {
            return;
        }
        long deadSessionId = currentSessionId;
        seenExpired = true;
        currentSessionId = null;
        if (purgeEphemeralRecords) {
            List<String> purged = new ArrayList<>();
            for (Map.Entry<String, Record> entry : records.entrySet()) {
                if (entry.getValue().ephemeral && entry.getValue().ownerSessionId == deadSessionId) {
                    purged.add(entry.getKey());
                }
            }
            purged.forEach(records::remove);
        }
        fireSessionEvent(SessionEvent.SessionLost);
    }

    /**
     * Fires an extra {@link SessionEvent#SessionReestablished}, like the session watcher of a
     * metadata store reporting that a fresh session was established after a loss.
     */
    void fireSessionReestablished() {
        fireSessionEvent(SessionEvent.SessionReestablished);
    }

    /** Overwrites a path with an ephemeral record owned by a foreign identity. */
    synchronized void putForeignRecord(String path, String foreignIdentity, byte[] value) {
        putForeignRecord(path, foreignIdentity, value, true);
    }

    /** Overwrites a path with a record of the given ephemerality owned by a foreign identity. */
    synchronized void putForeignRecord(String path, String foreignIdentity, byte[] value, boolean ephemeral) {
        Record existing = records.get(path);
        long now = System.currentTimeMillis();
        long version = existing != null ? existing.version + 1 : 0;
        records.put(path, new Record(version, value.clone(),
                existing != null ? existing.createdTimestamp : now, now,
                ephemeral, foreignIdentity, FOREIGN_SESSION_ID));
    }

    synchronized Optional<Record> getRecord(String path) {
        Record record = records.get(path);
        return record != null ? Optional.of(record) : Optional.empty();
    }

    synchronized long getRecordSession(String path) {
        Record record = records.get(path);
        return record != null ? record.ownerSessionId : Long.MIN_VALUE;
    }

    synchronized Long getCurrentSessionId() {
        return currentSessionId;
    }

    void setUnavailable(boolean unavailable) {
        this.unavailable = unavailable;
    }

    /** Switches the store to the zk ownership semantics (see {@link #zkOwnershipSemantics}). */
    void setZkOwnershipSemantics(boolean zkOwnershipSemantics) {
        this.zkOwnershipSemantics = zkOwnershipSemantics;
    }

    /** Makes the next n deletes fail with a BadVersion, modeling a lost delete/re-put race. */
    void failNextDeletesWithBadVersion(int n) {
        deletesToFailWithBadVersion.addAndGet(n);
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            sessionEventDispatcher.shutdownNow();
            super.close();
        }
    }
}

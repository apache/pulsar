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
package org.apache.pulsar.bookie.rackawareness;

import static org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * Owns the metadata cache and publishes the last successfully read bookie rack configuration.
 */
@CustomLog
public final class BookieRackConfigurationSnapshot implements AutoCloseable {
    public static final String RACK_CONFIGURATION_SNAPSHOT = "bookieRackConfigurationSnapshot";

    private static final long INITIAL_RETRY_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long MAX_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final MetadataStore store;
    private final MetadataCache<BookiesRackConfiguration> cache;
    private final long readTimeoutMillis;
    // DNS and BookKeeper topology locks can block independently for each subscriber. An idle subscriber
    // needs no thread, and each busy subscriber has at most one background task.
    private final ExecutorService applicationExecutor =
            Executors.newCachedThreadPool(new DefaultThreadFactory("bookie-rack-application", true));
    private final CompletableFuture<Void> firstLoad = new CompletableFuture<>();

    private final CompletableFuture<Void> firstRead = new CompletableFuture<>();
    // Guarded by this. Each subscriber orders its callbacks independently.
    private final List<Subscription> subscribers = new ArrayList<>();
    // Null means no successful read. An authoritative missing node is stored as an empty map.
    private volatile VersionedConfiguration latest;
    private volatile boolean closed;
    private volatile ScheduledThreadPoolExecutor executor;

    // Guarded by this.
    private Runnable cancelMetadataListener;
    private Runnable cancelSessionListener;
    private ScheduledFuture<?> scheduledRead;
    private ReadAttempt attempt;
    private long changeGeneration;
    private long retryDelayNanos = INITIAL_RETRY_DELAY_NANOS;
    private long retryAfterNanos;
    private boolean invalidateBeforeRetry;
    private boolean reapplyAfterNotification;
    private boolean startingRead;

    /**
     * Creates a snapshot on the supplied store. Call {@link #start()} to maintain it independently of consumers.
     */
    public BookieRackConfigurationSnapshot(MetadataStore store, long readTimeoutMillis) {
        this.store = store;
        this.cache = store.getMetadataCache(BookiesRackConfiguration.class);
        this.readTimeoutMillis = readTimeoutMillis;
    }

    /**
     * Begins loading and watching the configuration. Failures retry with backoff until a read succeeds.
     */
    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("Bookie rack configuration is closed");
        }
        if (executor != null) {
            return;
        }
        executor = new ScheduledThreadPoolExecutor(1, new DefaultThreadFactory("bookie-rack-configuration", true));
        executor.setRemoveOnCancelPolicy(true);
        cancelMetadataListener = store.registerCancellableListener(this::handleNotification);
        if (store instanceof MetadataStoreExtended storeExtended) {
            cancelSessionListener = storeExtended.registerCancellableSessionListener(event -> {
                if (event.isConnected()) {
                    requestRefresh(true, false);
                }
            });
        }
        scheduleRead();
    }

    private void handleNotification(Notification notification) {
        var type = notification.getType();
        if (BOOKIE_INFO_ROOT_PATH.equals(notification.getPath()) && type != NotificationType.ChildrenChanged) {
            // The store has already invalidated the cache or replaced the old entry with the new, pending read.
            requestRefresh(false, type == NotificationType.Created || type == NotificationType.Modified);
        }
    }

    private synchronized void requestRefresh(boolean invalidate, boolean reapplyOnUnchanged) {
        changeGeneration++;
        invalidateBeforeRetry |= invalidate;
        reapplyAfterNotification |= reapplyOnUnchanged;
        scheduleRead();
    }

    // Called under this monitor.
    private void scheduleRead() {
        if (!closed && executor != null && scheduledRead == null && (attempt == null || attempt.result.isDone())) {
            long delay = retryAfterNanos == 0 ? 0 : Math.max(0, retryAfterNanos - System.nanoTime());
            scheduledRead = executor.schedule(this::refresh, delay, TimeUnit.NANOSECONDS);
        }
    }

    VersionedConfiguration latest() {
        return latest;
    }

    CompletableFuture<Void> initialLoad() {
        return firstLoad.copy();
    }

    /** First completed read at startup, including its failure. A superseded success waits for publication. */
    public CompletableFuture<Void> initialRead() {
        return firstRead.copy();
    }

    /** Includes a successful read of an absent configuration. */
    public boolean hasLoaded() {
        return latest != null;
    }

    Subscription subscribe(Consumer<VersionedConfiguration> listener) {
        var subscription = new Subscription(listener);
        try {
            subscription.initialize();
        } catch (RuntimeException | Error e) {
            subscription.run();
            throw e;
        }
        return subscription;
    }

    private synchronized void executeApplication(Runnable task) {
        if (!closed) {
            applicationExecutor.execute(task);
        }
    }

    private synchronized ScheduledFuture<?> schedule(Runnable task, long delayNanos) {
        return closed || executor == null ? null : executor.schedule(task, delayNanos, TimeUnit.NANOSECONDS);
    }

    MetadataCache<BookiesRackConfiguration> cache() {
        return cache;
    }

    CompletableFuture<Void> refresh() {
        ReadAttempt read;
        boolean invalidate;
        ScheduledThreadPoolExecutor backgroundExecutor;
        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("Bookie rack configuration is closed"));
            }
            backgroundExecutor = executor;
            scheduledRead = null;
            if (attempt != null && (!attempt.result.isDone() || startingRead)) {
                return attempt.result;
            }
            read = new ReadAttempt(changeGeneration);
            attempt = read;
            startingRead = true;
            invalidate = invalidateBeforeRetry;
            invalidateBeforeRetry = false;
        }
        // A late raw completion can only complete its retired future and cannot publish a snapshot.
        var bounded = read.bounded.orTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS);
        if (backgroundExecutor != null) {
            bounded.whenCompleteAsync((configuration, error) -> finishAttempt(read, configuration, error),
                    backgroundExecutor);
        } else {
            bounded.whenComplete((configuration, error) -> finishAttempt(read, configuration, error));
        }
        FutureUtil.supplySafely(() -> {
            try {
                if (invalidate) {
                    cache.invalidate(BOOKIE_INFO_ROOT_PATH);
                }
                return cache.get(BOOKIE_INFO_ROOT_PATH);
            } finally {
                finishStartingRead();
            }
        }).thenApply(value -> value.map(BookieRackConfigurationSnapshot::immutableCopy).orElseGet(Map::of))
                .whenComplete((configuration, error) -> {
                    if (error == null) {
                        bounded.complete(configuration);
                    } else {
                        // Preserve the distinction between a store failure and our own read deadline.
                        bounded.completeExceptionally(new CompletionException(error));
                    }
                });
        return read.result;
    }

    private void finishStartingRead() {
        boolean closeCache;
        synchronized (this) {
            startingRead = false;
            closeCache = closed;
        }
        if (closeCache) {
            cache.close();
        }
    }

    private void finishAttempt(ReadAttempt read, Map<String, Map<String, BookieInfo>> configuration, Throwable error) {
        VersionedConfiguration published = null;
        boolean reapply = false;
        List<Subscription> listeners = List.of();
        synchronized (this) {
            if (closed || attempt != read) {
                return;
            }
            if (error == null && changeGeneration == read.generation) {
                if (latest == null || !latest.configuration().equals(configuration)) {
                    published = new VersionedConfiguration(latest == null ? 1 : latest.version() + 1, configuration);
                    latest = published;
                } else {
                    // A rewrite or recreation can change a Bookie's address without changing its rack or group.
                    reapply = reapplyAfterNotification;
                }
                if (published != null || reapply) {
                    listeners = List.copyOf(subscribers);
                }
                // Keep the request pending through failed or superseded reads.
                reapplyAfterNotification = false;
            }
            if (error == null) {
                retryAfterNanos = 0;
                retryDelayNanos = INITIAL_RETRY_DELAY_NANOS;
            } else {
                retryAfterNanos = System.nanoTime() + retryDelayNanos;
                retryDelayNanos = Math.min(MAX_RETRY_DELAY_NANOS, 2 * retryDelayNanos);
                // Retire the cache entry so a stuck raw read cannot pin the next attempt.
                invalidateBeforeRetry |= error instanceof TimeoutException;
            }
        }
        if (published != null && published.version() == 1) {
            firstRead.complete(null);
            firstLoad.complete(null);
        }
        for (var listener : listeners) {
            if (closed) {
                break;
            }
            listener.requestApplication(reapply);
        }
        if (error == null) {
            read.result.complete(null);
        } else {
            Throwable cause = error instanceof TimeoutException
                    ? new TimeoutException("Timed out after " + readTimeoutMillis
                            + " ms reading bookie isolation metadata at " + BOOKIE_INFO_ROOT_PATH)
                    : FutureUtil.unwrapCompletionException(error);
            firstRead.completeExceptionally(cause);
            read.result.completeExceptionally(cause);
            log.warn().onceEvery(Duration.ofMinutes(1)).attr("path", BOOKIE_INFO_ROOT_PATH)
                    .exceptionMessage(cause).log("Failed to refresh the bookie rack configuration");
        }
        synchronized (this) {
            if (attempt == read && (error != null || changeGeneration != read.generation)) {
                scheduleRead();
            }
        }
    }

    private static Map<String, Map<String, BookieInfo>> immutableCopy(BookiesRackConfiguration configuration) {
        Map<String, Map<String, BookieInfo>> groups = new LinkedHashMap<>();
        configuration.forEach((group, bookies) -> {
            Map<String, BookieInfo> copy = new LinkedHashMap<>();
            bookies.forEach((bookie, info) -> copy.put(bookie, new RackInfo(info.getRack(), info.getHostname())));
            groups.put(group, Collections.unmodifiableMap(copy));
        });
        return Collections.unmodifiableMap(groups);
    }

    @Override
    public void close() {
        ReadAttempt inFlight;
        boolean closeCache;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            subscribers.clear();
            inFlight = attempt;
            // A reserved call to get() may still be entering the cache. Its initiator will close it.
            closeCache = !startingRead;
            if (executor != null) {
                executor.shutdownNow();
            }
            applicationExecutor.shutdownNow();
        }
        var error = new IllegalStateException("Bookie rack configuration is closed");
        firstRead.completeExceptionally(error);
        firstLoad.completeExceptionally(error);
        if (inFlight != null) {
            inFlight.result.completeExceptionally(error);
            inFlight.bounded.completeExceptionally(error);
        }
        try {
            if (cancelMetadataListener != null) {
                cancelMetadataListener.run();
            }
        } finally {
            try {
                if (cancelSessionListener != null) {
                    cancelSessionListener.run();
                }
            } finally {
                if (closeCache) {
                    cache.close();
                }
            }
        }
    }

    record VersionedConfiguration(long version, Map<String, Map<String, BookieInfo>> configuration) {
    }

    private record RackInfo(String rack, String hostname) implements BookieInfo {
        @Override
        public String getRack() {
            return rack;
        }

        @Override
        public String getHostname() {
            return hostname;
        }
    }

    final class Subscription implements Runnable {
        private final Consumer<VersionedConfiguration> listener;
        private final AtomicBoolean firstBookieNotification = new AtomicBoolean(true);
        private final Object workLock = new Object();
        private volatile boolean cancelled;
        // Guarded by workLock. Never hold it while invoking the listener.
        private boolean applicationRequested;
        private boolean applicationScheduled;
        private boolean reapplyRequested;
        private ScheduledFuture<?> retry;
        private long retryGeneration;
        private long retryDelay = INITIAL_RETRY_DELAY_NANOS;
        // Guarded by this subscription's monitor, which serializes the listener.
        private long appliedVersion;
        private boolean reapplyPending;

        private Subscription(Consumer<VersionedConfiguration> listener) {
            this.listener = listener;
        }

        private synchronized void initialize() {
            VersionedConfiguration current;
            // Take the application monitor before publishing the subscription so no newer revision can
            // be attempted before the synchronous initial replay used by BookKeeper initialization.
            synchronized (BookieRackConfigurationSnapshot.this) {
                if (closed) {
                    throw new IllegalStateException("Bookie rack configuration is closed");
                }
                subscribers.add(this);
                current = latest;
            }
            if (current != null && !closed) {
                apply(current);
            }
        }

        private void requestApplication(boolean reapply) {
            synchronized (workLock) {
                if (!closed && !cancelled) {
                    applicationRequested = true;
                    reapplyRequested |= reapply;
                    scheduleApplication();
                }
            }
        }

        // Called under workLock. Multiple notifications can share one application of the latest revision.
        private void scheduleApplication() {
            if (!closed && !cancelled && applicationRequested && !applicationScheduled) {
                applicationScheduled = true;
                executeApplication(this::drainApplications);
            }
        }

        private void drainApplications() {
            try {
                while (true) {
                    boolean reapply;
                    synchronized (workLock) {
                        if (closed || cancelled || !applicationRequested) {
                            return;
                        }
                        applicationRequested = false;
                        reapply = reapplyRequested;
                        reapplyRequested = false;
                    }
                    applyLatestOrRetry(reapply);
                }
            } finally {
                synchronized (workLock) {
                    applicationScheduled = false;
                    scheduleApplication();
                }
            }
        }

        private synchronized void applyLatestOrRetry(boolean reapply) {
            if (closed || cancelled) {
                return;
            }
            reapplyPending |= reapply;
            var current = latest;
            if (current == null || (!reapplyPending && current.version() <= appliedVersion)) {
                return;
            }
            try {
                apply(current);
            } catch (RuntimeException error) {
                log.warn().onceEvery(Duration.ofMinutes(1)).exception(error)
                        .log("Failed to apply bookie rack configuration");
                synchronized (workLock) {
                    if (retry == null && !cancelled) {
                        long generation = ++retryGeneration;
                        retry = schedule(() -> requestRetry(generation), retryDelay);
                        retryDelay = Math.min(MAX_RETRY_DELAY_NANOS, 2 * retryDelay);
                    }
                }
            }
        }

        // Called under the application monitor. Later attempts always select the latest revision under that
        // same monitor, so an older successful replay cannot cancel a newer revision's retry.
        private void apply(VersionedConfiguration configuration) {
            listener.accept(configuration);
            appliedVersion = configuration.version();
            reapplyPending = false;
            synchronized (workLock) {
                cancelRetry();
                retryDelay = INITIAL_RETRY_DELAY_NANOS;
            }
        }

        private void requestRetry(long generation) {
            synchronized (workLock) {
                if (generation == retryGeneration) {
                    retry = null;
                    requestApplication(false);
                }
            }
        }

        // Called under workLock.
        private void cancelRetry() {
            if (retry != null) {
                retryGeneration++;
                retry.cancel(false);
                retry = null;
            }
        }

        // Bookie service info can become resolvable after a configuration has already been applied.
        void reapplyOnBookieChange() {
            if (closed || cancelled) {
                return;
            }
            if (firstBookieNotification.getAndSet(false)) {
                // Registration has just filled its service-info cache. BookKeeper builds its initial topology
                // after this callback returns, so opaque bookie IDs must have their addresses mapped now.
                applyLatestOrRetry(true);
            } else {
                requestApplication(true);
            }
        }

        @Override
        public void run() {
            // Do not acquire the application monitor, which can be held by an active callback.
            synchronized (workLock) {
                cancelled = true;
                cancelRetry();
            }
            synchronized (BookieRackConfigurationSnapshot.this) {
                subscribers.remove(this);
            }
        }
    }

    private static final class ReadAttempt {
        final long generation;
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final CompletableFuture<Map<String, Map<String, BookieInfo>>> bounded = new CompletableFuture<>();

        ReadAttempt(long generation) {
            this.generation = generation;
        }
    }
}

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
                    requestRefresh(true);
                }
            });
        }
        scheduleRead();
    }

    private void handleNotification(Notification notification) {
        if (BOOKIE_INFO_ROOT_PATH.equals(notification.getPath())
                && notification.getType() != NotificationType.ChildrenChanged) {
            // The store has already invalidated the cache or replaced the old entry with the new, pending read.
            requestRefresh(false);
        }
    }

    private synchronized void requestRefresh(boolean invalidate) {
        changeGeneration++;
        invalidateBeforeRetry |= invalidate;
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
        VersionedConfiguration current;
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("Bookie rack configuration is closed");
            }
            subscribers.add(subscription);
            current = latest;
        }
        if (current != null) {
            try {
                subscription.publish(current);
            } catch (RuntimeException | Error e) {
                subscription.run();
                throw e;
            }
        }
        return subscription;
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
        List<Subscription> listeners = List.of();
        synchronized (this) {
            if (closed || attempt != read) {
                return;
            }
            if (error == null && changeGeneration == read.generation
                    && (latest == null || !latest.configuration().equals(configuration))) {
                published = new VersionedConfiguration(latest == null ? 1 : latest.version() + 1, configuration);
                latest = published;
                listeners = List.copyOf(subscribers);
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
        if (published != null) {
            if (published.version() == 1) {
                firstRead.complete(null);
                firstLoad.complete(null);
            }
            for (var listener : listeners) {
                if (closed) {
                    break;
                }
                listener.publishOrRetry(published);
            }
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
        private final AtomicBoolean reapplyQueued = new AtomicBoolean();
        private long lastVersion;
        private long retryDelay = INITIAL_RETRY_DELAY_NANOS;
        private volatile ScheduledFuture<?> retry;
        private volatile boolean cancelled;

        private Subscription(Consumer<VersionedConfiguration> listener) {
            this.listener = listener;
        }

        // A new subscriber can replay the current version concurrently with a publication.
        private synchronized void publish(VersionedConfiguration configuration) {
            if (!closed && !cancelled && configuration.version() > lastVersion) {
                listener.accept(configuration);
                lastVersion = configuration.version();
                if (retry != null) {
                    retry.cancel(false);
                    retry = null;
                }
                retryDelay = INITIAL_RETRY_DELAY_NANOS;
            }
        }

        private synchronized void publishOrRetry(VersionedConfiguration configuration) {
            try {
                publish(configuration);
            } catch (RuntimeException error) {
                log.warn().onceEvery(Duration.ofMinutes(1)).exception(error)
                        .log("Failed to apply bookie rack configuration");
                if (retry == null && !cancelled) {
                    var scheduled = schedule(this::retryLatest, retryDelay);
                    retry = scheduled;
                    if (cancelled && scheduled != null) {
                        scheduled.cancel(false);
                    }
                    retryDelay = Math.min(MAX_RETRY_DELAY_NANOS, 2 * retryDelay);
                }
            }
        }

        private synchronized void retryLatest() {
            retry = null;
            publishOrRetry(latest);
        }

        // Bookie service info can become resolvable after a configuration has already been applied.
        void reapplyOnBookieChange() {
            if (cancelled) {
                return;
            }
            if (firstBookieNotification.getAndSet(false)) {
                // Registration has just filled its service-info cache. BookKeeper builds its initial topology
                // after this callback returns, so opaque bookie IDs must have their addresses mapped now.
                reapplyLatest();
            } else if (reapplyQueued.compareAndSet(false, true)) {
                schedule(() -> {
                    reapplyQueued.set(false);
                    reapplyLatest();
                }, 0);
            }
        }

        private synchronized void reapplyLatest() {
            var current = latest;
            if (current != null) {
                lastVersion = 0;
                publishOrRetry(current);
            }
        }

        @Override
        public void run() {
            // Do not acquire this subscription's monitor, which can be held by an active callback.
            cancelled = true;
            synchronized (BookieRackConfigurationSnapshot.this) {
                subscribers.remove(this);
            }
            var scheduled = retry;
            if (scheduled != null) {
                scheduled.cancel(false);
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

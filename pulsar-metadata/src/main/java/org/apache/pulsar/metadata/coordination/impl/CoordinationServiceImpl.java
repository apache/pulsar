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
package org.apache.pulsar.metadata.coordination.impl;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.common.util.GracefulExecutorServicesShutdown;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

@CustomLog
@SuppressWarnings("unchecked")
public class CoordinationServiceImpl implements CoordinationService {

    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private static final int GET_NEXT_COUNTER_VALUE_RETRY_COUNT = 5;

    private final MetadataStoreExtended store;

    private final Map<Object, LockManager<?>> lockManagers = new ConcurrentHashMap<>();
    // The leader elections that are not closed, by path, to which the store notifications and session
    // events are dispatched. A closed election is removed, and added back if it's re-opened.
    private final Map<String, LeaderElectionImpl<?>> leaderElections = new ConcurrentHashMap<>();
    private Runnable cancelLeaderElectionListener;
    private Runnable cancelLeaderElectionSessionListener;

    private final ScheduledExecutorService executor;

    public CoordinationServiceImpl(MetadataStoreExtended store) {
        this.store = store;
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("metadata-store-coordination-service"));
    }

    @Override
    public void close() throws Exception {
        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            futures.add(GracefulExecutorServicesShutdown
                    .initiate()
                    .shutdown(executor)
                    .handle());

            for (LeaderElection<?> le : leaderElections.values()) {
                futures.add(le.asyncClose());
            }

            for (LockManager<?> lm : lockManagers.values()) {
                futures.add(lm.asyncClose());
            }


            FutureUtils.collect(futures).get(CLOSE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (CompletionException ce) {
            throw MetadataStoreException.unwrap(ce);
        } finally {
            executor.shutdownNow();
            unregisterLeaderElectionListeners();
        }
    }

    @Override
    public <T> LockManager<T> getLockManager(Class<T> clazz) {
        return (LockManager<T>) lockManagers.computeIfAbsent(clazz,
                k -> new LockManagerImpl<T>(store, clazz, executor));
    }

    @Override
    public <T> LockManager<T> getLockManager(MetadataSerde<T> serde) {
        return (LockManager<T>) lockManagers.computeIfAbsent(serde,
                k -> new LockManagerImpl<T>(store, serde, executor));
    }

    @Override
    public CompletableFuture<Long> getNextCounterValue(String path) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        internalGetNextCounterValueWithRetry(path, future, GET_NEXT_COUNTER_VALUE_RETRY_COUNT);
        return future;
    }

    private CompletableFuture<Long> internalGetNextCounterValue(String path) {
        return store.exists(path)
                .thenCompose(exists -> {
                    if (exists) {
                        // The base path already exists
                        return incrementCounter(path);
                    } else {
                        return store.put(path, new byte[0], Optional.empty())
                                .thenCompose(__ -> incrementCounter(path));
                    }
                });
    }

    private void internalGetNextCounterValueWithRetry(String path, CompletableFuture<Long> future, int count) {
        if (count == 0) {
            log.error().attr("path", path).log("The number of retries has exhausted when get next counter value");
            future.completeExceptionally(new MetadataStoreException("The number of retries has exhausted"));
            return;
        }
        this.internalGetNextCounterValue(path)
                .thenAccept(future::complete)
                .exceptionally(ex -> {
                    if (ex.getCause() instanceof MetadataStoreException.BadVersionException) {
                        log.warn().attr("path", path)
                                .log("Failed to get next counter value because of bad version."
                                        + " Retry to get next counter value");
                        internalGetNextCounterValueWithRetry(path, future, count - 1);
                    } else {
                        log.error().attr("path", path).exception(ex).log("Failed to get next counter value");
                        future.completeExceptionally(ex);
                    }
                    return null;
                });
    }

    private CompletableFuture<Long> incrementCounter(String path) {
        String counterBasePath = path + "/-";
        return store
                .put(counterBasePath, new byte[0], Optional.of(-1L),
                        EnumSet.of(CreateOption.Ephemeral, CreateOption.Sequential))
                .thenApply(stat -> {
                    String[] parts = stat.getPath().split("/");
                    String seq = parts[parts.length - 1].replace('-', ' ').trim();
                    return Long.parseLong(seq);
                });
    }

    @Override
    public <T> LeaderElection<T> getLeaderElection(Class<T> clazz, String path,
            Consumer<LeaderElectionState> stateChangesListener) {

        registerLeaderElectionListeners();
        return (LeaderElection<T>) leaderElections.computeIfAbsent(path,
                key -> new LeaderElectionImpl<T>(this, store, clazz, path, stateChangesListener, executor));
    }

    /**
     * Register the store listener and the session listener of the elections, once: the elections share them, they
     * dispatch to the elections that are not closed, and they are unregistered when this service is closed. The
     * store calls its listeners in registration order, so every election sees a notification where the first
     * election was created: after the listeners registered before it, like the one of the broker's metadata
     * resources, and before the ones registered after it. That's why they are registered with the first election,
     * rather than when this service is created.
     */
    private synchronized void registerLeaderElectionListeners() {
        if (cancelLeaderElectionListener == null) {
            cancelLeaderElectionListener = store.registerCancellableListener(this::handleLeaderElectionNotification);
            cancelLeaderElectionSessionListener =
                    store.registerCancellableSessionListener(this::handleLeaderElectionSessionEvent);
        }
    }

    private synchronized void unregisterLeaderElectionListeners() {
        if (cancelLeaderElectionListener != null) {
            cancelLeaderElectionListener.run();
            cancelLeaderElectionSessionListener.run();
            cancelLeaderElectionListener = null;
            cancelLeaderElectionSessionListener = null;
        }
    }

    private void handleLeaderElectionNotification(Notification notification) {
        LeaderElectionImpl<?> leaderElection = leaderElections.get(notification.getPath());
        if (leaderElection != null) {
            leaderElection.handlePathNotification(notification);
        }
    }

    private void handleLeaderElectionSessionEvent(SessionEvent event) {
        leaderElections.values().forEach(leaderElection -> leaderElection.handleSessionNotification(event));
    }

    /**
     * Stop dispatching the store notifications and session events to a leader election that was closed.
     */
    void removeLeaderElection(String path, LeaderElectionImpl<?> leaderElection) {
        leaderElections.remove(path, leaderElection);
    }

    /**
     * Dispatch the store notifications and session events again to a closed leader election that is
     * re-opened, unless another election was created for its path since it was closed.
     *
     * @return whether the election was added back
     */
    boolean readdLeaderElection(String path, LeaderElectionImpl<?> leaderElection) {
        return leaderElections.putIfAbsent(path, leaderElection) == null;
    }

    @VisibleForTesting
    Map<String, LeaderElectionImpl<?>> getLeaderElections() {
        return Collections.unmodifiableMap(leaderElections);
    }
}

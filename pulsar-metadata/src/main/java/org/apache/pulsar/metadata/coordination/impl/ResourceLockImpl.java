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

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.BackoffBuilder;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class ResourceLockImpl<T> implements ResourceLock<T> {

    private final MetadataStoreExtended store;
    private final MetadataSerde<T> serde;
    private final String path;

    private volatile T value;
    private long version;
    private final CompletableFuture<Void> expiredFuture;
    private boolean revalidateAfterReconnection = false;
    private final Backoff backoff;
    private final FutureUtil.Sequencer<Void> sequencer;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> revalidateTask;

    private enum State {
        Init,
        Valid,
        Releasing,
        Released,
    }

    private State state;

    ResourceLockImpl(MetadataStoreExtended store, MetadataSerde<T> serde, String path,
                     ScheduledExecutorService executor) {
        this.store = store;
        this.serde = serde;
        this.path = path;
        this.version = -1;
        this.expiredFuture = new CompletableFuture<>();
        this.sequencer = FutureUtil.Sequencer.create();
        this.state = State.Init;
        this.executor = executor;
        this.backoff = new BackoffBuilder()
                .setInitialTime(100, TimeUnit.MILLISECONDS)
                .setMax(60, TimeUnit.SECONDS)
                .create();
    }

    @Override
    public synchronized T getValue() {
        return value;
    }

    @Override
    public synchronized CompletableFuture<Void> updateValue(T newValue) {
        // If there is an operation in progress, we're going to let it complete before attempting to
        // update the value
        return sequencer.sequential(() -> {
            synchronized (ResourceLockImpl.this) {
                if (state != State.Valid) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Lock was not in valid state: " + state));
                }

                return acquire(newValue);
            }
        });
    }

    @Override
    public synchronized CompletableFuture<Void> release() {
        if (state == State.Released) {
            return CompletableFuture.completedFuture(null);
        }

        state = State.Releasing;
        if (revalidateTask != null) {
            revalidateTask.cancel(true);
        }

        CompletableFuture<Void> result = new CompletableFuture<>();

        store.delete(path, Optional.of(version))
                .thenRun(() -> {
                    synchronized (ResourceLockImpl.this) {
                        state = State.Released;
                    }
                    expiredFuture.complete(null);
                    result.complete(null);
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof MetadataStoreException.NotFoundException) {
                        // The lock is not there on release. We can anyway proceed
                        synchronized (ResourceLockImpl.this) {
                            state = State.Released;
                        }
                        expiredFuture.complete(null);
                        result.complete(null);
                    } else {
                        result.completeExceptionally(ex);
                    }
                    return null;
                });

        return result;
    }

    @Override
    public CompletableFuture<Void> getLockExpiredFuture() {
        return expiredFuture;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    synchronized CompletableFuture<Void> acquire(T newValue) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        acquireWithNoRevalidation(newValue)
                .thenRun(() -> result.complete(null))
                .exceptionally(ex -> {
                    if (ex.getCause() instanceof LockBusyException) {
                        revalidate(newValue)
                                .thenAccept(__ -> result.complete(null))
                                .exceptionally(ex1 -> {
                                   result.completeExceptionally(ex1);
                                   return null;
                                });
                    } else {
                        result.completeExceptionally(ex.getCause());
                    }
                    return null;
                });

        return result;
    }

    // Simple operation of acquiring the lock with no retries, or checking for the lock content
    private CompletableFuture<Void> acquireWithNoRevalidation(T newValue) {
        if (log.isDebugEnabled()) {
            log.debug("acquireWithNoRevalidation,newValue={},version={}", newValue, version);
        }
        byte[] payload;
        try {
            payload = serde.serialize(path, newValue);
        } catch (Throwable t) {
            return FutureUtils.exception(t);
        }

        CompletableFuture<Void> result = new CompletableFuture<>();
        store.put(path, payload, Optional.of(version), EnumSet.of(CreateOption.Ephemeral))
                .thenAccept(stat -> {
                    synchronized (ResourceLockImpl.this) {
                        state = State.Valid;
                        version = stat.getVersion();
                        value = newValue;
                    }
                    log.info("Acquired resource lock on {}", path);
                    result.complete(null);
                }).exceptionally(ex -> {
            if (ex.getCause() instanceof BadVersionException) {
                result.completeExceptionally(
                        new LockBusyException("Resource at " + path + " is already locked"));
            } else {
                result.completeExceptionally(ex.getCause());
            }
            return null;
        });

        return result;
    }

    synchronized void lockWasInvalidated() {
        log.info("Lock on resource {} was invalidated. state {}", path, state);
        silentRevalidateOnce();
    }

    synchronized CompletableFuture<Void> revalidateIfNeededAfterReconnection() {
        if (revalidateAfterReconnection) {
            revalidateAfterReconnection = false;
            log.warn("Revalidate lock at {} after reconnection", path);
            return silentRevalidateOnce();
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Revalidate the distributed lock if it is not released.
     * This method is thread-safe and it will perform multiple re-validation operations in turn.
     */
    synchronized CompletableFuture<Void> silentRevalidateOnce() {
        if (state != State.Valid) {
            return CompletableFuture.completedFuture(null);
        }

        return sequencer.sequential(() -> revalidate(value))
                .thenRun(() -> {
                    log.info("Successfully revalidated the lock on {}", path);
                    backoff.reset();
                })
                .exceptionally(ex -> {
                    synchronized (ResourceLockImpl.this) {
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        if (realCause instanceof BadVersionException || realCause instanceof LockBusyException) {
                            log.warn("Failed to revalidate the lock at {}. Marked as expired. {}",
                                    path, realCause.getMessage());
                            state = State.Released;
                            expiredFuture.complete(null);
                        } else {
                            // We failed to revalidate the lock due to connectivity issue
                            // Continue assuming we hold the lock, until we can revalidate it, either
                            // on Reconnected or SessionReestablished events.
                            revalidateAfterReconnection = true;

                            long delayMillis = backoff.next();
                            log.warn("Failed to revalidate the lock at {}: {} - Retrying in {} seconds", path,
                                    realCause.getMessage(), delayMillis / 1000.0);
                            revalidateTask =
                                    executor.schedule(this::silentRevalidateOnce, delayMillis, TimeUnit.MILLISECONDS);
                        }
                    }
                    return null;
                });
    }

    private synchronized CompletableFuture<Void> revalidate(T newValue) {
        // Since the distributed lock has been expired, we don't need to revalidate it.
        if (state != State.Valid && state != State.Init) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Lock was not in valid state: " + state));
        }
        if (log.isDebugEnabled()) {
            log.debug("doRevalidate with newValue={}, version={}", newValue, version);
        }
        return store.get(path)
                .thenCompose(optGetResult -> {
                    if (!optGetResult.isPresent()) {
                        // The lock just disappeared, try to acquire it again
                        // Reset the expectation on the version
                        setVersion(-1L);
                        return acquireWithNoRevalidation(newValue)
                                .thenRun(() -> log.info("Successfully re-acquired missing lock at {}", path));
                    }

                    GetResult res = optGetResult.get();
                    if (!res.getStat().isEphemeral()) {
                        return FutureUtils.exception(
                                new LockBusyException(
                                        "Path " + path + " is already created as non-ephemeral"));
                    }

                    T existingValue;
                    try {
                        existingValue = serde.deserialize(path, res.getValue(), res.getStat());
                    } catch (Throwable t) {
                        return FutureUtils.exception(t);
                    }

                    synchronized (ResourceLockImpl.this) {
                        if (newValue.equals(existingValue)) {
                            // The lock value is still the same, that means that we're the
                            // logical "owners" of the lock.

                            if (res.getStat().isCreatedBySelf()) {
                                // If the new lock belongs to the same session, there's no
                                // need to recreate it.
                                version = res.getStat().getVersion();
                                value = newValue;
                                return CompletableFuture.completedFuture(null);
                            } else {
                                // The lock needs to get recreated since it belong to an earlier
                                // session which maybe expiring soon
                                log.info("Deleting stale lock at {}", path);
                                return store.delete(path, Optional.of(res.getStat().getVersion()))
                                        .thenRun(() ->
                                            // Reset the expectation that the key is not there anymore
                                            setVersion(-1L)
                                        )
                                        .thenCompose(__ -> acquireWithNoRevalidation(newValue))
                                        .thenRun(() -> log.info("Successfully re-acquired stale lock at {}", path));
                            }
                        }

                        // At this point we have an existing lock with a value different to what we
                        // expect. If our session is the owner, we can recreate, otherwise the
                        // lock has been acquired by someone else and we give up.

                        if (!res.getStat().isCreatedBySelf()) {
                            return FutureUtils.exception(
                                    new LockBusyException("Resource at " + path + " is already locked"));
                        }

                        return store.delete(path, Optional.of(res.getStat().getVersion()))
                                .thenRun(() ->
                                    // Reset the expectation that the key is not there anymore
                                    setVersion(-1L)
                                )
                                .thenCompose(__ -> acquireWithNoRevalidation(newValue))
                                .thenRun(() -> log.info("Successfully re-acquired lock at {}", path));
                    }
                });
    }

    private synchronized void setVersion(long version) {
        this.version = version;
    }
}

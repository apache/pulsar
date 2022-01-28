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
package org.apache.pulsar.metadata.coordination.impl;

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
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
    private CompletableFuture<Void> revalidateFuture;

    private enum State {
        Init,
        Valid,
        Releasing,
        Released,
    }

    private State state;

    public ResourceLockImpl(MetadataStoreExtended store, MetadataSerde<T> serde, String path) {
        this.store = store;
        this.serde = serde;
        this.path = path;
        this.version = -1;
        this.expiredFuture = new CompletableFuture<>();
        this.state = State.Init;
    }

    @Override
    public synchronized T getValue() {
        return value;
    }

    @Override
    public synchronized CompletableFuture<Void> updateValue(T newValue) {
       return acquire(newValue);
    }

    @Override
    public synchronized CompletableFuture<Void> release() {
        if (state == State.Released) {
            return CompletableFuture.completedFuture(null);
        }

        state = State.Releasing;
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
        if (state != State.Valid) {
            // Ignore notifications while we're releasing the lock ourselves
            return;
        }

        log.info("Lock on resource {} was invalidated", path);
        revalidate(value)
                .thenRun(() -> log.info("Successfully revalidated the lock on {}", path))
                .exceptionally(ex -> {
                    synchronized (ResourceLockImpl.this) {
                        if (ex.getCause() instanceof BadVersionException) {
                            log.warn("Failed to revalidate the lock at {}. Marked as expired", path);
                            state = State.Released;
                            expiredFuture.complete(null);
                        } else {
                            // We failed to revalidate the lock due to connectivity issue
                            // Continue assuming we hold the lock, until we can revalidate it, either
                            // on Reconnected or SessionReestablished events.
                            log.warn("Failed to revalidate the lock at {}. Retrying later on reconnection {}", path,
                                    ex.getCause().getMessage());
                        }
                    }
                    return null;
                });
    }

    synchronized CompletableFuture<Void> revalidateIfNeededAfterReconnection() {
        if (revalidateAfterReconnection) {
            revalidateAfterReconnection = false;
            log.warn("Revalidate lock at {} after reconnection", path);
            return revalidate(value);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    synchronized CompletableFuture<Void> revalidate(T newValue) {
        if (revalidateFuture == null || revalidateFuture.isDone()) {
            revalidateFuture = doRevalidate(newValue);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Previous revalidating is not finished while revalidate newValue={}, value={}, version={}",
                        newValue, value, version);
            }
            CompletableFuture<Void> newFuture = new CompletableFuture<>();
            revalidateFuture.whenComplete((unused, throwable) -> {
                doRevalidate(newValue).thenRun(() -> newFuture.complete(null))
                        .exceptionally(throwable1 -> {
                            newFuture.completeExceptionally(throwable1);
                            return null;
                        });
            });
            revalidateFuture = newFuture;
        }
        return revalidateFuture;
    }

    private synchronized CompletableFuture<Void> doRevalidate(T newValue) {
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

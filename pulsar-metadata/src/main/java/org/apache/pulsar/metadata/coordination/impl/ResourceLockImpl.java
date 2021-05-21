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
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.MetadataSerde;

@Slf4j
public class ResourceLockImpl<T> implements ResourceLock<T> {

    private final MetadataStoreExtended store;
    private final MetadataSerde<T> serde;
    private final String path;

    private volatile T value;
    private long version;
    private final CompletableFuture<Void> expiredFuture;

    private enum State {
        Init,
        Valid,
        Releasing,
        Released,
    }

    private State state;

    public ResourceLockImpl(MetadataStoreExtended store, MetadataSerde<T> serde, String path, T value) {
        this.store = store;
        this.serde = serde;
        this.path = path;
        this.value = value;
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
        byte[] payload;
        try {
            payload = serde.serialize(newValue);
        } catch (Throwable t) {
            return FutureUtils.exception(t);
        }

        return store.put(path, payload, Optional.of(version))
                .thenAccept(stat -> {
                    synchronized (ResourceLockImpl.this) {
                        value = newValue;
                        version = stat.getVersion();
                    }
                });
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

    synchronized CompletableFuture<Void> acquire() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        acquireWithNoRevalidation()
                .thenRun(() -> result.complete(null))
                .exceptionally(ex -> {
                    if (ex.getCause() instanceof LockBusyException) {
                        revalidate()
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
    private CompletableFuture<Void> acquireWithNoRevalidation() {
        byte[] payload;
        try {
            payload = serde.serialize(value);
        } catch (Throwable t) {
            return FutureUtils.exception(t);
        }

        CompletableFuture<Void> result = new CompletableFuture<>();
        store.put(path, payload, Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral))
                .thenAccept(stat -> {
                    synchronized (ResourceLockImpl.this) {
                        state = State.Valid;
                        version = stat.getVersion();
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

    synchronized  void lockWasInvalidated() {
        if (state != State.Valid) {
            // Ignore notifications while we're releasing the lock ourselves
            return;
        }

        log.info("Lock on resource {} was invalidated", path);
        revalidate()
                .thenRun(() -> log.info("Successfully revalidated the lock on {}", path))
                .exceptionally(ex -> {
                    synchronized (ResourceLockImpl.this) {
                        log.warn("Failed to revalidate the lock at {}. Marked as expired", path);
                        state = State.Released;
                        expiredFuture.complete(null);
                    }
                    return null;
                });
    }

    synchronized CompletableFuture<Void> revalidate() {
        return store.get(path)
                .thenCompose(optGetResult -> {
                    if (!optGetResult.isPresent()) {
                        // The lock just disappeared, try to acquire it again
                        return acquireWithNoRevalidation()
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
                        existingValue = serde.deserialize(optGetResult.get().getValue());
                    } catch (Throwable t) {
                        return FutureUtils.exception(t);
                    }

                    synchronized (ResourceLockImpl.this) {
                        if (value.equals(existingValue)) {
                            // The lock value is still the same, that means that we're the
                            // logical "owners" of the lock.

                            if (res.getStat().isCreatedBySelf()) {
                                // If the new lock belongs to the same session, there's no
                                // need to recreate it.
                                version = res.getStat().getVersion();
                            } else {
                                // The lock needs to get recreated since it belong to an earlier
                                // session which maybe expiring soon
                                log.info("Deleting stale lock at {}", path);
                                return store.delete(path, Optional.of(res.getStat().getVersion()))
                                        .thenCompose(__ -> acquireWithNoRevalidation())
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
                                .thenCompose(__ -> acquireWithNoRevalidation())
                                .thenRun(() -> log.info("Successfully re-acquired lock at {}", path));
                    }
                });
    }
}

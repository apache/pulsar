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

import com.fasterxml.jackson.databind.type.TypeFactory;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.cache.impl.JSONMetadataSerdeSimpleType;
import org.apache.pulsar.metadata.cache.impl.MetadataSerde;

@Slf4j
class LockManagerImpl<T> implements LockManager<T> {

    private final Set<ResourceLock<T>> locks = new HashSet<>();
    private final MetadataStoreExtended store;
    private final MetadataCache<T> cache;
    private final MetadataSerde<T> serde;

    private static enum State {
        Ready, Closed
    }

    private State state = State.Ready;

    LockManagerImpl(MetadataStoreExtended store, Class<T> clazz) {
        this.store = store;
        this.cache = store.getMetadataCache(clazz);
        this.serde = new JSONMetadataSerdeSimpleType<>(TypeFactory.defaultInstance().constructSimpleType(clazz, null));
    }

    @Override
    public CompletableFuture<Optional<T>> readLock(String path) {
        return cache.get(path);
    }

    @Override
    public CompletableFuture<ResourceLock<T>> acquireLock(String path, T value) {
        byte[] payload;
        try {
            payload = serde.serialize(value);
        } catch (Throwable t) {
            return FutureUtils.exception(t);
        }

        CompletableFuture<ResourceLock<T>> result = new CompletableFuture<>();
        store.put(path, payload, Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral))
                .thenAccept(stat -> {
                    ResourceLock<T> lock = new ResourceLockImpl<>(store, serde, path, value,
                            stat.getVersion());
                    synchronized (LockManagerImpl.this) {
                        if (state == State.Ready) {
                            log.info("Acquired resource lock on {}", path);
                            locks.add(lock);
                            lock.getLockExpiredFuture().thenRun(() -> {
                                log.info("Released resource lock on {}", path);
                                synchronized (LockManagerImpl.this) {
                                    locks.remove(lock);
                                }
                            });
                        } else {
                            // LockManager was closed in between. Release the lock asynchronously
                            lock.release();
                        }
                    }
                    result.complete(lock);
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

    @Override
    public CompletableFuture<List<String>> listLocks(String path) {
        return cache.getChildren(path);
    }

    @Override
    public void close() throws Exception {
        try {
            asyncClose().join();
        } catch (CompletionException e) {
            throw MetadataStoreException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        Set<ResourceLock<T>> locks;
        synchronized (this) {
            if (state != State.Ready) {
                return CompletableFuture.completedFuture(null);
            }

            locks = new HashSet<>(this.locks);
            this.state = State.Closed;
        }

        return FutureUtils.collect(
                locks.stream().map(ResourceLock::release)
                        .collect(Collectors.toList()))
                .thenApply(x -> null);
    }
}

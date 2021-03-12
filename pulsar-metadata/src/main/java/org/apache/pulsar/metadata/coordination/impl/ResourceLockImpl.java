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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.cache.impl.MetadataSerde;

public class ResourceLockImpl<T> implements ResourceLock<T> {

    private final MetadataStore store;
    private final MetadataSerde<T> serde;
    private final String path;

    private volatile T value;
    private long version;
    private final CompletableFuture<Void> expiredFuture;

    private static enum State {
        Valid, Released
    }

    private State state;

    public ResourceLockImpl(MetadataStore store, MetadataSerde<T> serde, String path, T value, long version) {
        this.store = store;
        this.serde = serde;
        this.path = path;
        this.value = value;
        this.version = version;
        this.expiredFuture = new CompletableFuture<>();
        this.state = State.Valid;
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

        return store.delete(path, Optional.of(version))
                .thenRun(() -> {
                    synchronized (ResourceLockImpl.this) {
                        state = State.Released;
                    }
                    expiredFuture.complete(null);
                });
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
}

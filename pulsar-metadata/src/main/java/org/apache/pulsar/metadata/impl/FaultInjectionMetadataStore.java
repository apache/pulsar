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
package org.apache.pulsar.metadata.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataEvent;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;

/**
 * Add possibility to inject failures during tests that interact with MetadataStore.
 */
public class FaultInjectionMetadataStore implements MetadataStoreExtended {

    private final MetadataStoreExtended store;
    private final AtomicReference<MetadataStoreException> alwaysFail;
    private final CopyOnWriteArrayList<Failure> failures;
    private final List<Consumer<SessionEvent>> sessionListeners = new CopyOnWriteArrayList<>();

    public enum OperationType {
        GET,
        GET_CHILDREN,
        EXISTS,
        PUT,
        DELETE,
    }

    @Data
    private static class Failure {
        private final MetadataStoreException exception;
        private final BiPredicate<OperationType, String> predicate;
    }

    public FaultInjectionMetadataStore(MetadataStoreExtended store) {
        this.store = store;
        this.failures = new CopyOnWriteArrayList<>();
        this.alwaysFail = new AtomicReference<>();
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        Optional<MetadataStoreException> ex = programmedFailure(OperationType.GET, path);
        if (ex.isPresent()) {
            return FutureUtil.failedFuture(ex.get());
        }

        return store.get(path);
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String path) {
        Optional<MetadataStoreException> ex = programmedFailure(OperationType.GET_CHILDREN, path);
        if (ex.isPresent()) {
            return FutureUtil.failedFuture(ex.get());
        }

        return store.getChildren(path);
    }

    @Override
    public CompletableFuture<List<String>> getChildrenFromStore(String path) {
        Optional<MetadataStoreException> ex = programmedFailure(OperationType.GET_CHILDREN, path);
        if (ex.isPresent()) {
            return FutureUtil.failedFuture(ex.get());
        }

        return store.getChildrenFromStore(path);
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        Optional<MetadataStoreException> ex = programmedFailure(OperationType.EXISTS, path);
        if (ex.isPresent()) {
            return FutureUtil.failedFuture(ex.get());
        }

        return store.exists(path);
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        Optional<MetadataStoreException> ex = programmedFailure(OperationType.PUT, path);
        if (ex.isPresent()) {
            return FutureUtil.failedFuture(ex.get());
        }

        return store.put(path, value, expectedVersion);
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion,
                                       EnumSet<CreateOption> options) {
        Optional<MetadataStoreException> ex = programmedFailure(OperationType.PUT, path);
        if (ex.isPresent()) {
            return FutureUtil.failedFuture(ex.get());
        }

        return store.put(path, value, expectedVersion, options);
    }

    @Override
    public CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion) {
        Optional<MetadataStoreException> ex = programmedFailure(OperationType.DELETE, path);
        if (ex.isPresent()) {
            return FutureUtil.failedFuture(ex.get());
        }

        return store.delete(path, expectedVersion);
    }

    @Override
    public CompletableFuture<Void> deleteRecursive(String path) {
        Optional<MetadataStoreException> ex = programmedFailure(OperationType.DELETE, path);
        if (ex.isPresent()) {
            return FutureUtil.failedFuture(ex.get());
        }

        return store.deleteRecursive(path);
    }

    @Override
    public void registerListener(Consumer<Notification> listener) {
        store.registerListener(listener);
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(Class<T> clazz, MetadataCacheConfig cacheConfig) {
        return injectMetadataStoreInMetadataCache(store.getMetadataCache(clazz, cacheConfig));
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(TypeReference<T> typeRef, MetadataCacheConfig cacheConfig) {
        return injectMetadataStoreInMetadataCache(store.getMetadataCache(typeRef, cacheConfig));
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(MetadataSerde<T> serde, MetadataCacheConfig cacheConfig) {
        return injectMetadataStoreInMetadataCache(store.getMetadataCache(serde, cacheConfig));
    }

    @SneakyThrows
    private <T> MetadataCache<T> injectMetadataStoreInMetadataCache(MetadataCache<T> metadataCache) {
        if (metadataCache instanceof MetadataCacheImpl) {
            FieldUtils.writeField(metadataCache, "store", this, true);
        } else {
            throw new UnsupportedOperationException("Metadata cache implementation "
                    + metadataCache.getClass().getName() + " not supported by FaultInjectionMetadataStore");
        }
        return metadataCache;
    }

    @Override
    public void registerSessionListener(Consumer<SessionEvent> listener) {
        store.registerSessionListener(listener);
        sessionListeners.add(listener);
    }

    @Override
    public CompletableFuture<Void> handleMetadataEvent(MetadataEvent event) {
        return store.handleMetadataEvent(event);
    }

    @Override
    public void close() throws Exception {
        store.close();
    }

    public void failConditional(MetadataStoreException ex, BiPredicate<OperationType, String> predicate) {
        failures.add(new Failure(ex, predicate));
    }

    public void setAlwaysFail(MetadataStoreException ex) {
        this.alwaysFail.set(ex);
    }

    public void unsetAlwaysFail() {
        this.alwaysFail.set(null);
    }

    public void triggerSessionEvent(SessionEvent event) {
        sessionListeners.forEach(l -> l.accept(event));
    }

    private Optional<MetadataStoreException> programmedFailure(OperationType op, String path) {
        MetadataStoreException ex = this.alwaysFail.get();
        if (ex != null) {
            return Optional.of(ex);
        }
        while (true) {
            Optional<Failure> failure = failures.stream().filter(f -> f.predicate.test(op, path)).findFirst();
            if (failure.isPresent()) {
                if (failures.remove(failure.get())) {
                    return failure.map(Failure::getException);
                }
                // failure is taken by other threads. Retry.
            } else {
                return Optional.empty();
            }
        }
    }
}

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
package org.apache.pulsar.metadata.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

/**
 * Add possibility to inject delays during tests that interact with MetadataStore.
 * This class provides two kinds of delays:
 * 1. Add a random delay (set by {@link #setMinRandomDelayMills} and {@link #setMaxRandomDelayMills}) in each
 * metadata operation.
 * 2. Add a condition delay with target operation and path, this type of delay will be removed once fired. See
 * {@link #delayConditional}
 */
@Slf4j
public class DelayInjectionMetadataStore implements MetadataStoreExtended {

    private final MetadataStoreExtended store;
    private final CopyOnWriteArrayList<Delay> delays;
    private final AtomicInteger maxRandomDelayMills = new AtomicInteger();
    private final AtomicInteger minRandomDelayMills = new AtomicInteger(0);
    private final List<Consumer<SessionEvent>> sessionListeners = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService scheduler;

    public enum OperationType {
        GET,
        GET_CHILDREN,
        EXISTS,
        PUT,
        DELETE,
    }

    @Data
    private static class Delay {
        private final CompletableFuture<Void> delay;
        private final BiPredicate<OperationType, String> predicate;
    }

    public DelayInjectionMetadataStore(MetadataStoreExtended store) {
        this.store = store;
        this.delays = new CopyOnWriteArrayList<>();
        scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("metadata-store-delay-injection"));
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    private CompletableFuture<Void> getRandomDelayStage() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        scheduler.schedule(() -> {
            future.complete(null);
        }, RandomUtils.nextInt(minRandomDelayMills.get(), maxRandomDelayMills.get()), TimeUnit.MILLISECONDS);
        return future;
    }

    CompletableFuture injectRandomDelay(Supplier<CompletableFuture<?>> op) {
        return getRandomDelayStage() //pre delay
                .thenCompose((ignore) -> op.get()
                        .thenCompose(result -> getRandomDelayStage() //post delay.
                                .thenApply(v -> result)));
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        if (maxRandomDelayMills.get() > 0) {
            return injectRandomDelay(()->store.get(path));
        }
        Optional<CompletableFuture<Void>> delay = programmedDelays(OperationType.GET, path);
        if (delay.isPresent()) {
            return delay.get().thenCompose(__ -> store.get(path));
        }
        return store.get(path);
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String path) {
        if (maxRandomDelayMills.get() > 0) {
            return injectRandomDelay(() -> store.getChildren(path));
        }
        Optional<CompletableFuture<Void>> delay = programmedDelays(OperationType.GET_CHILDREN, path);
        if (delay.isPresent()) {
            return delay.get().thenCompose(__ -> store.getChildren(path));
        }
        return store.getChildren(path);
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        if (maxRandomDelayMills.get() > 0) {
            return injectRandomDelay(() -> store.exists(path));
        }
        Optional<CompletableFuture<Void>> delay = programmedDelays(OperationType.EXISTS, path);
        if (delay.isPresent()) {
            return delay.get().thenCompose(__ -> store.exists(path));
        }
        return store.exists(path);
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        if (maxRandomDelayMills.get() > 0) {
            return injectRandomDelay(() -> store.put(path, value, expectedVersion));
        }
        Optional<CompletableFuture<Void>> delay = programmedDelays(OperationType.PUT, path);
        if (delay.isPresent()) {
            return delay.get().thenCompose(__ -> store.put(path, value, expectedVersion));
        }
        return store.put(path, value, expectedVersion);
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion,
                                       EnumSet<CreateOption> options) {
        if (maxRandomDelayMills.get() > 0) {
            return injectRandomDelay(() -> store.put(path, value, expectedVersion, options));
        }
        Optional<CompletableFuture<Void>> delay = programmedDelays(OperationType.PUT, path);
        if (delay.isPresent()) {
            return delay.get().thenCompose(__ -> store.put(path, value, expectedVersion, options));
        }
        return store.put(path, value, expectedVersion, options);
    }

    @Override
    public CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion) {
        if (maxRandomDelayMills.get() > 0) {
            return injectRandomDelay(() -> store.delete(path, expectedVersion));
        }

        Optional<CompletableFuture<Void>> delay = programmedDelays(OperationType.DELETE, path);
        if (delay.isPresent()) {
            return delay.get().thenCompose(__ -> store.delete(path, expectedVersion));
        }
        return store.delete(path, expectedVersion);
    }

    @Override
    public CompletableFuture<Void> deleteRecursive(String path) {
        if (maxRandomDelayMills.get() > 0) {
            return injectRandomDelay(() -> store.deleteRecursive(path));
        }

        Optional<CompletableFuture<Void>> delay = programmedDelays(OperationType.DELETE, path);
        if (delay.isPresent()) {
            return delay.get().thenCompose(__ -> store.deleteRecursive(path));
        }
        return store.deleteRecursive(path);
    }

    @Override
    public void registerListener(Consumer<Notification> listener) {
        store.registerListener(listener);
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(Class<T> clazz) {
        return store.getMetadataCache(clazz);
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(TypeReference<T> typeRef) {
        return store.getMetadataCache(typeRef);
    }

    @Override
    public <T> MetadataCache<T> getMetadataCache(MetadataSerde<T> serde) {
        return store.getMetadataCache(serde);
    }

    @Override
    public void registerSessionListener(Consumer<SessionEvent> listener) {
        store.registerSessionListener(listener);
        sessionListeners.add(listener);
    }

    @Override
    public void close() throws Exception {
        store.close();
    }

    public void setMaxRandomDelayMills(int maxRandomDelayMills) {
        this.maxRandomDelayMills.set(maxRandomDelayMills);
    }

    public void setMinRandomDelayMills(int minRandomDelayMills) {
        this.minRandomDelayMills.set(minRandomDelayMills);
    }

    public void delayConditional(CompletableFuture<Void> wait, BiPredicate<OperationType, String> predicate) {
        delays.add(new Delay(wait, predicate));
    }

    public void triggerSessionEvent(SessionEvent event) {
        sessionListeners.forEach(l -> l.accept(event));
    }

    private Optional<CompletableFuture<Void>> programmedDelays(OperationType op, String path) {
        Optional<Delay> delay = delays.stream()
                .filter(f -> f.predicate.test(op, path))
                .findFirst();
        if (delay.isPresent() && delays.remove(delay.get())) {
            return delay.map(Delay::getDelay);
        }
        return Optional.empty();
    }
}

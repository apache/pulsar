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
package org.apache.pulsar.broker.loadbalance.extensions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;

/**
 * The broker registry impl, base on the LockManager.
 */
@Slf4j
public class BrokerRegistryImpl implements BrokerRegistry {

    private static final String LOOKUP_DATA_PATH = "/loadbalance/brokers";

    private final PulsarService pulsar;

    private final ServiceConfiguration conf;

    private final BrokerLookupData brokerLookupData;

    private final LockManager<BrokerLookupData> brokerLookupDataLockManager;

    private final String brokerZNodePath;

    private final String lookupServiceAddress;

    @VisibleForTesting
    protected final Map<String, BrokerLookupData> brokerLookupDataCache;

    private final ScheduledExecutorService scheduler;

    private final List<BiConsumer<String, NotificationType>> listeners;

    private volatile ResourceLock<BrokerLookupData> brokerLookupDataLock;

    private volatile boolean registered = false;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile CompletableFuture<Void> cacheReloadFuture;

    public BrokerRegistryImpl(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.conf = pulsar.getConfiguration();
        this.brokerLookupDataLockManager = pulsar.getCoordinationService().getLockManager(BrokerLookupData.class);
        this.scheduler = pulsar.getLoadManagerExecutor();
        this.brokerLookupDataCache = new ConcurrentHashMap<>();
        this.listeners = new ArrayList<>();
        this.cacheReloadFuture = CompletableFuture.completedFuture(null);
        this.lookupServiceAddress = pulsar.getLookupServiceAddress();
        this.brokerZNodePath = LOOKUP_DATA_PATH + "/" + lookupServiceAddress;
        this.brokerLookupData = new BrokerLookupData(
                pulsar.getSafeWebServiceAddress(),
                pulsar.getWebServiceAddressTls(),
                pulsar.getBrokerServiceUrl(),
                pulsar.getBrokerServiceUrlTls(),
                pulsar.getAdvertisedListeners(),
                pulsar.getProtocolDataToAdvertise(),
                pulsar.getConfiguration().isEnablePersistentTopics(),
                pulsar.getConfiguration().isEnableNonPersistentTopics(),
                pulsar.getBrokerVersion());
    }

    @Override
    public void start() throws PulsarServerException {
        if (started.get()) {
            return;
        }
        pulsar.getLocalMetadataStore().registerListener(this::handleMetadataStoreNotification);
        try {
            this.register();
            // Update all lookup data to cache
            this.reloadAllBrokerLookupCacheAsync()
                    .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
            this.started.set(true);
        } catch (MetadataStoreException | ExecutionException | InterruptedException | TimeoutException e) {
            throw new PulsarServerException(e);
        }
    }

    @Override
    public boolean isStarted() {
        return this.started.get();
    }

    @Override
    public synchronized void register() throws MetadataStoreException {
        if (!registered) {
            try {
                this.brokerLookupDataLock = brokerLookupDataLockManager.acquireLock(brokerZNodePath, brokerLookupData)
                        .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
                registered = true;
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw MetadataStoreException.unwrap(e);
            }
        }
    }

    @Override
    public synchronized void unregister() throws MetadataStoreException {
        if (registered) {
            try {
                brokerLookupDataLock.release().get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
                registered = false;
            } catch (CompletionException | InterruptedException | ExecutionException | TimeoutException e) {
                throw MetadataStoreException.unwrap(e);
            }
        }
    }

    @Override
    public String getLookupServiceAddress() {
        return this.lookupServiceAddress;
    }

    @Override
    public CompletableFuture<List<String>> getAvailableBrokersAsync() {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        brokerLookupDataLockManager.listLocks(LOOKUP_DATA_PATH)
                .thenAccept(listLocks ->  {
                    if (this.brokerLookupDataCache.size() != listLocks.size()
                            || !this.brokerLookupDataCache.keySet().containsAll(listLocks)) {
                        this.triggerCacheReload();
                    }
                    future.complete(Lists.newArrayList(listLocks));
                })
                .exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    log.warn("Error when trying to get active brokers, return cached active brokers.", realCause);
                    future.complete(Lists.newArrayList(this.brokerLookupDataCache.keySet()));
                    return null;
                });
        return future;
    }

    @Override
    public CompletableFuture<Optional<BrokerLookupData>> lookupAsync(String broker) {
        return brokerLookupDataLockManager.readLock(keyPath(broker));
    }

    @Override
    public void forEach(BiConsumer<String, BrokerLookupData> action) {
        this.brokerLookupDataCache.forEach(action);
    }

    public void listen(BiConsumer<String, NotificationType> listener) {
        this.listeners.add(listener);
    }

    @Override
    public void close() throws PulsarServerException {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        try {
            this.unregister();
            brokerLookupDataLockManager.close();
            scheduler.shutdown();
            this.brokerLookupDataCache.clear();
            this.listeners.clear();
        } catch (Exception ex) {
            if (ex.getCause() instanceof MetadataStoreException.NotFoundException) {
                throw new PulsarServerException.NotFoundException(MetadataStoreException.unwrap(ex));
            } else {
                throw new PulsarServerException(MetadataStoreException.unwrap(ex));
            }
        }
    }

    private void handleMetadataStoreNotification(Notification t) {
        if (t.getPath().startsWith(LOOKUP_DATA_PATH) && t.getPath().length() > LOOKUP_DATA_PATH.length()) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Handle notification: [{}]", t);
                }
                this.scheduler.submit(() -> {
                    String lookupServiceAddress = t.getPath().substring(LOOKUP_DATA_PATH.length() + 1);
                    this.updateBrokerLookupDataToLocalCache(lookupServiceAddress, t.getType());
                    for (BiConsumer<String, NotificationType> listener : listeners) {
                        listener.accept(lookupServiceAddress, t.getType());
                    }
                });
            } catch (RejectedExecutionException e) {
                // Executor is shutting down
            }
        }
    }

    private void updateBrokerLookupDataToLocalCache(String lookupServiceAddress, NotificationType type) {
        switch (type) {
            case Created, Modified, ChildrenChanged -> {
                try {
                    Optional<BrokerLookupData> lookupData =
                            brokerLookupDataLockManager.readLock(keyPath(lookupServiceAddress))
                                    .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
                    if (lookupData.isEmpty()) {
                        brokerLookupDataCache.remove(lookupServiceAddress);
                        log.info("[{}] Broker lookup data is not present", lookupServiceAddress);
                        break;
                    }
                    brokerLookupDataCache.put(lookupServiceAddress, lookupData.get());
                } catch (Exception e) {
                    log.warn("Error reading broker data from cache for broker - [{}], [{}]",
                            lookupServiceAddress, e.getMessage());
                }
            }
            case Deleted -> brokerLookupDataCache.remove(lookupServiceAddress);
        }
    }

    @VisibleForTesting
    protected synchronized CompletableFuture<Void> reloadAllBrokerLookupCacheAsync() {
        if (!cacheReloadFuture.isDone()) {
            return cacheReloadFuture;
        }
        cacheReloadFuture = new CompletableFuture<>();
        brokerLookupDataLockManager.listLocks(LOOKUP_DATA_PATH).thenAccept(activeBrokers -> {
            final List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String broker : activeBrokers) {
                futures.add(brokerLookupDataLockManager.readLock(keyPath(broker)).thenAccept(lookupData -> {
                    if (lookupData.isEmpty()) {
                        brokerLookupDataCache.remove(broker);
                        log.info("[{}] Broker lookup data is not present", broker);
                        return;
                    }
                    // Replace or initialize the lookup data.
                    brokerLookupDataCache.put(broker, lookupData.get());
                }).exceptionally(ex -> {
                    log.warn("Error reading broker lookup data from cache for broker - [{}], [{}]",
                            broker, ex.getMessage());
                    return null;
                }));
            }
            FutureUtil.waitForAll(futures).thenAccept(__ -> {
                // Remove obsolete brokers.
                for (final String broker : brokerLookupDataCache.keySet()) {
                    if (!activeBrokers.contains(broker)) {
                        brokerLookupDataCache.remove(broker);
                    }
                }
                cacheReloadFuture.complete(null);
            }).exceptionally(e -> {
                log.warn("Error to reload the broker lookup cache, [{}]", e.getMessage());
                cacheReloadFuture.complete(null);
                return null;
            });
        });
        return cacheReloadFuture;
    }

    private synchronized void triggerCacheReload() {
        if (cacheReloadFuture.isDone()) {
            scheduler.submit(this::reloadAllBrokerLookupCacheAsync);
        }
    }

    private String keyPath(String lookupServiceAddress) {
        return String.format("%s/%s", LOOKUP_DATA_PATH, lookupServiceAddress);
    }
}

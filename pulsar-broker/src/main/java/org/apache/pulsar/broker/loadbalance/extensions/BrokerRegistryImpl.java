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

    protected static final String LOOKUP_DATA_PATH = "/loadbalance/brokers";

    private final PulsarService pulsar;

    private final ServiceConfiguration conf;

    private final BrokerLookupData brokerLookupData;

    private final LockManager<BrokerLookupData> brokerLookupDataLockManager;

    private final String brokerId;

    private final ScheduledExecutorService scheduler;

    private final List<BiConsumer<String, NotificationType>> listeners;

    private volatile ResourceLock<BrokerLookupData> brokerLookupDataLock;

    protected enum State {
        Init,
        Started,
        Registered,
        Closed
    }

    private State state;

    public BrokerRegistryImpl(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.conf = pulsar.getConfiguration();
        this.brokerLookupDataLockManager = pulsar.getCoordinationService().getLockManager(BrokerLookupData.class);
        this.scheduler = pulsar.getLoadManagerExecutor();
        this.listeners = new ArrayList<>();
        this.brokerId = pulsar.getLookupServiceAddress();
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
        this.state = State.Init;
    }

    @Override
    public synchronized void start() throws PulsarServerException {
        if (this.state != State.Init) {
            return;
        }
        pulsar.getLocalMetadataStore().registerListener(this::handleMetadataStoreNotification);
        try {
            this.state = State.Started;
            this.register();
        } catch (MetadataStoreException e) {
            throw new PulsarServerException(e);
        }
    }

    @Override
    public boolean isStarted() {
        return this.state == State.Started || this.state == State.Registered;
    }

    @Override
    public synchronized void register() throws MetadataStoreException {
        if (this.state == State.Started) {
            try {
                this.brokerLookupDataLock = brokerLookupDataLockManager.acquireLock(keyPath(brokerId), brokerLookupData)
                        .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
                this.state = State.Registered;
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw MetadataStoreException.unwrap(e);
            }
        }
    }

    @Override
    public synchronized void unregister() throws MetadataStoreException {
        if (this.state == State.Registered) {
            try {
                this.brokerLookupDataLock.release()
                        .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
                this.state = State.Started;
            } catch (CompletionException | InterruptedException | ExecutionException | TimeoutException e) {
                throw MetadataStoreException.unwrap(e);
            }
        }
    }

    @Override
    public String getBrokerId() {
        return this.brokerId;
    }

    @Override
    public CompletableFuture<List<String>> getAvailableBrokersAsync() {
        this.checkState();
        return brokerLookupDataLockManager.listLocks(LOOKUP_DATA_PATH).thenApply(Lists::newArrayList);
    }

    @Override
    public CompletableFuture<Optional<BrokerLookupData>> lookupAsync(String broker) {
        this.checkState();
        return brokerLookupDataLockManager.readLock(keyPath(broker));
    }

    public CompletableFuture<Map<String, BrokerLookupData>> getAvailableBrokerLookupDataAsync() {
        this.checkState();
        return this.getAvailableBrokersAsync().thenCompose(availableBrokers -> {
            Map<String, BrokerLookupData> map = new ConcurrentHashMap<>();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String brokerId : availableBrokers) {
                futures.add(this.lookupAsync(brokerId).thenAccept(lookupDataOpt -> {
                    if (lookupDataOpt.isPresent()) {
                        map.put(brokerId, lookupDataOpt.get());
                    } else {
                        log.warn("Got an empty lookup data, brokerId: {}", brokerId);
                    }
                }));
            }
            return FutureUtil.waitForAll(futures).thenApply(__ -> map);
        });
    }

    public synchronized void addListener(BiConsumer<String, NotificationType> listener) {
        this.checkState();
        this.listeners.add(listener);
    }

    @Override
    public synchronized void close() throws PulsarServerException {
        if (this.state == State.Closed) {
            return;
        }
        try {
            this.listeners.clear();
            this.unregister();
            this.brokerLookupDataLockManager.close();
        } catch (Exception ex) {
            if (ex.getCause() instanceof MetadataStoreException.NotFoundException) {
                throw new PulsarServerException.NotFoundException(MetadataStoreException.unwrap(ex));
            } else {
                throw new PulsarServerException(MetadataStoreException.unwrap(ex));
            }
        } finally {
            this.state = State.Closed;
        }
    }

    private void handleMetadataStoreNotification(Notification t) {
        if (!this.isStarted() || !isVerifiedNotification(t)) {
            return;
        }
        try {
            if (log.isDebugEnabled()) {
                log.debug("Handle notification: [{}]", t);
            }
            if (listeners.isEmpty()) {
                return;
            }
            this.scheduler.submit(() -> {
                String brokerId = t.getPath().substring(LOOKUP_DATA_PATH.length() + 1);
                for (BiConsumer<String, NotificationType> listener : listeners) {
                    listener.accept(brokerId, t.getType());
                }
            });
        } catch (RejectedExecutionException e) {
            // Executor is shutting down
        }
    }

    @VisibleForTesting
    protected static boolean isVerifiedNotification(Notification t) {
       return t.getPath().startsWith(LOOKUP_DATA_PATH + "/") && t.getPath().length() > LOOKUP_DATA_PATH.length() + 1;
    }

    @VisibleForTesting
    protected static String keyPath(String brokerId) {
        return String.format("%s/%s", LOOKUP_DATA_PATH, brokerId);
    }

    private void checkState() throws IllegalStateException {
        if (this.state == State.Closed) {
            throw new IllegalStateException("The registry already closed.");
        }
    }
}

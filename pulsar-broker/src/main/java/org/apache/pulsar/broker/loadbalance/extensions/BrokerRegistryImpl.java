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

import static org.apache.pulsar.broker.loadbalance.LoadManager.LOADBALANCE_BROKERS_ROOT;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.CreateOption;

/**
 * The broker registry impl, base on the LockManager.
 */
@Slf4j
public class BrokerRegistryImpl implements BrokerRegistry {

    private static final int MAX_REGISTER_RETRY_DELAY_IN_MILLIS = 1000;

    private final PulsarService pulsar;

    private final ServiceConfiguration conf;

    private final BrokerLookupData brokerLookupData;

    private final MetadataCache<BrokerLookupData> brokerLookupDataMetadataCache;

    private final String brokerIdKeyPath;

    private final ScheduledExecutorService scheduler;

    private final List<BiConsumer<String, NotificationType>> listeners;

    protected enum State {
        Init,
        Started,
        Registered,
        Unregistering,
        Closed
    }

    @VisibleForTesting
    final AtomicReference<State> state = new AtomicReference<>(State.Init);

    @VisibleForTesting
    BrokerRegistryImpl(PulsarService pulsar, MetadataCache<BrokerLookupData> brokerLookupDataMetadataCache) {
        this.pulsar = pulsar;
        this.conf = pulsar.getConfiguration();
        this.brokerLookupDataMetadataCache = brokerLookupDataMetadataCache;
        this.scheduler = pulsar.getLoadManagerExecutor();
        this.listeners = new ArrayList<>();
        this.brokerIdKeyPath = keyPath(pulsar.getBrokerId());
        this.brokerLookupData = new BrokerLookupData(
                pulsar.getWebServiceAddress(),
                pulsar.getWebServiceAddressTls(),
                pulsar.getBrokerServiceUrl(),
                pulsar.getBrokerServiceUrlTls(),
                pulsar.getAdvertisedListeners(),
                pulsar.getProtocolDataToAdvertise(),
                pulsar.getConfiguration().isEnablePersistentTopics(),
                pulsar.getConfiguration().isEnableNonPersistentTopics(),
                conf.getLoadManagerClassName(),
                System.currentTimeMillis(),
                pulsar.getBrokerVersion(),
                pulsar.getConfig().lookupProperties());
    }

    public BrokerRegistryImpl(PulsarService pulsar) {
        this(pulsar, pulsar.getLocalMetadataStore().getMetadataCache(BrokerLookupData.class));
    }

    @Override
    public synchronized void start() throws PulsarServerException {
        if (!this.state.compareAndSet(State.Init, State.Started)) {
            throw new PulsarServerException("Cannot start the broker registry in state " + state.get());
        }
        pulsar.getLocalMetadataStore().registerListener(this::handleMetadataStoreNotification);
        try {
            this.registerAsync().get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw PulsarServerException.from(e);
        }
    }

    @Override
    public boolean isStarted() {
        final var state = this.state.get();
        return state == State.Started || state == State.Registered;
    }

    @Override
    public boolean isRegistered() {
        final var state = this.state.get();
        return state == State.Registered;
    }

    @Override
    public CompletableFuture<Void> registerAsync() {
        final var state = this.state.get();
        if (state != State.Started && state != State.Registered) {
            log.info("[{}] Skip registering self because the state is {}", getBrokerId(), state);
            return CompletableFuture.completedFuture(null);
        }
        log.info("[{}] Started registering self to {} (state: {})", getBrokerId(), brokerIdKeyPath, state);
        return brokerLookupDataMetadataCache.put(brokerIdKeyPath, brokerLookupData, EnumSet.of(CreateOption.Ephemeral))
                .orTimeout(pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS)
                .whenComplete((__, ex) -> {
                    if (ex == null) {
                        this.state.set(State.Registered);
                        log.info("[{}] Finished registering self", getBrokerId());
                    } else {
                        log.error("[{}] Failed registering self", getBrokerId(), ex);
                    }
                });
    }

    private void doRegisterAsyncWithRetries(int retry, CompletableFuture<Void> future) {
        pulsar.getExecutor().schedule(() -> {
            registerAsync().whenComplete((__, e) -> {
                if (e != null) {
                    doRegisterAsyncWithRetries(retry + 1, future);
                } else {
                    future.complete(null);
                }
            });
        }, Math.min(MAX_REGISTER_RETRY_DELAY_IN_MILLIS, retry * retry * 50), TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Void> registerAsyncWithRetries() {
        var retryFuture = new CompletableFuture<Void>();
        doRegisterAsyncWithRetries(0, retryFuture);
        return retryFuture;
    }

    @Override
    public synchronized void unregister() throws MetadataStoreException {
        if (state.compareAndSet(State.Registered, State.Unregistering)) {
            try {
                brokerLookupDataMetadataCache.delete(brokerIdKeyPath)
                        .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof MetadataStoreException.NotFoundException) {
                    log.warn("{} has already been unregistered", brokerIdKeyPath);
                } else {
                    throw MetadataStoreException.unwrap(e);
                }
            } catch (InterruptedException | TimeoutException e) {
                throw MetadataStoreException.unwrap(e);
            } finally {
                state.set(State.Started);
            }
        }
    }

    @Override
    public String getBrokerId() {
        return pulsar.getBrokerId();
    }

    @Override
    public CompletableFuture<List<String>> getAvailableBrokersAsync() {
        this.checkState();
        return brokerLookupDataMetadataCache.getChildren(LOADBALANCE_BROKERS_ROOT);
    }

    @Override
    public CompletableFuture<Optional<BrokerLookupData>> lookupAsync(String broker) {
        this.checkState();
        return brokerLookupDataMetadataCache.get(keyPath(broker));
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
        if (this.state.get() == State.Closed) {
            return;
        }
        try {
            this.listeners.clear();
            this.unregister();
        } catch (Exception ex) {
            log.error("Unexpected error when unregistering the broker registry", ex);
        } finally {
            this.state.set(State.Closed);
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
            // The registered node is an ephemeral node that could be deleted when the metadata store client's session
            // is expired. In this case, we should register again.
            final var brokerId = t.getPath().substring(LOADBALANCE_BROKERS_ROOT.length() + 1);

            CompletableFuture<Void> register;
            if (t.getType() == NotificationType.Deleted && getBrokerId().equals(brokerId)) {
                this.state.set(State.Started);
                register = registerAsyncWithRetries();
            } else {
                register = CompletableFuture.completedFuture(null);
            }
            // Make sure to run the listeners after re-registered.
            register.thenAccept(__ -> {
                if (listeners.isEmpty()) {
                    return;
                }
                this.scheduler.submit(() -> {
                    for (BiConsumer<String, NotificationType> listener : listeners) {
                        listener.accept(brokerId, t.getType());
                    }
                });
            });

        } catch (RejectedExecutionException e) {
            // Executor is shutting down
        }
    }

    @VisibleForTesting
    protected static boolean isVerifiedNotification(Notification t) {
       return t.getPath().startsWith(LOADBALANCE_BROKERS_ROOT + "/")
               && t.getPath().length() > LOADBALANCE_BROKERS_ROOT.length() + 1;
    }

    @VisibleForTesting
    protected static String keyPath(String brokerId) {
        return String.format("%s/%s", LOADBALANCE_BROKERS_ROOT, brokerId);
    }

    private void checkState() throws IllegalStateException {
        if (this.state.get() == State.Closed) {
            throw new IllegalStateException("The registry already closed.");
        }
    }
}

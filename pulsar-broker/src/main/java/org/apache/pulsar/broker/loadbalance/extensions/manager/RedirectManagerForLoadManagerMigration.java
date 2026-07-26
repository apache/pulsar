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
package org.apache.pulsar.broker.loadbalance.extensions.manager;

import static org.apache.pulsar.broker.loadbalance.LoadManager.LOADBALANCE_BROKERS_ROOT;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.coordination.LockManager;

/**
 * RedirectManagerForLoadManagerMigration checks if the current load manager class name is the same as the latest
 * service lookup data.
 * If not, it will redirect to a random broker with the same load manager class name.
 * The name of this class is misleading since it doesn't manage all redirects.
 */
@CustomLog
public class RedirectManagerForLoadManagerMigration {
    private final PulsarService pulsar;

    private final LockManager<BrokerLookupData> brokerLookupDataLockManager;


    public RedirectManagerForLoadManagerMigration(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.brokerLookupDataLockManager = pulsar.getCoordinationService().getLockManager(BrokerLookupData.class);
    }

    @VisibleForTesting
    public RedirectManagerForLoadManagerMigration(PulsarService pulsar,
                                                  LockManager<BrokerLookupData> brokerLookupDataLockManager) {
        this.pulsar = pulsar;
        this.brokerLookupDataLockManager = brokerLookupDataLockManager;
    }

    public CompletableFuture<Map<String, BrokerLookupData>> getAvailableBrokerLookupDataAsync() {
        return brokerLookupDataLockManager.listLocks(LOADBALANCE_BROKERS_ROOT).thenCompose(availableBrokers -> {
            Map<String, BrokerLookupData> map = new ConcurrentHashMap<>();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String brokerId : availableBrokers) {
                futures.add(this.brokerLookupDataLockManager.readLock(
                        String.format("%s/%s", LOADBALANCE_BROKERS_ROOT, brokerId)).thenAccept(lookupDataOpt -> {
                    if (lookupDataOpt.isPresent()) {
                        map.put(brokerId, lookupDataOpt.get());
                    } else {
                        log.warn().attr("broker", brokerId).log("Got an empty lookup data, brokerId");
                    }
                }));
            }

            return FutureUtil.waitForAll(futures).thenApply(__ -> map);
        });
    }

    /**
     * Redirect the request to another broker if the load balancer on the current broker is using the load manager
     * of the latest service lookup data available in the metadata store.
     *
     * @param options lookup options
     * @return lookup result
     */
    public CompletableFuture<Optional<LookupResult>> redirectIfLoadBalancerOnBrokerIsNotExpected(
            LookupOptions options) {
        if (!pulsar.getConfiguration().isLoadManagerMigrationEnabled()) {
            // no-op when load manager migration is disabled.
            return CompletableFuture.completedFuture(Optional.empty());
        }
        String currentLMClassName = pulsar.getConfiguration().getLoadManagerClassName();
        boolean debug = ExtensibleLoadManagerImpl.debug(pulsar.getConfiguration(), log);
        return getAvailableBrokerLookupDataAsync().thenApply(lookupDataMap -> {
            if (lookupDataMap.isEmpty()) {
                String errorMsg = "No available broker found.";
                log.warn(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            AtomicReference<BrokerLookupData> latestServiceLookupData = new AtomicReference<>();
            AtomicLong lastStartTimestamp = new AtomicLong(0L);
            lookupDataMap.forEach((key, value) -> {
                if (lastStartTimestamp.get() <= value.getStartTimestamp()) {
                    lastStartTimestamp.set(value.getStartTimestamp());
                    latestServiceLookupData.set(value);
                }
            });
            if (latestServiceLookupData.get() == null) {
                String errorMsg = "No latest service lookup data found.";
                log.warn(errorMsg);
                throw new IllegalStateException(errorMsg);
            }

            if (Objects.equals(latestServiceLookupData.get().getLoadManagerClassName(), currentLMClassName)) {
                if (debug) {
                    log.info().attr("name", currentLMClassName)
                            .log("No need to redirect, current load manager class name");
                }
                return Optional.empty();
            }
            var serviceLookupDataObj = latestServiceLookupData.get();
            var candidateBrokers = new ArrayList<BrokerLookupData>();
            lookupDataMap.forEach((key, value) -> {
                if (Objects.equals(value.getLoadManagerClassName(), serviceLookupDataObj.getLoadManagerClassName())) {
                    candidateBrokers.add(value);
                }
            });
            var selectedBroker = candidateBrokers.get((int) (Math.random() * candidateBrokers.size()));

            return Optional.of(selectedBroker.toLoadManagerMigrationLookupResult(options));
        });
    }
}

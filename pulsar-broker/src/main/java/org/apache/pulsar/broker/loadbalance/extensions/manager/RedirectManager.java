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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;

@Slf4j
public class RedirectManager {
    private final PulsarService pulsar;

    private final LockManager<BrokerLookupData> brokerLookupDataLockManager;


    public RedirectManager(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.brokerLookupDataLockManager = pulsar.getCoordinationService().getLockManager(BrokerLookupData.class);
    }

    @VisibleForTesting
    public RedirectManager(PulsarService pulsar, LockManager<BrokerLookupData> brokerLookupDataLockManager) {
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
                        log.warn("Got an empty lookup data, brokerId: {}", brokerId);
                    }
                }));
            }

            return FutureUtil.waitForAll(futures).thenApply(__ -> map);
        });
    }

    public CompletableFuture<Optional<LookupResult>> findRedirectLookupResultAsync() {
        String currentLMClassName = pulsar.getConfiguration().getLoadManagerClassName();
        boolean debug = ExtensibleLoadManagerImpl.debug(pulsar.getConfiguration(), log);
        return getAvailableBrokerLookupDataAsync().thenApply(lookupDataMap -> {
            if (lookupDataMap.isEmpty()) {
                String errorMsg = "No available broker found.";
                log.warn(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            AtomicReference<ServiceLookupData> latestServiceLookupData = new AtomicReference<>();
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
                    log.info("No need to redirect, current load manager class name: {}",
                            currentLMClassName);
                }
                return Optional.empty();
            }
            var serviceLookupDataObj = latestServiceLookupData.get();
            var candidateBrokers = new ArrayList<ServiceLookupData>();
            lookupDataMap.forEach((key, value) -> {
                if (Objects.equals(value.getLoadManagerClassName(), serviceLookupDataObj.getLoadManagerClassName())) {
                    candidateBrokers.add(value);
                }
            });
            var selectedBroker = candidateBrokers.get((int) (Math.random() * candidateBrokers.size()));

            return Optional.of(new LookupResult(selectedBroker.getWebServiceUrl(),
                    selectedBroker.getWebServiceUrlTls(),
                    selectedBroker.getPulsarServiceUrl(),
                    selectedBroker.getPulsarServiceUrlTls(),
                    true));
        });
    }

}

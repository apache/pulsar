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

package org.apache.pulsar.broker.loadbalance.extensions.channel;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.metadata.api.MetadataStoreException;

/**
 * ServiceUnitStateTableView base class.
 */
@Slf4j
abstract class ServiceUnitStateTableViewBase implements ServiceUnitStateTableView {
    protected static final String INVALID_STATE_ERROR_MSG = "The tableview has not been started.";
    private final Map<NamespaceBundle, Boolean> ownedServiceUnitsMap = new ConcurrentHashMap<>();
    private final Set<NamespaceBundle> ownedServiceUnits = Collections.unmodifiableSet(ownedServiceUnitsMap.keySet());
    private String brokerId;
    private PulsarService pulsar;
    protected void init(PulsarService pulsar) throws MetadataStoreException {
        this.pulsar = pulsar;
        this.brokerId = pulsar.getBrokerId();
        // Add heartbeat and SLA monitor namespace bundle.
        NamespaceName heartbeatNamespace =
                NamespaceService.getHeartbeatNamespace(brokerId, pulsar.getConfiguration());
        NamespaceName heartbeatNamespaceV2 = NamespaceService
                .getHeartbeatNamespaceV2(brokerId, pulsar.getConfiguration());
        NamespaceName slaMonitorNamespace = NamespaceService
                .getSLAMonitorNamespace(brokerId, pulsar.getConfiguration());
        try {
            pulsar.getNamespaceService().getNamespaceBundleFactory()
                    .getFullBundleAsync(heartbeatNamespace)
                    .thenAccept(fullBundle -> ownedServiceUnitsMap.put(fullBundle, true))
                    .thenCompose(__ -> pulsar.getNamespaceService().getNamespaceBundleFactory()
                            .getFullBundleAsync(heartbeatNamespaceV2))
                    .thenAccept(fullBundle -> ownedServiceUnitsMap.put(fullBundle, true))
                    .thenCompose(__ -> pulsar.getNamespaceService().getNamespaceBundleFactory()
                            .getFullBundleAsync(slaMonitorNamespace))
                    .thenAccept(fullBundle -> ownedServiceUnitsMap.put(fullBundle, true))
                    .thenApply(__ -> null).get(pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds(),
                            TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new MetadataStoreException(e);
        }
    }

    @Override
    public Set<NamespaceBundle> ownedServiceUnits() {
        return ownedServiceUnits;
    }

    protected void updateOwnedServiceUnits(String key, ServiceUnitStateData val) {
        NamespaceBundle namespaceBundle = LoadManagerShared.getNamespaceBundle(pulsar, key);
        var state = ServiceUnitStateData.state(val);
        ownedServiceUnitsMap.compute(namespaceBundle, (k, v) -> {
            if (state == Owned && brokerId.equals(val.dstBroker())) {
                return true;
            } else if (state == Splitting && brokerId.equals(val.sourceBroker())) {
                return true;
            } else {
                return null;
            }
        });
    }
}

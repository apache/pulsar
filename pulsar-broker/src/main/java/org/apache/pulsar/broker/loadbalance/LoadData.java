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
package org.apache.pulsar.broker.loadbalance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceBundle;

/**
 * This class represents all data that could be relevant when making a load management decision.
 */
public class LoadData {
    /**
     * Map from broker names to their available data.
     */
    private final Map<String, BrokerData> brokerData;

    /**
     * Map from bundle names to their time-sensitive aggregated data.
     */
    private final Map<String, BundleData> bundleData;

    /**
     * Map from recently unloaded bundles to the timestamp of when they were last loaded.
     */
    private final Map<String, Long> recentlyUnloadedBundles;

    /**
     * Initialize a LoadData.
     */
    public LoadData() {
        this.brokerData = new ConcurrentHashMap<>();
        this.bundleData = new ConcurrentHashMap<>();
        this.recentlyUnloadedBundles = new ConcurrentHashMap<>();
    }

    public Map<String, BrokerData> getBrokerData() {
        return brokerData;
    }

    public Map<String, BundleData> getBundleData() {
        return bundleData;
    }

    public Map<String, BundleData> getBundleDataForLoadShedding() {
        return bundleData.entrySet().stream()
                .filter(e -> !NamespaceService.isSystemServiceNamespace(
                        NamespaceBundle.getBundleNamespace(e.getKey())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Map<String, Long> getRecentlyUnloadedBundles() {
        return recentlyUnloadedBundles;
    }
}

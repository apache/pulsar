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
package org.apache.pulsar.broker.loadbalance.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * The cache for the bundle ranges.
 * The first key is the broker id and the second key is the namespace name, the value is the set of bundle ranges of
 * that namespace. When the broker key is accessed if the associated value is not present, an empty map will be created
 * as the initial value that will never be removed.
 * Therefore, for each broker, there could only be one internal map during the whole lifetime. Then it will be safe
 * to apply the synchronized key word on the value for thread safe operations.
 */
public class BundleRangeCache {

    // Map from brokers to namespaces to the bundle ranges in that namespace assigned to that broker.
    // Used to distribute bundles within a namespace evenly across brokers.
    private final Map<String, Map<String, Set<String>>> data = new ConcurrentHashMap<>();

    public void reloadFromBundles(String broker, Stream<String> bundles) {
        final var namespaceToBundleRange = data.computeIfAbsent(broker, __ -> new HashMap<>());
        synchronized (namespaceToBundleRange) {
            namespaceToBundleRange.clear();
            bundles.forEach(bundleName -> {
                final String namespace = LoadManagerShared.getNamespaceNameFromBundleName(bundleName);
                final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundleName);
                namespaceToBundleRange.computeIfAbsent(namespace, __ -> new HashSet<>()).add(bundleRange);
            });
        }
    }

    public void add(String broker, String namespace, String bundleRange) {
        final var namespaceToBundleRange = data.computeIfAbsent(broker, __ -> new HashMap<>());
        synchronized (namespaceToBundleRange) {
            namespaceToBundleRange.computeIfAbsent(namespace, __ -> new HashSet<>()).add(bundleRange);
        }
    }

    public int getBundleRangeCount(String broker, String namespace) {
        final var namespaceToBundleRange = data.computeIfAbsent(broker, __ -> new HashMap<>());
        synchronized (namespaceToBundleRange) {
            final var bundleRangeSet = namespaceToBundleRange.get(namespace);
            return bundleRangeSet != null ? bundleRangeSet.size() : 0;
        }
    }

    /**
     * Get the map whose key is the broker and value is the namespace that has at least 1 cached bundle range.
     */
    public Map<String, List<String>> getBrokerToNamespacesMap() {
        final var brokerToNamespaces = new HashMap<String, List<String>>();
        for (var entry : data.entrySet()) {
            final var broker = entry.getKey();
            final var namespaceToBundleRange = entry.getValue();
            synchronized (namespaceToBundleRange) {
                brokerToNamespaces.put(broker, namespaceToBundleRange.keySet().stream().toList());
            }
        }
        return brokerToNamespaces;
    }
}

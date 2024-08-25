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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class BundleRangeCache {

    // Map from brokers to namespaces to the bundle ranges in that namespace assigned to that broker.
    // Used to distribute bundles within a namespace evenly across brokers.
    private final Map<String, Map<String, Set<String>>> data = new ConcurrentHashMap<>();

    public void reloadFromBundles(String broker, Stream<String> bundles) {
        final var namespaceToBundleRange = new ConcurrentHashMap<String, Set<String>>();
        bundles.forEach(bundleName -> {
            final String namespace = LoadManagerShared.getNamespaceNameFromBundleName(bundleName);
            final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundleName);
            initConcurrentHashSet(namespaceToBundleRange, namespace).add(bundleRange);
        });
        data.put(broker, namespaceToBundleRange);
    }

    public void addBundleRange(String broker, String namespace, String bundleRange) {
        getBundleRangeSet(broker, namespace).add(bundleRange);
    }

    public int getBundles(String broker, String namespace) {
        return getBundleRangeSet(broker, namespace).size();
    }

    public List<CompletableFuture<Void>> runTasks(
            BiFunction<String/* broker */, String/* namespace */, CompletableFuture<Void>> task) {
        return data.entrySet().stream().flatMap(e -> {
            final var broker = e.getKey();
            return e.getValue().entrySet().stream().filter(__ -> !__.getValue().isEmpty()).map(Map.Entry::getKey)
                    .map(namespace -> task.apply(broker, namespace));
        }).toList();
    }

    private Set<String> getBundleRangeSet(String broker, String namespace) {
        return initConcurrentHashSet(data.computeIfAbsent(broker, __ -> new ConcurrentHashMap<>()), namespace);
    }

    private static Set<String> initConcurrentHashSet(Map<String, Set<String>> namespaceToBundleRangeSet,
                                                     String namespace) {
        return namespaceToBundleRangeSet.computeIfAbsent(namespace, __ -> ConcurrentHashMap.newKeySet());
    }
}

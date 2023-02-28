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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;

/**
 * Simple Round Robin Broker Selection Strategy.
 */
public class RoundRobinBrokerSelector implements ModularLoadManagerStrategy {

    final AtomicInteger count = new AtomicInteger();
    final AtomicReference<List<String>> ref = new AtomicReference<>(List.of());

    @Override
    public Optional<String> selectBroker(Set<String> candidates, BundleData bundleToAssign, LoadData loadData,
                                         ServiceConfiguration conf) {
        int candidateSize = candidates.size();
        if (candidateSize == 0) {
            return Optional.empty();
        }

        var cache = ref.get();
        int cacheSize = cache.size();
        int index = count.getAndUpdate(i -> i == Integer.MAX_VALUE ? 0 : i + 1) % candidateSize;
        boolean updateCacheRef = false;

        if (cacheSize <= index || candidateSize != cacheSize) {
            cache = List.copyOf(candidates);
            updateCacheRef = true;
        }

        var selected = cache.get(index);
        if (!candidates.contains(selected)) {
            cache = List.copyOf(candidates);
            updateCacheRef = true;
            selected = cache.get(index);
        }

        if (updateCacheRef) {
            ref.set(cache);
        }

        return Optional.of(selected);
    }
}

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
package org.apache.pulsar.broker.loadbalance.extensions.data;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

/**
 * Defines the information of top bundles load data.
 */
@Getter
public class TopBundlesLoadData {

    private final List<BundleLoadData> topBundlesLoadData;

    public record BundleLoadData(String bundleName, NamespaceBundleStats stats) {
        public BundleLoadData {
            Objects.requireNonNull(bundleName);
        }
    }

    private TopBundlesLoadData(List<BundleLoadData> bundleStats, int topK) {
        topBundlesLoadData = bundleStats
                .stream()
                .sorted((o1, o2) -> o2.stats().compareTo(o1.stats()))
                .limit(topK)
                .collect(Collectors.toList());
    }

    /**
     * Give full bundle stats, and return the top K bundle stats.
     *
     * @param bundleStats full bundle stats.
     * @param topK Top K bundles.
     */
    public static TopBundlesLoadData of(List<BundleLoadData> bundleStats, int topK) {
        return new TopBundlesLoadData(bundleStats, topK);
    }
}

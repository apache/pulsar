/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.loadbalance;

import java.util.Map;

import com.yahoo.pulsar.broker.PulsarService;

/**
 * Load management component which determines the criteria for unloading bundles.
 */
public interface LoadSheddingStrategy {

    /**
     * Recommend that all of the returned bundles be unloaded.
     * 
     * @param loadData
     *            The load data to used to make the unloading decision.
     * @param pulsar
     *            Pulsar service to use.
     * @return A map from all selected bundles to the brokers on which they reside.
     */
    Map<String, String> findBundlesForUnloading(LoadData loadData, PulsarService pulsar);
}

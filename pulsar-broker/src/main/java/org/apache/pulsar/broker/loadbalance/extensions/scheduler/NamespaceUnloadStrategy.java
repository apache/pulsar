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
package org.apache.pulsar.broker.loadbalance.extensions.scheduler;

import java.util.Map;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;

/**
 * The namespace unload strategy.
 * Used to determine the criteria for unloading bundles.
 *
 * Migrate from {@link org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy}
 */
public interface NamespaceUnloadStrategy {

    /**
     * Recommend that all the returned bundles be unloaded.
     *
     * @param context                 The context used for decisions.
     * @param recentlyUnloadedBundles The recently unloaded bundles.
     * @param recentlyUnloadedBrokers The recently unloaded brokers.
     * @return unloadDecision containing a list of the bundles that should be unloaded.
     */
    UnloadDecision findBundlesForUnloading(LoadManagerContext context,
                                           Map<String, Long> recentlyUnloadedBundles,
                                           Map<String, Long> recentlyUnloadedBrokers);

}

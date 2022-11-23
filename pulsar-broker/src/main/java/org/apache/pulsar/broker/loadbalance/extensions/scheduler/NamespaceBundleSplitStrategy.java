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

import java.util.Set;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;

/**
 * Determines which bundles should be split based on various thresholds.
 *
 * Migrate from {@link org.apache.pulsar.broker.loadbalance.BundleSplitStrategy}
 */
public interface NamespaceBundleSplitStrategy {

    /**
     * Determines which bundles, if any, should be split.
     *
     * @param context The context used for decisions.
     * @return A set of the bundles that should be split.
     */
    Set<Split> findBundlesToSplit(LoadManagerContext context);
}

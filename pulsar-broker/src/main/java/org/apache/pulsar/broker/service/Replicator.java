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
package org.apache.pulsar.broker.service;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupDispatchLimiter;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;

public interface Replicator {

    void startProducer();

    ReplicatorStatsImpl getStats();

    CompletableFuture<Void> terminate();

    CompletableFuture<Void> disconnect(boolean failIfHasBacklog, boolean closeTheStartingProducer);

    void updateRates();

    String getRemoteCluster();

    default void initializeDispatchRateLimiterIfNeeded() {
        //No-op
    }

    default void updateRateLimiter() {
    }

    default Optional<DispatchRateLimiter> getRateLimiter() {
        return Optional.empty();
    }

    default Optional<ResourceGroupDispatchLimiter> getResourceGroupDispatchRateLimiter() {
        return Optional.empty();
    }

    boolean isConnected();

    long getNumberOfEntriesInBacklog();

    boolean isTerminated();
}

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
package org.apache.pulsar.client.admin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.policies.data.WorkerFunctionInstanceStats;
import org.apache.pulsar.common.stats.Metrics;

/**
 * Admin interface for worker stats management.
 */
public interface Worker {

    /**
     * Get all functions stats on a worker.
     * @return
     * @throws PulsarAdminException
     */
    List<WorkerFunctionInstanceStats> getFunctionsStats() throws PulsarAdminException;

    /**
     * Get all functions stats on a worker asynchronously.
     * @return
     */
    CompletableFuture<List<WorkerFunctionInstanceStats>> getFunctionsStatsAsync();

    /**
     * Get worker metrics.
     * @return
     * @throws PulsarAdminException
     */
    Collection<Metrics> getMetrics() throws PulsarAdminException;

    /**
     * Get worker metrics asynchronously.
     * @return
     */
    CompletableFuture<Collection<Metrics>> getMetricsAsync();

    /**
     * Get List of all workers belonging to this cluster.
     * @return
     * @throws PulsarAdminException
     */
    List<WorkerInfo> getCluster() throws PulsarAdminException;

    /**
     * Get List of all workers belonging to this cluster asynchronously.
     * @return
     */
    CompletableFuture<List<WorkerInfo>> getClusterAsync();

    /**
     * Get the worker who is the leader of the cluster.
     * @return
     * @throws PulsarAdminException
     */
    WorkerInfo getClusterLeader() throws PulsarAdminException;

    /**
     * Get the worker who is the leader of the cluster asynchronously.
     * @return
     */
    CompletableFuture<WorkerInfo> getClusterLeaderAsync();

    /**
     * Get the function assignment among the cluster.
     * @return
     * @throws PulsarAdminException
     */
    Map<String, Collection<String>> getAssignments() throws PulsarAdminException;

    /**
     * Get the function assignment among the cluster asynchronously.
     * @return
     */
    CompletableFuture<Map<String, Collection<String>>> getAssignmentsAsync();
}

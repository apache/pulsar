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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.common.stats.AllocatorStats;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;

/**
 * Admin interface for brokers management.
 */
public interface BrokerStats {

    /**
     * Returns Monitoring metrics.
     *
     * @return
     * @throws PulsarAdminException
     */

    JsonArray getMetrics() throws PulsarAdminException;

    /**
     * Returns Monitoring metrics asynchronously.
     *
     * @return
     */

    CompletableFuture<JsonArray> getMetricsAsync();

    /**
     * Requests JSON string server mbean dump.
     * <p/>
     * Notes: since we don't plan to introspect the response we avoid converting the response into POJO.
     *
     * @return
     * @throws PulsarAdminException
     */
    JsonArray getMBeans() throws PulsarAdminException;

    /**
     * Requests JSON string server mbean dump asynchronously.
     * <p/>
     * Notes: since we don't plan to introspect the response we avoid converting the response into POJO.
     *
     * @return
     */
    CompletableFuture<JsonArray> getMBeansAsync();

    /**
     * Returns JSON string topics stats.
     * <p/>
     * Notes: since we don't plan to introspect the response we avoid converting the response into POJO.
     *
     * @return
     * @throws PulsarAdminException
     */
    JsonObject getTopics() throws PulsarAdminException;

    /**
     * Returns JSON string topics stats asynchronously.
     * <p/>
     * Notes: since we don't plan to introspect the response we avoid converting the response into POJO.
     *
     * @return
     */
    CompletableFuture<JsonObject> getTopicsAsync();

    /**
     * Get pending bookie client op stats by namespace.
     * <p/>
     * Notes: since we don't plan to introspect the response we avoid converting the response into POJO.
     *
     * @return
     * @throws PulsarAdminException
     */
    JsonObject getPendingBookieOpsStats() throws PulsarAdminException;

    /**
     * Get pending bookie client op stats by namespace asynchronously.
     * <p/>
     * Notes: since we don't plan to introspect the response we avoid converting the response into POJO.
     *
     * @return
     */
    CompletableFuture<JsonObject> getPendingBookieOpsStatsAsync();

    /**
     * Get the stats for the Netty allocator.
     *
     * @param allocatorName
     * @return
     * @throws PulsarAdminException
     */
    AllocatorStats getAllocatorStats(String allocatorName) throws PulsarAdminException;

    /**
     * Get the stats for the Netty allocator asynchronously.
     *
     * @param allocatorName
     * @return
     */
    CompletableFuture<AllocatorStats> getAllocatorStatsAsync(String allocatorName);

    /**
     * Get load for this broker.
     *
     * @return
     * @throws PulsarAdminException
     */
    LoadManagerReport getLoadReport() throws PulsarAdminException;

    /**
     * Get load for this broker asynchronously.
     *
     * @return
     */
    CompletableFuture<LoadManagerReport> getLoadReportAsync();
}

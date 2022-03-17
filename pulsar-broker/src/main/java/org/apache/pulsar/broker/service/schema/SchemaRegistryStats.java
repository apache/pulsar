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
package org.apache.pulsar.broker.service.schema;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import java.util.concurrent.atomic.AtomicBoolean;

class SchemaRegistryStats implements AutoCloseable {
    private static final String SCHEMA_ID = "schema";
    private static final String STATUS = "status";
    private static final String CLUSTER = "cluster";
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_FAILED = "failed";
    private static final double[] QUANTILES = {0.50, 0.75, 0.95, 0.99};
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);

    private final Counter delSchemaOps;
    private final Counter getSchemaOps;
    private final Counter putSchemaOps;

    private final Counter schemaIncompatibleCount;
    private final Counter schemaCompatibleCount;

    private final Summary delSchemaLatency;
    private final Summary getSchemaLatency;
    private final Summary putSchemaLatency;
    private final String clusterName;

    private static volatile SchemaRegistryStats instance;

    static synchronized SchemaRegistryStats getInstance(final int interval, final String cluster) {
        if (null == instance) {
            instance = new SchemaRegistryStats(interval, cluster);
        }

        return instance;
    }

    private SchemaRegistryStats(final int interval, final String cluster) {
        this.clusterName = cluster;

        this.delSchemaOps = Counter.build("pulsar_schema_del_ops_count", "-")
                .labelNames(CLUSTER, SCHEMA_ID, STATUS)
                .create()
                .register();
        this.getSchemaOps = Counter.build("pulsar_schema_get_ops_count", "-")
                .labelNames(CLUSTER, SCHEMA_ID, STATUS)
                .create()
                .register();
        this.putSchemaOps = Counter.build("pulsar_schema_put_ops_count", "-")
                .labelNames(CLUSTER, SCHEMA_ID, STATUS)
                .create()
                .register();

        this.schemaCompatibleCount = Counter.build("pulsar_schema_compatible_count", "-")
                .labelNames(CLUSTER, SCHEMA_ID)
                .create()
                .register();
        this.schemaIncompatibleCount = Counter.build("pulsar_schema_incompatible_count", "-")
                .labelNames(CLUSTER, SCHEMA_ID)
                .create()
                .register();

        this.delSchemaLatency = this.setQuantiles(
                Summary.build("pulsar_schema_del_ops_latency", "-")
                        .maxAgeSeconds(interval).labelNames(CLUSTER, SCHEMA_ID));
        this.getSchemaLatency = this.setQuantiles(
                Summary.build("pulsar_schema_get_ops_latency", "-")
                        .maxAgeSeconds(interval).labelNames(CLUSTER, SCHEMA_ID));
        this.putSchemaLatency = this.setQuantiles(
                Summary.build("pulsar_schema_put_ops_latency", "-")
                        .maxAgeSeconds(interval).labelNames(CLUSTER, SCHEMA_ID));
    }

    private Summary setQuantiles(Summary.Builder builder) {
        for (double quantile : QUANTILES) {
            builder.quantile(quantile, 0.01D);
        }
        return builder.create().register();
    }

    void recordDelSuccess(String schemaId) {
        this.delSchemaOps.labels(clusterName, schemaId, STATUS_SUCCESS).inc();
    }

    void recordDelFailed(String schemaId) {
        this.delSchemaOps.labels(clusterName, schemaId, STATUS_FAILED).inc();
    }

    void recordGetSuccess(String schemaId) {
        this.getSchemaOps.labels(clusterName, schemaId, STATUS_SUCCESS).inc();
    }

    void recordGetFailed(String schemaId) {
        this.getSchemaOps.labels(clusterName, schemaId, STATUS_FAILED).inc();
    }

    void recordPutSuccess(String schemaId) {
        this.putSchemaOps.labels(clusterName, schemaId, STATUS_SUCCESS).inc();
    }

    void recordPutFailed(String schemaId) {
        this.putSchemaOps.labels(clusterName, schemaId, STATUS_FAILED).inc();
    }

    void recordDelLatency(String schemaId, long millis) {
        this.delSchemaLatency.labels(clusterName, schemaId).observe(millis);
    }

    void recordGetLatency(String schemaId, long millis) {
        this.getSchemaLatency.labels(clusterName, schemaId).observe(millis);
    }

    void recordPutLatency(String schemaId, long millis) {
        this.putSchemaLatency.labels(clusterName, schemaId).observe(millis);
    }

    void recordSchemaIncompatible(String schemaId) {
        this.schemaIncompatibleCount.labels(clusterName, schemaId).inc();
    }

    void recordSchemaCompatible(String schemaId) {
        this.schemaCompatibleCount.labels(clusterName, schemaId).inc();
    }

    @Override
    public void close() throws Exception {
        if (CLOSED.compareAndSet(false, true)) {
            CollectorRegistry.defaultRegistry.unregister(this.delSchemaOps);
            CollectorRegistry.defaultRegistry.unregister(this.getSchemaOps);
            CollectorRegistry.defaultRegistry.unregister(this.putSchemaOps);
            CollectorRegistry.defaultRegistry.unregister(this.schemaCompatibleCount);
            CollectorRegistry.defaultRegistry.unregister(this.schemaIncompatibleCount);
            CollectorRegistry.defaultRegistry.unregister(this.delSchemaLatency);
            CollectorRegistry.defaultRegistry.unregister(this.getSchemaLatency);
            CollectorRegistry.defaultRegistry.unregister(this.putSchemaLatency);
        }
    }
}

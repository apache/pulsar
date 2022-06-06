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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.common.naming.TopicName;

class SchemaRegistryStats implements AutoCloseable, Runnable {
    private static final String NAMESPACE = "namespace";
    private static final double[] QUANTILES = {0.50, 0.75, 0.95, 0.99, 0.999, 0.9999, 1};
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);

    private final Counter getOpsFailedCounter;
    private final Counter putOpsFailedCounter;
    private final Counter deleteOpsFailedCounter;

    private final Counter compatibleCounter;
    private final Counter incompatibleCounter;

    private final Summary deleteOpsLatency;
    private final Summary getOpsLatency;
    private final Summary putOpsLatency;

    private final Map<String, Long> namespaceAccess = new ConcurrentHashMap<>();
    private ScheduledFuture<?> future;

    private static volatile SchemaRegistryStats instance;

    static synchronized SchemaRegistryStats getInstance(ScheduledExecutorService scheduler) {
        if (null == instance) {
            instance = new SchemaRegistryStats(scheduler);
        }

        return instance;
    }

    private SchemaRegistryStats(ScheduledExecutorService scheduler) {
        this.deleteOpsFailedCounter = Counter.build("pulsar_schema_del_ops_failed_count", "-")
                .labelNames(NAMESPACE).create().register();
        this.getOpsFailedCounter = Counter.build("pulsar_schema_get_ops_failed_count", "-")
                .labelNames(NAMESPACE).create().register();
        this.putOpsFailedCounter = Counter.build("pulsar_schema_put_ops_failed_count", "-")
                .labelNames(NAMESPACE).create().register();

        this.compatibleCounter = Counter.build("pulsar_schema_compatible_count", "-")
                .labelNames(NAMESPACE).create().register();
        this.incompatibleCounter = Counter.build("pulsar_schema_incompatible_count", "-")
                .labelNames(NAMESPACE).create().register();

        this.deleteOpsLatency = this.buildSummary("pulsar_schema_del_ops_latency", "-");
        this.getOpsLatency = this.buildSummary("pulsar_schema_get_ops_latency", "-");
        this.putOpsLatency = this.buildSummary("pulsar_schema_put_ops_latency", "-");

        if (null != scheduler) {
            this.future = scheduler.scheduleAtFixedRate(this, 1, 1, TimeUnit.MINUTES);
        }
    }

    private Summary buildSummary(String name, String help) {
        Summary.Builder builder = Summary.build(name, help).labelNames(NAMESPACE);

        for (double quantile : QUANTILES) {
            builder.quantile(quantile, 0.01D);
        }

        return builder.create().register();
    }

    void recordDelFailed(String schemaId) {
        this.deleteOpsFailedCounter.labels(getNamespace(schemaId)).inc();
    }

    void recordGetFailed(String schemaId) {
        this.getOpsFailedCounter.labels(getNamespace(schemaId)).inc();
    }

    void recordPutFailed(String schemaId) {
        this.putOpsFailedCounter.labels(getNamespace(schemaId)).inc();
    }

    void recordDelLatency(String schemaId, long millis) {
        this.deleteOpsLatency.labels(getNamespace(schemaId)).observe(millis);
    }

    void recordGetLatency(String schemaId, long millis) {
        this.getOpsLatency.labels(getNamespace(schemaId)).observe(millis);
    }

    void recordPutLatency(String schemaId, long millis) {
        this.putOpsLatency.labels(getNamespace(schemaId)).observe(millis);
    }

    void recordSchemaIncompatible(String schemaId) {
        this.incompatibleCounter.labels(getNamespace(schemaId)).inc();
    }

    void recordSchemaCompatible(String schemaId) {
        this.compatibleCounter.labels(getNamespace(schemaId)).inc();
    }


    private String getNamespace(String schemaId) {
        String namespace;
        try {
            namespace = TopicName.get(schemaId).getNamespace();
        } catch (IllegalArgumentException t) {
            namespace = "unknown";
        }

        this.namespaceAccess.put(namespace, System.currentTimeMillis());
        return namespace;
    }


    private void removeChild(String namespace) {
        getOpsFailedCounter.remove(namespace);
        putOpsFailedCounter.remove(namespace);
        deleteOpsFailedCounter.remove(namespace);
        compatibleCounter.remove(namespace);
        incompatibleCounter.remove(namespace);
        deleteOpsLatency.remove(namespace);
        getOpsLatency.remove(namespace);
        putOpsLatency.remove(namespace);
    }

    @Override
    public void close() throws Exception {
        if (CLOSED.compareAndSet(false, true)) {
            CollectorRegistry.defaultRegistry.unregister(this.deleteOpsFailedCounter);
            CollectorRegistry.defaultRegistry.unregister(this.getOpsFailedCounter);
            CollectorRegistry.defaultRegistry.unregister(this.putOpsFailedCounter);
            CollectorRegistry.defaultRegistry.unregister(this.compatibleCounter);
            CollectorRegistry.defaultRegistry.unregister(this.incompatibleCounter);
            CollectorRegistry.defaultRegistry.unregister(this.deleteOpsLatency);
            CollectorRegistry.defaultRegistry.unregister(this.getOpsLatency);
            CollectorRegistry.defaultRegistry.unregister(this.putOpsLatency);
            if (null != this.future) {
                this.future.cancel(false);
            }
        }
    }

    @Override
    public void run() {
        long now = System.currentTimeMillis();
        long interval = TimeUnit.MINUTES.toMillis(5);

        this.namespaceAccess.entrySet().removeIf(entry -> {
            String namespace = entry.getKey();
            long accessTime = entry.getValue();
            if (now - accessTime > interval) {
                this.removeChild(namespace);
                return true;
            }
            return false;
        });
    }
}

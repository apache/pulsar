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
package org.apache.bookkeeper.mledger;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.pulsar.common.naming.TopicName;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Management Bean for a {@link LedgerOffloader}.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public class LedgerOffloaderStats {
    private static final String TOPIC_LABEL = "topic";
    private static final String NAMESPACE_LABEL = "namespace";

    private Counter offloadError;
    private Counter offloadBytes;
    private Summary readLedgerLatency;
    private Counter writeStorageError;
    private Counter readOffloadError;
    private Counter readOffloadBytes;
    private Summary readOffloadIndexLatency;
    private Summary readOffloadDataLatency;

    private Map<String, String> topic2Namespace;
    private final boolean exposeLedgerMetrics;
    private final boolean exposeTopicLevelMetrics;

    private static volatile LedgerOffloaderStats instance;
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

    private LedgerOffloaderStats(boolean exposeLedgerMetrics, boolean exposeTopicLevelMetrics) {
        this.exposeLedgerMetrics = exposeLedgerMetrics;
        this.exposeTopicLevelMetrics = exposeTopicLevelMetrics;
        if (!exposeLedgerMetrics) {
            return;
        }

        this.topic2Namespace = new ConcurrentHashMap<>();
        String[] labels = exposeTopicLevelMetrics
                ? new String[]{NAMESPACE_LABEL, TOPIC_LABEL} : new String[]{NAMESPACE_LABEL};

        this.offloadError = Counter.build("brk_ledgeroffloader_offload_error", "")
                .labelNames(labels).create().register();
        this.offloadBytes = Counter.build("brk_ledgeroffloader_offload_bytes", "-")
                .labelNames(labels).create().register();
        this.readOffloadError = Counter.build("brk_ledgeroffloader_read_offload_error", "-")
                .labelNames(labels).create().register();
        this.readOffloadBytes = Counter.build("brk_ledgeroffloader_read_offload_bytes", "-")
                .labelNames(labels).create().register();
        this.writeStorageError = Counter.build("brk_ledgeroffloader_write_storage_error", "-")
                .labelNames(labels).create().register();

        this.readOffloadIndexLatency = Summary.build("brk_ledgeroffloader_read_offload_index_latency", "-")
                .labelNames(labels).create().register();
        this.readOffloadDataLatency = Summary.build("brk_ledgeroffloader_read_offload_data_latency", "-")
                .labelNames(labels).create().register();
        this.readLedgerLatency = Summary.build("brk_ledgeroffloader_read_ledger_latency", "-")
                .labelNames(labels).create().register();
    }

    public void recordOffloadError(String topic) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String namespace = this.getNamespace(topic);
        if (exposeTopicLevelMetrics) {
            this.offloadError.labels(namespace, topic).inc();
        } else {
            this.offloadError.labels(namespace).inc();
        }
    }

    public void recordOffloadBytes(String topic, long size) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String namespace = this.getNamespace(topic);
        if (exposeTopicLevelMetrics) {
            this.offloadBytes.labels(namespace, topic).inc(size);
        } else {
            this.offloadBytes.labels(namespace).inc(size);
        }
    }

    public void recordReadLedgerLatency(String topic, long latency, TimeUnit unit) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String namespace = this.getNamespace(topic);
        if (exposeTopicLevelMetrics) {
            this.readLedgerLatency.labels(namespace, topic).observe(unit.toMicros(latency));
        } else {
            this.readLedgerLatency.labels(namespace).observe(unit.toMicros(latency));
        }
    }

    public void recordWriteToStorageError(String topic) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String namespace = this.getNamespace(topic);
        if (exposeTopicLevelMetrics) {
            this.writeStorageError.labels(namespace, topic).inc();
        } else {
            this.writeStorageError.labels(namespace).inc();
        }
    }

    public void recordReadOffloadError(String topic) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String namespace = this.getNamespace(topic);
        if (exposeTopicLevelMetrics) {
            this.readOffloadError.labels(namespace, topic).inc();
        } else {
            this.readOffloadError.labels(namespace).inc();
        }
    }

    public void recordReadOffloadBytes(String topic, long size) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String namespace = this.getNamespace(topic);
        if (exposeTopicLevelMetrics) {
            this.readOffloadBytes.labels(namespace, topic).inc(size);
        } else {
            this.readOffloadBytes.labels(namespace).inc(size);
        }
    }

    public void recordReadOffloadIndexLatency(String topic, long latency, TimeUnit unit) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String namespace = this.getNamespace(topic);
        if (exposeTopicLevelMetrics) {
            this.readOffloadIndexLatency.labels(namespace, topic).observe(unit.toMicros(latency));
        } else {
            this.readOffloadIndexLatency.labels(namespace).observe(unit.toMicros(latency));
        }
    }

    public void recordReadOffloadDataLatency(String topic, long latency, TimeUnit unit) {
        if (!exposeLedgerMetrics) {
            return;
        }

        String namespace = this.getNamespace(topic);
        if (exposeTopicLevelMetrics) {
            this.readOffloadDataLatency.labels(namespace, topic).observe(unit.toMicros(latency));
        } else {
            this.readOffloadDataLatency.labels(namespace).observe(unit.toMicros(latency));
        }
    }


    private String getNamespace(String topic) {
        return this.topic2Namespace.computeIfAbsent(topic, __ -> TopicName.get(topic).getNamespace());
    }


    public static void initialize(boolean exposeLedgerMetrics,
                                  boolean exposeTopicLevelMetrics) {
        if (INITIALIZED.compareAndSet(false, true)) {
            instance = new LedgerOffloaderStats(exposeLedgerMetrics, exposeTopicLevelMetrics);
        }
    }

    public static LedgerOffloaderStats getInstance() {
        if (instance == null) {
            throw new IllegalStateException("Ledger offloader stats is not initialized");
        }

        return instance;
    }
}

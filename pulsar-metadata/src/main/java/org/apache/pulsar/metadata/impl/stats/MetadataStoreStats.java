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
package org.apache.pulsar.metadata.impl.stats;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MetadataStoreStats {
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final double[] BUCKETS = new double[]{1, 3, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000};

    private final Histogram getOpsLatency;
    private final Histogram delOpsLatency;
    private final Histogram putOpsLatency;
    private final Counter getFailedCounter;
    private final Counter delFailedCounter;
    private final Counter putFailedCounter;
    private final Counter putBytesCounter;

    private MetadataStoreStats() {
        getOpsLatency = Histogram.build("pulsar_metadata_store_get_ops_latency", "-")
                .buckets(BUCKETS)
                .register();
        delOpsLatency = Histogram.build("pulsar_metadata_store_del_ops_latency", "-")
                .buckets(BUCKETS)
                .register();
        putOpsLatency = Histogram.build("pulsar_metadata_store_put_ops_latency", "-")
                .buckets(BUCKETS)
                .register();

        getFailedCounter = Counter.build("pulsar_metadata_store_get_ops_failed", "-")
                .register();
        delFailedCounter = Counter.build("pulsar_metadata_store_del_ops_failed", "-")
                .register();
        putFailedCounter = Counter.build("pulsar_metadata_store_put_ops_failed", "-")
                .register();
        putBytesCounter = Counter.build("pulsar_metadata_store_put_bytes", "-")
                .register();
    }

    public void recordGetOpsLatency(long millis) {
        this.getOpsLatency.observe(millis);
    }

    public void recordDelOpsLatency(long millis) {
        this.delOpsLatency.observe(millis);
    }

    public void recordPutOpsLatency(long millis, int bytes) {
        this.putOpsLatency.observe(millis);
        this.putBytesCounter.inc(bytes);
    }

    public void recordGetOpsFailed() {
        this.getFailedCounter.inc();
    }

    public void recordDelOpsFailed() {
        this.delFailedCounter.inc();
    }

    public void recordPutOpsFailed() {
        this.putFailedCounter.inc();
    }

    private static MetadataStoreStats instance;

    public static MetadataStoreStats create() {
        if (INITIALIZED.compareAndSet(false, true)) {
            instance = new MetadataStoreStats();
        }

        return instance;
    }
}

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
package org.apache.pulsar.metadata.impl.stats;

import io.prometheus.metrics.core.datapoints.DistributionDataPoint;
import io.prometheus.metrics.core.metrics.Histogram;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BatchMetadataStoreStats implements AutoCloseable {
    private static final double[] BUCKETS = new double[]{1, 5, 10, 20, 50, 100, 200, 500, 1000};
    private static final String NAME = "name";

    // The names keep the unit suffix that simpleclient's unit("ms") used to append.
    private static final Histogram OPS_WAITING = Histogram.builder()
            .name("pulsar_batch_metadata_store_queue_wait_time_ms")
            .help("-")
            .labelNames(NAME)
            .classicOnly()
            .classicUpperBounds(BUCKETS)
            .register();
    private static final Histogram BATCH_EXECUTE_TIME = Histogram.builder()
            .name("pulsar_batch_metadata_store_batch_execute_time_ms")
            .help("-")
            .labelNames(NAME)
            .classicOnly()
            .classicUpperBounds(BUCKETS)
            .register();
    private static final Histogram OPS_PER_BATCH = Histogram.builder()
            .name("pulsar_batch_metadata_store_batch_size")
            .help("-")
            .labelNames(NAME)
            .classicOnly()
            .classicUpperBounds(BUCKETS)
            .register();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String metadataStoreName;

    private final DistributionDataPoint batchOpsWaitingChild;
    private final DistributionDataPoint batchExecuteTimeChild;
    private final DistributionDataPoint opsPerBatchChild;

    public BatchMetadataStoreStats(String metadataStoreName) {
        this.metadataStoreName = metadataStoreName;
        this.batchOpsWaitingChild = OPS_WAITING.labelValues(metadataStoreName);
        this.batchExecuteTimeChild = BATCH_EXECUTE_TIME.labelValues(metadataStoreName);
        this.opsPerBatchChild = OPS_PER_BATCH.labelValues(metadataStoreName);
    }

    public void recordOpWaiting(long millis) {
        this.batchOpsWaitingChild.observe(millis);
    }

    public void recordBatchExecuteTime(long millis) {
        this.batchExecuteTimeChild.observe(millis);
    }

    public void recordOpsInBatch(int ops) {
        this.opsPerBatchChild.observe(ops);
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            OPS_WAITING.remove(this.metadataStoreName);
            BATCH_EXECUTE_TIME.remove(this.metadataStoreName);
            OPS_PER_BATCH.remove(metadataStoreName);
        }
    }
}

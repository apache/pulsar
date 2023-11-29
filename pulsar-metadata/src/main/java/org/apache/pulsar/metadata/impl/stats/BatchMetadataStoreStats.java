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

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BatchMetadataStoreStats implements AutoCloseable {
    private static final double[] BUCKETS = new double[]{1, 5, 10, 20, 50, 100, 200, 500, 1000};
    private static final String NAME = "name";

    private static final Gauge EXECUTOR_QUEUE_SIZE = Gauge
            .build("pulsar_batch_metadata_store_executor_queue_size", "-")
            .labelNames(NAME)
            .register();
    private static final Histogram OPS_WAITING = Histogram
            .build("pulsar_batch_metadata_store_queue_wait_time", "-")
            .unit("ms")
            .labelNames(NAME)
            .buckets(BUCKETS)
            .register();
    private static final Histogram BATCH_EXECUTE_TIME = Histogram
            .build("pulsar_batch_metadata_store_batch_execute_time", "-")
            .unit("ms")
            .labelNames(NAME)
            .buckets(BUCKETS)
            .register();
    private static final Histogram OPS_PER_BATCH = Histogram
            .build("pulsar_batch_metadata_store_batch_size", "-")
            .labelNames(NAME)
            .buckets(BUCKETS)
            .register();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ThreadPoolExecutor executor;
    private final String metadataStoreName;

    private final Histogram.Child batchOpsWaitingChild;
    private final Histogram.Child batchExecuteTimeChild;
    private final Histogram.Child opsPerBatchChild;

    public BatchMetadataStoreStats(String metadataStoreName, ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor tx) {
            this.executor = tx;
        } else {
            this.executor = null;
        }
        this.metadataStoreName = metadataStoreName;

        EXECUTOR_QUEUE_SIZE.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return BatchMetadataStoreStats.this.executor == null ? 0 :
                        BatchMetadataStoreStats.this.executor.getQueue().size();
            }
        }, metadataStoreName);

        this.batchOpsWaitingChild = OPS_WAITING.labels(metadataStoreName);
        this.batchExecuteTimeChild = BATCH_EXECUTE_TIME.labels(metadataStoreName);
        this.opsPerBatchChild = OPS_PER_BATCH.labels(metadataStoreName);

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
            EXECUTOR_QUEUE_SIZE.remove(this.metadataStoreName);
            OPS_WAITING.remove(this.metadataStoreName);
            BATCH_EXECUTE_TIME.remove(this.metadataStoreName);
            OPS_PER_BATCH.remove(metadataStoreName);
        }
    }
}

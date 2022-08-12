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
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.metadata.impl.batching.MetadataOp;
import org.jctools.queues.MessagePassingQueue;

public final class BatchMetadataStoreStats implements AutoCloseable {
    private static final double[] BUCKETS = new double[]{1, 5, 10, 20, 50, 100, 200, 500, 1000};
    private static final AtomicInteger COUNTER = new AtomicInteger(1);
    private static final String LABEL_NAME = "name";

    private static final Gauge QUEUEING_READ_OPS = Gauge
            .build("pulsar_batch_metadata_store_queueing_read", "-")
            .labelNames(LABEL_NAME)
            .register();
    private static final Gauge QUEUEING_WRITE_OPS = Gauge
            .build("pulsar_batch_metadata_store_queueing_write", "-")
            .labelNames(LABEL_NAME)
            .register();
    private static final Gauge EXECUTOR_QUEUE_SIZE = Gauge
            .build("pulsar_batch_metadata_store_executor_queue_size", "-")
            .labelNames(LABEL_NAME)
            .register();
    private static final Counter READ_OPS_OVERFLOW = Counter
            .build("pulsar_batch_metadata_store_read_overflow" , "-")
            .labelNames(LABEL_NAME)
            .register();
    private static final Counter WRITE_OPS_OVERFLOW = Counter
            .build("pulsar_batch_metadata_store_write_overflow" , "-")
            .labelNames(LABEL_NAME)
            .register();
    private static final Histogram BATCH_OPS_WAITING = Histogram
            .build("pulsar_batch_metadata_store_waiting", "-")
            .unit("ms")
            .labelNames(LABEL_NAME)
            .buckets(BUCKETS)
            .register();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ThreadPoolExecutor executor;
    private final MessagePassingQueue<MetadataOp> readOps;
    private final MessagePassingQueue<MetadataOp> writeOps;
    private final String metadataStoreName;

    private final Counter.Child readOpsOverflowChild;
    private final Counter.Child writeOpsOverflowChild;
    private final Histogram.Child batchOpsWaitingChild;

    public BatchMetadataStoreStats(ExecutorService executor, MessagePassingQueue<MetadataOp> readOps,
                                   MessagePassingQueue<MetadataOp> writeOps) {
        if (executor instanceof ThreadPoolExecutor tx) {
            this.executor = tx;
        } else {
            this.executor = null;
        }
        this.readOps = readOps;
        this.writeOps = writeOps;
        this.metadataStoreName = "metadata_store_" + COUNTER.getAndIncrement();


        QUEUEING_READ_OPS.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return BatchMetadataStoreStats.this.readOps.size();
            }
        }, metadataStoreName);

        QUEUEING_WRITE_OPS.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return BatchMetadataStoreStats.this.writeOps.size();
            }
        }, metadataStoreName);

        EXECUTOR_QUEUE_SIZE.setChild(new Gauge.Child() {
            @Override
            public double get() {
                return BatchMetadataStoreStats.this.executor == null ? 0 :
                        BatchMetadataStoreStats.this.executor.getQueue().size();
            }
        }, metadataStoreName);

        this.readOpsOverflowChild = READ_OPS_OVERFLOW.labels(metadataStoreName);
        this.writeOpsOverflowChild = WRITE_OPS_OVERFLOW.labels(metadataStoreName);
        this.batchOpsWaitingChild = BATCH_OPS_WAITING.labels(metadataStoreName);
    }

    public void recordOpWaiting(long millis) {
        this.batchOpsWaitingChild.observe(millis);
    }

    public void recordReadOpOverflow() {
        this.readOpsOverflowChild.inc();
    }

    public void recordWriteOpOverflow() {
        this.writeOpsOverflowChild.inc();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            QUEUEING_READ_OPS.remove(this.metadataStoreName);
            QUEUEING_WRITE_OPS.remove(this.metadataStoreName);
            EXECUTOR_QUEUE_SIZE.remove(this.metadataStoreName);
            BATCH_OPS_WAITING.remove(this.metadataStoreName);
            READ_OPS_OVERFLOW.remove(this.metadataStoreName);
            WRITE_OPS_OVERFLOW.remove(this.metadataStoreName);
        }
    }
}

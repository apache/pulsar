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
package org.apache.pulsar.broker.transaction.buffer.stats;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TxnSnapshotSegmentStats implements AutoCloseable {
    private static final String NAMESPACE_LABEL_NAME = "namespace";
    private static final String TOPIC_NAME_LABEL_NAME = "topic_name";
    private static final String OPERATION_LABEL_NAME = "op";
    private static final String STATUS_LABEL_NAME = "status";
    private static final String PREFIX = "pulsar_txn_tb_snapshot_";

    private static final Counter SNAPSHOT_SEGMENT_OP_TOTAL = Counter
            .build(PREFIX + "segment_op_total",
                    "Pulsar transaction buffer snapshot segment operation count.")
            .labelNames(NAMESPACE_LABEL_NAME, TOPIC_NAME_LABEL_NAME, OPERATION_LABEL_NAME, STATUS_LABEL_NAME)
            .register();

    private static final Counter SNAPSHOT_INDEX_OP_TOTAL = Counter
            .build(PREFIX + "index_op_total", "Pulsar transaction buffer snapshot index operation count.")
            .labelNames(NAMESPACE_LABEL_NAME, TOPIC_NAME_LABEL_NAME, OPERATION_LABEL_NAME, STATUS_LABEL_NAME)
            .register();

    private static final Gauge SNAPSHOT_SEGMENT_TOTAL = Gauge
            .build(PREFIX + "index_total",
                    "Number of snapshot segments maintained in the Pulsar transaction buffer.")
            .labelNames("namespace", "topic")
            .register();

    private static final Histogram SNAPSHOT_INDEX_ENTRY_BYTES = Histogram
            .build(PREFIX + "index_entry_bytes",
                    "Size of the snapshot index entry maintained in the Pulsar transaction buffer.")
            .labelNames("namespace", "topic")
            .unit("bytes")
            .register();

    private final Counter.Child segmentOpAddSuccessChild;
    private final Counter.Child segmentOpDelSuccessChild;
    private final Counter.Child segmentOpReadSuccessChild;
    private final Counter.Child indexOpAddSuccessChild;
    private final Counter.Child indexOpDelSuccessChild;
    private final Counter.Child indexOpReadSuccessChild;
    private final Counter.Child segmentOpAddFailedChild;
    private final Counter.Child segmentOpDelFailedChild;
    private final Counter.Child segmentOpReadFailedChild;
    private final Counter.Child indexOpAddFailedChild;
    private final Counter.Child indexOpDelFailedChild;
    private final Counter.Child indexOpReadFailedChild;
    private final Gauge.Child snapshotSegmentTotalChild;
    private final Histogram.Child snapshotIndexEntryBytesChild;

    private final String namespace;
    private final String topicName;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public TxnSnapshotSegmentStats(String namespace, String topicName) {
        this.namespace = namespace;
        this.topicName = topicName;

        this.segmentOpAddSuccessChild = SNAPSHOT_SEGMENT_OP_TOTAL
                .labels(namespace, topicName, "add", "success");
        this.segmentOpDelSuccessChild = SNAPSHOT_SEGMENT_OP_TOTAL
                .labels(namespace, topicName, "del", "success");
        this.segmentOpReadSuccessChild = SNAPSHOT_SEGMENT_OP_TOTAL
                .labels(namespace, topicName, "read", "success");
        this.indexOpAddSuccessChild = SNAPSHOT_INDEX_OP_TOTAL
                .labels(namespace, topicName, "add", "success");
        this.indexOpDelSuccessChild = SNAPSHOT_INDEX_OP_TOTAL
                .labels(namespace, topicName, "del", "success");
        this.indexOpReadSuccessChild = SNAPSHOT_INDEX_OP_TOTAL
                .labels(namespace, topicName, "read", "success");
        this.segmentOpAddFailedChild = SNAPSHOT_SEGMENT_OP_TOTAL
                .labels(namespace, topicName, "add", "fail");
        this.segmentOpDelFailedChild = SNAPSHOT_SEGMENT_OP_TOTAL
                .labels(namespace, topicName, "del", "fail");
        this.segmentOpReadFailedChild = SNAPSHOT_SEGMENT_OP_TOTAL
                .labels(namespace, topicName, "read", "fail");
        this.indexOpAddFailedChild = SNAPSHOT_INDEX_OP_TOTAL
                .labels(namespace, topicName, "add", "fail");
        this.indexOpDelFailedChild = SNAPSHOT_INDEX_OP_TOTAL
                .labels(namespace, topicName, "del", "fail");
        this.indexOpReadFailedChild = SNAPSHOT_INDEX_OP_TOTAL
                .labels(namespace, topicName, "read", "fail");
        this.snapshotSegmentTotalChild = SNAPSHOT_SEGMENT_TOTAL
                .labels(namespace, topicName);
        this.snapshotIndexEntryBytesChild = SNAPSHOT_INDEX_ENTRY_BYTES
                .labels(namespace, topicName);
    }

    public void recordSegmentOpAddSuccess() {
        this.segmentOpAddSuccessChild.inc();
    }

    public void recordSegmentOpDelSuccess() {
        this.segmentOpDelSuccessChild.inc();
    }

    public void recordSegmentOpReadSuccess() {
        this.segmentOpReadSuccessChild.inc();
    }

    public void recordIndexOpAddSuccess() {
        this.indexOpAddSuccessChild.inc();
    }

    public void recordIndexOpDelSuccess() {
        this.indexOpDelSuccessChild.inc();
    }

    public void recordIndexOpReadSuccess() {
        this.indexOpReadSuccessChild.inc();
    }

    public void recordSegmentOpAddFail() {
        this.segmentOpAddFailedChild.inc();
    }

    public void recordSegmentOpDelFail() {
        this.segmentOpDelFailedChild.inc();
    }

    public void recordSegmentOpReadFail() {
        this.segmentOpReadFailedChild.inc();
    }

    public void recordIndexOpAddFail() {
        this.indexOpAddFailedChild.inc();
    }

    public void recordIndexOpDelFail() {
        this.indexOpDelFailedChild.inc();
    }

    public void recordIndexOpReadFail() {
        this.indexOpReadFailedChild.inc();
    }

    public void setSnapshotSegmentTotal(double value) {
        snapshotSegmentTotalChild.set(value);
    }

    public void observeSnapshotIndexEntryBytes(double value) {
        snapshotIndexEntryBytesChild.observe(value);
    }

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            SNAPSHOT_SEGMENT_OP_TOTAL.clear();
            SNAPSHOT_INDEX_OP_TOTAL.clear();
            SNAPSHOT_SEGMENT_TOTAL.remove(namespace, topicName);
            SNAPSHOT_INDEX_ENTRY_BYTES.remove(namespace, topicName);
        }
    }
}


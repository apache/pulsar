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

public final class MetadataStoreStats implements AutoCloseable {
    private static final double[] BUCKETS = new double[]{1, 3, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000};
    private static final String LABEL_NAME = "name";
    protected static final String PREFIX = "pulsar_metadata_store_";

    private static final Histogram GET_OPS_SUCCEED = Histogram
            .build(PREFIX + "get_latency", "-")
            .unit("ms")
            .buckets(BUCKETS)
            .labelNames(LABEL_NAME)
            .register();
    private static final Histogram DEL_OPS_SUCCEED = Histogram
            .build(PREFIX + "del_latency", "-")
            .unit("ms")
            .buckets(BUCKETS)
            .labelNames(LABEL_NAME)
            .register();
    private static final Histogram PUT_OPS_SUCCEED = Histogram
            .build(PREFIX + "put_latency", "-")
            .unit("ms")
            .buckets(BUCKETS)
            .labelNames(LABEL_NAME)
            .register();
    private static final Counter GET_OPS_FAILED = Counter
            .build(PREFIX + "get_failed", "-")
            .labelNames(LABEL_NAME)
            .register();
    private static final Counter DEL_OPS_FAILED = Counter
            .build(PREFIX + "del_failed", "-")
            .labelNames(LABEL_NAME)
            .register();
    private static final Counter PUT_OPS_FAILED = Counter
            .build(PREFIX + "put_failed", "-")
            .labelNames(LABEL_NAME)
            .register();
    private static final Counter PUT_BYTES = Counter
            .build(PREFIX + "put", "-")
            .unit("bytes")
            .labelNames(LABEL_NAME)
            .register();

    private final Histogram.Child getOpsSucceedChild;
    private final Histogram.Child delOpsSucceedChild;
    private final Histogram.Child puttOpsSucceedChild;
    private final Counter.Child getOpsFailedChild;
    private final Counter.Child delOpsFailedChild;
    private final Counter.Child putOpsFailedChild;
    private final Counter.Child putBytesChild;
    private final String metadataStoreName;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MetadataStoreStats(String metadataStoreName) {
        this.metadataStoreName = metadataStoreName;

        this.getOpsSucceedChild = GET_OPS_SUCCEED.labels(metadataStoreName);
        this.delOpsSucceedChild = DEL_OPS_SUCCEED.labels(metadataStoreName);
        this.puttOpsSucceedChild = PUT_OPS_SUCCEED.labels(metadataStoreName);
        this.getOpsFailedChild = GET_OPS_FAILED.labels(metadataStoreName);
        this.delOpsFailedChild = DEL_OPS_FAILED.labels(metadataStoreName);
        this.putOpsFailedChild = PUT_OPS_FAILED.labels(metadataStoreName);
        this.putBytesChild = PUT_BYTES.labels(metadataStoreName);
    }

    public void recordGetOpsLatency(long millis) {
        this.getOpsSucceedChild.observe(millis);
    }

    public void recordDelOpsLatency(long millis) {
        this.delOpsSucceedChild.observe(millis);
    }

    public void recordPutOpsLatency(long millis, int bytes) {
        this.puttOpsSucceedChild.observe(millis);
        this.putBytesChild.inc(bytes);
    }

    public void recordGetOpsFailed() {
        this.getOpsFailedChild.inc();
    }

    public void recordDelOpsFailed() {
        this.delOpsFailedChild.inc();
    }

    public void recordPutOpsFailed() {
        this.putOpsFailedChild.inc();
    }

    @Override
    public void close() throws Exception {
        if (this.closed.compareAndSet(false, true)) {
            GET_OPS_SUCCEED.remove(this.metadataStoreName);
            DEL_OPS_SUCCEED.remove(this.metadataStoreName);
            PUT_OPS_SUCCEED.remove(this.metadataStoreName);
            GET_OPS_FAILED.remove(this.metadataStoreName);
            DEL_OPS_FAILED.remove(this.metadataStoreName);
            PUT_OPS_FAILED.remove(this.metadataStoreName);
            PUT_BYTES.remove(this.metadataStoreName);
        }
    }
}

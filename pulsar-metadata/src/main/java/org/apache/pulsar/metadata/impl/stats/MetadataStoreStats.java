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
    private static final String OPS_TYPE_LABEL_NAME = "type";
    private static final String METADATA_STORE_LABEL_NAME = "name";
    private static final String OPS_TYPE_GET = "get";
    private static final String OPS_TYPE_DEL = "del";
    private static final String OPS_TYPE_PUT = "put";
    protected static final String PREFIX = "pulsar_metadata_store_";

    private static final Histogram OPS_SUCCEED = Histogram
            .build(PREFIX + "ops_latency", "-")
            .unit("ms")
            .buckets(BUCKETS)
            .labelNames(METADATA_STORE_LABEL_NAME, OPS_TYPE_LABEL_NAME)
            .register();
    private static final Counter OPS_FAILED = Counter
            .build(PREFIX + "ops_failed", "-")
            .labelNames(METADATA_STORE_LABEL_NAME, OPS_TYPE_LABEL_NAME)
            .register();
    private static final Counter PUT_BYTES = Counter
            .build(PREFIX + "put_", "-")
            .unit("bytes")
            .labelNames(METADATA_STORE_LABEL_NAME)
            .register();

    private final Histogram.Child getOpsSucceedChild;
    private final Histogram.Child delOpsSucceedChild;
    private final Histogram.Child putOpsSucceedChild;
    private final Counter.Child getOpsFailedChild;
    private final Counter.Child delOpsFailedChild;
    private final Counter.Child putOpsFailedChild;
    private final Counter.Child putBytesChild;
    private final String metadataStoreName;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MetadataStoreStats(String metadataStoreName) {
        this.metadataStoreName = metadataStoreName;

        this.getOpsSucceedChild = OPS_SUCCEED.labels(metadataStoreName, OPS_TYPE_GET);
        this.delOpsSucceedChild = OPS_SUCCEED.labels(metadataStoreName, OPS_TYPE_DEL);
        this.putOpsSucceedChild = OPS_SUCCEED.labels(metadataStoreName, OPS_TYPE_PUT);
        this.getOpsFailedChild = OPS_FAILED.labels(metadataStoreName, OPS_TYPE_GET);
        this.delOpsFailedChild = OPS_FAILED.labels(metadataStoreName, OPS_TYPE_DEL);
        this.putOpsFailedChild = OPS_FAILED.labels(metadataStoreName, OPS_TYPE_PUT);
        this.putBytesChild = PUT_BYTES.labels(metadataStoreName);
    }

    public void recordGetOpsLatency(long millis) {
        this.getOpsSucceedChild.observe(millis);
    }

    public void recordDelOpsLatency(long millis) {
        this.delOpsSucceedChild.observe(millis);
    }

    public void recordPutOpsLatency(long millis, int bytes) {
        this.putOpsSucceedChild.observe(millis);
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
            OPS_SUCCEED.remove(this.metadataStoreName, OPS_TYPE_GET);
            OPS_SUCCEED.remove(this.metadataStoreName, OPS_TYPE_DEL);
            OPS_SUCCEED.remove(this.metadataStoreName, OPS_TYPE_PUT);
            OPS_FAILED.remove(this.metadataStoreName, OPS_TYPE_GET);
            OPS_FAILED.remove(this.metadataStoreName, OPS_TYPE_DEL);
            OPS_FAILED.remove(this.metadataStoreName, OPS_TYPE_PUT);
            PUT_BYTES.remove(this.metadataStoreName);
        }
    }
}

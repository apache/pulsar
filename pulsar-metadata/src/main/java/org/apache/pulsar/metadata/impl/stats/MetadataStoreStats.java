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

import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MetadataStoreStats implements AutoCloseable {
    private static final double[] BUCKETS = new double[]{1, 3, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000};
    private static final String OPS_TYPE_LABEL_NAME = "type";
    private static final String METADATA_STORE_LABEL_NAME = "name";
    private static final String STATUS = "status";

    private static final String OPS_TYPE_GET = "get";
    private static final String OPS_TYPE_DEL = "del";
    private static final String OPS_TYPE_PUT = "put";
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_FAIL = "fail";

    protected static final String PREFIX = "pulsar_metadata_store_";


    private static Histogram OPS_LATENCY;
    private static Counter PUT_BYTES;

    private Histogram.Child getOpsSucceedChild;
    private Histogram.Child delOpsSucceedChild;
    private Histogram.Child putOpsSucceedChild;
    private Histogram.Child getOpsFailedChild;
    private Histogram.Child delOpsFailedChild;
    private Histogram.Child putOpsFailedChild;
    private Counter.Child putBytesChild;
    private final String metadataStoreName;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MetadataStoreStats(String metadataStoreName) {
        MetadataStoreStats.registerMetrics(CollectorRegistry.defaultRegistry);

        this.metadataStoreName = metadataStoreName;

        this.getOpsSucceedChild = OPS_LATENCY.labels(metadataStoreName, OPS_TYPE_GET, STATUS_SUCCESS);
        this.delOpsSucceedChild = OPS_LATENCY.labels(metadataStoreName, OPS_TYPE_DEL, STATUS_SUCCESS);
        this.putOpsSucceedChild = OPS_LATENCY.labels(metadataStoreName, OPS_TYPE_PUT, STATUS_SUCCESS);
        this.getOpsFailedChild = OPS_LATENCY.labels(metadataStoreName, OPS_TYPE_GET, STATUS_FAIL);
        this.delOpsFailedChild = OPS_LATENCY.labels(metadataStoreName, OPS_TYPE_DEL, STATUS_FAIL);
        this.putOpsFailedChild = OPS_LATENCY.labels(metadataStoreName, OPS_TYPE_PUT, STATUS_FAIL);
        this.putBytesChild = PUT_BYTES.labels(metadataStoreName);
    }

    private static String metricOverrideByUnittest = "";

    @VisibleForTesting
    public static String registerMetrics(CollectorRegistry registry) {
        boolean isUnittestMetricOverride = registry.equals(CollectorRegistry.defaultRegistry);

        // lock to avoid different instance re-register metrics
        synchronized (MetadataStoreStats.class) {
            boolean createMetric = false;

            if (OPS_LATENCY == null) {
                createMetric = true;

                OPS_LATENCY = Histogram
                        .build(PREFIX + "ops_latency", "-")
                        .unit("ms")
                        .buckets(BUCKETS)
                        .labelNames(METADATA_STORE_LABEL_NAME, OPS_TYPE_LABEL_NAME, STATUS)
                        .register(registry);
            }

            if (PUT_BYTES == null) {
                PUT_BYTES = Counter
                        .build(PREFIX + "put", "-")
                        .unit("bytes")
                        .labelNames(METADATA_STORE_LABEL_NAME)
                        .register(registry);
            }

            if (createMetric && isUnittestMetricOverride) {
                metricOverrideByUnittest = UUID.randomUUID().toString();
                return metricOverrideByUnittest;
            }
        }

        return "";
    }

    @VisibleForTesting
    public static void unregisterMetrics(String registerToken, CollectorRegistry registry) {
        synchronized (MetadataStoreStats.class) {
            if (registerToken.equals(metricOverrideByUnittest)) {
                metricOverrideByUnittest = "";

                registry.unregister(PUT_BYTES);
                registry.unregister(OPS_LATENCY);

                PUT_BYTES = null;
                OPS_LATENCY = null;

                registerMetrics(CollectorRegistry.defaultRegistry);
            }
        }
    }

    public void recordGetOpsSucceeded(long millis) {
        this.getOpsSucceedChild.observe(millis);
    }

    public void recordDelOpsSucceeded(long millis) {
        this.delOpsSucceedChild.observe(millis);
    }

    public void recordPutOpsSucceeded(long millis, int bytes) {
        this.putOpsSucceedChild.observe(millis);
        this.putBytesChild.inc(bytes);
    }

    public void recordGetOpsFailed(long millis) {
        this.getOpsFailedChild.observe(millis);
    }

    public void recordDelOpsFailed(long millis) {
        this.delOpsFailedChild.observe(millis);
    }

    public void recordPutOpsFailed(long millis) {
        this.putOpsFailedChild.observe(millis);
    }

    @Override
    public void close() throws Exception {
        if (this.closed.compareAndSet(false, true)) {
            synchronized (MetadataStoreStats.this) {
                if (OPS_LATENCY != null) {
                    OPS_LATENCY.remove(this.metadataStoreName, OPS_TYPE_GET, STATUS_SUCCESS);
                    OPS_LATENCY.remove(this.metadataStoreName, OPS_TYPE_DEL, STATUS_SUCCESS);
                    OPS_LATENCY.remove(this.metadataStoreName, OPS_TYPE_PUT, STATUS_SUCCESS);
                    OPS_LATENCY.remove(this.metadataStoreName, OPS_TYPE_GET, STATUS_FAIL);
                    OPS_LATENCY.remove(this.metadataStoreName, OPS_TYPE_DEL, STATUS_FAIL);
                    OPS_LATENCY.remove(this.metadataStoreName, OPS_TYPE_PUT, STATUS_FAIL);
                }

                if (PUT_BYTES != null) {
                    PUT_BYTES.remove(this.metadataStoreName);
                }
            }
        }
    }
}

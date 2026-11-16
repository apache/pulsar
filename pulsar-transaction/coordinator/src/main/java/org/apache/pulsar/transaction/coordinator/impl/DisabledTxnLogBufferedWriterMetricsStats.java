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
package org.apache.pulsar.transaction.coordinator.impl;

import io.prometheus.metrics.model.registry.PrometheusRegistry;

public class DisabledTxnLogBufferedWriterMetricsStats extends TxnLogBufferedWriterMetricsStats {

    public static final DisabledTxnLogBufferedWriterMetricsStats DISABLED_BUFFERED_WRITER_METRICS =
            new DisabledTxnLogBufferedWriterMetricsStats();

    private DisabledTxnLogBufferedWriterMetricsStats() {
        // A private registry that is never scraped, so the no-op instance stays out of the default registry.
        super("disabled", new String[0], new String[0], new PrometheusRegistry());
    }

    public void close() {
    }

    public void triggerFlushByRecordsCount(int recordCount, long bytesSize, long delayMillis) {
    }

    public void triggerFlushByBytesSize(int recordCount, long bytesSize, long delayMillis) {
    }

    public void triggerFlushByByMaxDelay(int recordCount, long bytesSize, long delayMillis) {
    }

    public void triggerFlushByLargeSingleData(int recordCount, long bytesSize, long delayMillis) {
    }
}

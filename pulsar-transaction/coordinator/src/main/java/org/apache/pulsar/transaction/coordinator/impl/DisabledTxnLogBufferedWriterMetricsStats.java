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

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

public class DisabledTxnLogBufferedWriterMetricsStats extends TxnLogBufferedWriterMetricsStats {

    public static final DisabledTxnLogBufferedWriterMetricsStats DISABLED_BUFFERED_WRITER_METRICS =
            new DisabledTxnLogBufferedWriterMetricsStats();

    private static class DisabledCollectorRegistry extends CollectorRegistry {

        private static final DisabledCollectorRegistry INSTANCE = new DisabledCollectorRegistry();

        public void register(Collector m) {
        }
        public void unregister(Collector m) {
        }
    }

    private DisabledTxnLogBufferedWriterMetricsStats() {
        super("disabled", new String[0], new String[0], DisabledCollectorRegistry.INSTANCE);
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

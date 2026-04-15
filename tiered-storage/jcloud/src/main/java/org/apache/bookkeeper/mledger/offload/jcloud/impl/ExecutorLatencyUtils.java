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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;

final class ExecutorLatencyUtils {

    private ExecutorLatencyUtils() {
    }

    static Runnable trackOffloadExecutorQueueLatency(LedgerOffloaderStats offloaderStats,
                                                     String topicName,
                                                     Runnable task) {
        return trackQueueLatency(offloaderStats, topicName, task, false);
    }

    static Runnable trackReadOffloadExecutorQueueLatency(LedgerOffloaderStats offloaderStats,
                                                         String topicName,
                                                         Runnable task) {
        return trackQueueLatency(offloaderStats, topicName, task, true);
    }

    private static Runnable trackQueueLatency(LedgerOffloaderStats offloaderStats,
                                              String topicName,
                                              Runnable task,
                                              boolean readTask) {
        final long enqueueTimeNanos = System.nanoTime();
        return () -> {
            long queuedLatencyNanos = System.nanoTime() - enqueueTimeNanos;
            if (readTask) {
                offloaderStats.recordReadOffloadExecutorQueueLatency(topicName, queuedLatencyNanos,
                        TimeUnit.NANOSECONDS);
            } else {
                offloaderStats.recordOffloadExecutorQueueLatency(topicName, queuedLatencyNanos,
                        TimeUnit.NANOSECONDS);
            }
            task.run();
        };
    }
}

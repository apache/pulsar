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

package org.apache.pulsar.client.impl.metrics;

import org.apache.pulsar.client.impl.MemoryLimitController;

public class MemoryBufferStats implements AutoCloseable {

    public static final String BUFFER_USAGE_COUNTER = "pulsar.client.memory.buffer.usage";
    private final ObservableUpDownCounter bufferUsageCounter;

    public static final String BUFFER_LIMIT_COUNTER = "pulsar.client.memory.buffer.limit";
    private final ObservableUpDownCounter bufferLimitCounter;

    public MemoryBufferStats(InstrumentProvider instrumentProvider, MemoryLimitController memoryLimitController) {
        bufferUsageCounter = instrumentProvider.newObservableUpDownCounter(
                BUFFER_USAGE_COUNTER,
                Unit.Bytes,
                "Current memory buffer usage by the client",
                null, // no topic
                null, // no extra attributes
                measurement -> {
                    if (memoryLimitController.isMemoryLimited()) {
                        measurement.record(memoryLimitController.currentUsage());
                    }
                });

        bufferLimitCounter = instrumentProvider.newObservableUpDownCounter(
                BUFFER_LIMIT_COUNTER,
                Unit.Bytes,
                "Memory buffer limit configured for the client",
                null, // no topic
                null, // no extra attributes
                measurement -> {
                    if (memoryLimitController.isMemoryLimited()) {
                        measurement.record(memoryLimitController.memoryLimit());
                    }
                });
    }

    @Override
    public void close() {
        bufferUsageCounter.close();
        bufferLimitCounter.close();
    }
}
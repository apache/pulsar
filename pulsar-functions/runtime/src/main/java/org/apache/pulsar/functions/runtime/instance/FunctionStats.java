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
package org.apache.pulsar.functions.runtime.instance;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * Function stats.
 */
@Slf4j
@Getter
@Setter
public class FunctionStats {
    @Getter
    @Setter
    class Stats {
        private long totalProcessed;
        private long totalSuccessfullyProcessed;
        private long totalUserExceptions;
        private long totalSystemExceptions;
        private long totalTimeoutExceptions;
        private Map<String, Long> totalDeserializationExceptions = new HashMap<>();
        private long totalSerializationExceptions;
        private long totalLatencyMs;

        public void incrementProcessed() { totalProcessed++; }
        public void incrementSuccessfullyProcessed(long latency) {
            totalSuccessfullyProcessed++;
            totalLatencyMs += latency;
        }
        public void incrementUserExceptions() { totalUserExceptions++; }
        public void incrementSystemExceptions() { totalSystemExceptions++; }
        public void incrementTimeoutExceptions() { totalTimeoutExceptions++; }
        public void incrementDeserializationExceptions(String topic) {
            if (!totalDeserializationExceptions.containsKey(topic)) {
                totalDeserializationExceptions.put(topic, 0l);
            }
            totalDeserializationExceptions.put(topic, totalDeserializationExceptions.get(topic) + 1);
        }
        public void incrementSerializationExceptions() { totalSerializationExceptions++; }
        public void reset() {
            totalProcessed = 0;
            totalSuccessfullyProcessed = 0;
            totalUserExceptions = 0;
            totalSystemExceptions = 0;
            totalTimeoutExceptions = 0;
            totalDeserializationExceptions.clear();
            totalSerializationExceptions = 0;
            totalLatencyMs = 0;
        }
        public double computeLatency() {
            if (totalSuccessfullyProcessed <= 0) {
                return 0;
            } else {
                return totalLatencyMs / totalSuccessfullyProcessed;
            }
        }
    }

    private Stats currentStats;
    private Stats totalStats;

    public FunctionStats() {
        currentStats = new Stats();
        totalStats = new Stats();
    }

    public void incrementProcessed() {
        currentStats.incrementProcessed();
        totalStats.incrementProcessed();
    }

    public void incrementSuccessfullyProcessed(long latency) {
        currentStats.incrementSuccessfullyProcessed(latency);
        totalStats.incrementSuccessfullyProcessed(latency);
    }
    public void incrementUserExceptions() {
        currentStats.incrementUserExceptions();
        totalStats.incrementUserExceptions();
    }
    public void incrementSystemExceptions() {
        currentStats.incrementSystemExceptions();
        totalStats.incrementSystemExceptions();
    }
    public void incrementTimeoutExceptions() {
        currentStats.incrementTimeoutExceptions();
        totalStats.incrementTimeoutExceptions();
    }
    public void incrementDeserializationExceptions(String topic) {
        currentStats.incrementDeserializationExceptions(topic);
        totalStats.incrementDeserializationExceptions(topic);
    }
    public void incrementSerializationExceptions() {
        currentStats.incrementSerializationExceptions();
        totalStats.incrementSerializationExceptions();
    }
    public void resetCurrent() {
        currentStats.reset();
    }
}

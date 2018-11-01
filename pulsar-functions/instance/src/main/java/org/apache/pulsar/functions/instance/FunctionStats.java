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
package org.apache.pulsar.functions.instance;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.proto.InstanceCommunication;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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
        private List<InstanceCommunication.FunctionStatus.ExceptionInformation> latestUserExceptions =
                new LinkedList<>();
        private long totalSystemExceptions;
        private List<InstanceCommunication.FunctionStatus.ExceptionInformation> latestSystemExceptions =
                new LinkedList<>();
        private Map<String, Long> totalDeserializationExceptions = new HashMap<>();
        private long totalSerializationExceptions;
        private long totalLatencyMs;
        private long lastInvocationTime;

        public void incrementProcessed(long processedAt) {
            totalProcessed++;
            lastInvocationTime = processedAt;
        }
        public void incrementSuccessfullyProcessed(long latency) {
            totalSuccessfullyProcessed++;
            totalLatencyMs += latency;
        }
        public void incrementUserExceptions(Exception ex) {
            InstanceCommunication.FunctionStatus.ExceptionInformation info =
                    InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                    .setExceptionString(ex.getMessage()).setMsSinceEpoch(System.currentTimeMillis()).build();
            latestUserExceptions.add(info);
            if (latestUserExceptions.size() > 10) {
                latestUserExceptions.remove(0);
            }
            totalUserExceptions++;
        }
        public void incrementSystemExceptions(Exception ex) {
            InstanceCommunication.FunctionStatus.ExceptionInformation info =
                    InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                            .setExceptionString(ex.getMessage()).setMsSinceEpoch(System.currentTimeMillis()).build();
            latestSystemExceptions.add(info);
            if (latestSystemExceptions.size() > 10) {
                latestSystemExceptions.remove(0);
            }
            totalSystemExceptions++;
        }
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
            totalDeserializationExceptions.clear();
            totalSerializationExceptions = 0;
            totalLatencyMs = 0;
        }
        public double computeLatency() {
            if (totalSuccessfullyProcessed <= 0) {
                return 0;
            } else {
                return totalLatencyMs / (double) totalSuccessfullyProcessed;
            }
        }
        
        public void update(Stats stats) {
            if (stats == null) {
                return;
            }
            this.totalProcessed = stats.totalProcessed;
            this.totalSuccessfullyProcessed = stats.totalSuccessfullyProcessed;
            this.totalUserExceptions = stats.totalUserExceptions;
            this.latestUserExceptions.clear();
            this.latestSystemExceptions.clear();
            this.totalDeserializationExceptions.clear();
            this.latestUserExceptions.addAll(stats.latestUserExceptions);
            this.latestSystemExceptions.addAll(stats.latestSystemExceptions);
            this.totalDeserializationExceptions.putAll(stats.totalDeserializationExceptions);
            this.totalSystemExceptions = stats.totalSystemExceptions;
            this.latestSystemExceptions = stats.latestSystemExceptions;
            this.totalSerializationExceptions = stats.totalSerializationExceptions;
            this.totalLatencyMs = stats.totalLatencyMs;
            this.lastInvocationTime = stats.lastInvocationTime;
        }
    }

    private Stats currentStats;
    private Stats totalStats;
    private Stats stats;
    
    public FunctionStats() {
        currentStats = new Stats();
        stats = new Stats();
        totalStats = new Stats();
    }

    public void incrementProcessed(long processedAt) {
        currentStats.incrementProcessed(processedAt);
        totalStats.incrementProcessed(processedAt);
    }

    public void incrementSuccessfullyProcessed(long latency) {
        currentStats.incrementSuccessfullyProcessed(latency);
        totalStats.incrementSuccessfullyProcessed(latency);
    }
    public void incrementUserExceptions(Exception ex) {
        currentStats.incrementUserExceptions(ex);
        totalStats.incrementUserExceptions(ex);
    }
    public void incrementSystemExceptions(Exception ex) {
        currentStats.incrementSystemExceptions(ex);
        totalStats.incrementSystemExceptions(ex);
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
        stats.update(currentStats);
        currentStats.reset();
    }
}

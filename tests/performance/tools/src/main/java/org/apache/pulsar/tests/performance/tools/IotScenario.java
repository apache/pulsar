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
package org.apache.pulsar.tests.performance.tools;

import java.util.ArrayList;
import java.util.List;

/** Configuration selected from the {@code workloads.iotTelemetry} scenario subtree. */
public record IotScenario(String serviceUrl, String topicPrefix, String subscriptionPrefix,
                          int durationSeconds, int warmupSeconds, long warmupMessages,
                          int rate, long numberOfMessages, int payloadBytes, int deviceCount,
                          int gatewayCount, int topicCount, int applicationCount,
                          int clientsPerApplication, int ioThreads, int listenerThreads,
                          int maxOutstanding, boolean batchingEnabled, boolean precreateProducers,
                          int consumerTimeoutSeconds,
                          int clientRestartIntervalSeconds, double clientRestartFraction) {
    public IotScenario {
        if (serviceUrl == null || serviceUrl.isBlank() || topicPrefix == null || topicPrefix.isBlank()
                || subscriptionPrefix == null || subscriptionPrefix.isBlank()) {
            throw new IllegalArgumentException("Service URL, topic prefix and subscription prefix are required");
        }
        if (durationSeconds < 1 || warmupSeconds < 0 || warmupMessages < 0
                || (warmupSeconds > 0 && warmupMessages > 0)
                || (warmupSeconds > 0 && rate == 0)
                || rate < 0 || numberOfMessages < 0
                || (rate == 0 && numberOfMessages == 0) || payloadBytes < TelemetryMessage.HEADER_BYTES
                || deviceCount < 1 || gatewayCount < 1 || topicCount < 1 || applicationCount < 1
                || clientsPerApplication < 1 || ioThreads < 1 || listenerThreads < 1
                || maxOutstanding < 1 || consumerTimeoutSeconds < durationSeconds + warmupSeconds
                || clientRestartIntervalSeconds < 0 || clientRestartFraction < 0 || clientRestartFraction > 1) {
            throw new IllegalArgumentException("IoT scenario counts and sizes are invalid");
        }
    }

    public long messageCount() {
        return Math.addExact(warmupMessageCount(), measurementMessageCount());
    }

    public long warmupMessageCount() {
        return warmupMessages > 0
                ? warmupMessages
                : Math.multiplyExact((long) warmupSeconds, rate);
    }

    public long measurementMessageCount() {
        return numberOfMessages > 0 ? numberOfMessages : Math.multiplyExact((long) durationSeconds, rate);
    }

    public List<String> topics() {
        List<String> result = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            result.add(topicPrefix + i);
        }
        return result;
    }

    public String subscriptionName(int applicationIndex) {
        if (applicationIndex < 0 || applicationIndex >= applicationCount) {
            throw new IllegalArgumentException("Application index out of range: " + applicationIndex);
        }
        return subscriptionPrefix + applicationIndex;
    }
}

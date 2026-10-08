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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * When an application that joined after the measurement started catches up: when each of its topics has delivered a
 * measured message received within the threshold of its publishing, after the application joined. An application
 * that joined at the start doesn't catch up.
 */
final class CatchUpTracker {
    private final int topicCount;
    private final long thresholdMillis;
    // each topic's first receipt of a message within the threshold
    private final Map<String, Long> caughtUpTopics = new ConcurrentHashMap<>();
    private final AtomicLong joinEpochMs = new AtomicLong();
    // when it caught up and its received messages then, published together
    private final AtomicReference<Result> caughtUp = new AtomicReference<>();

    private record Result(long epochMs, long messages) {
    }

    CatchUpTracker(int topicCount, long thresholdMillis) {
        this.topicCount = topicCount;
        this.thresholdMillis = thresholdMillis;
    }

    void joined(long epochMs) {
        joinEpochMs.set(epochMs);
    }

    /**
     * Records a received measured message.
     *
     * @param messages the application's received messages, read when it has caught up
     */
    void received(String topic, long publishEpochMs, long receivedEpochMs, LongSupplier messages) {
        if (joinEpochMs.get() == 0 || caughtUp.get() != null
                || receivedEpochMs - publishEpochMs > thresholdMillis) {
            return;
        }
        if (caughtUpTopics.putIfAbsent(topic, receivedEpochMs) == null && caughtUpTopics.size() >= topicCount) {
            // the last topic's receipt, which another listener may have recorded after this one
            caughtUp.compareAndSet(null, new Result(Collections.max(caughtUpTopics.values()), messages.getAsLong()));
        }
    }

    long joinEpochMs() {
        return joinEpochMs.get();
    }

    long caughtUpEpochMs() {
        Result result = caughtUp.get();
        return result != null ? result.epochMs() : 0;
    }

    long messagesWhenCaughtUp() {
        Result result = caughtUp.get();
        return result != null ? result.messages() : 0;
    }

    long thresholdMillis() {
        return thresholdMillis;
    }
}

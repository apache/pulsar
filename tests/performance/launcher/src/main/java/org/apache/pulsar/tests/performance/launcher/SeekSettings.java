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
package org.apache.pulsar.tests.performance.launcher;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

/**
 * The {@code seeks} section of a scenario's {@code offload} section: after the workloads have finished, the launcher
 * seeks a subscription of its own to random publish times, {@code count} times, each a reset of the subscription by
 * timestamp on a random topic, which makes the broker search the topic's ledgers by publish time, and with
 * {@code peek} reads the message at the new position, so that each seek is a random read of the topic's data,
 * offloaded or not.
 *
 * <pre>
 * offload:
 *   seeks:
 *     count: 500
 *     seed: 1
 *     peek: true
 *     unloadFirst: true
 *     timeoutSeconds: 1800
 * </pre>
 *
 * @param count how many seeks
 * @param seed the seed of the random topics and publish times, so that runs seek alike
 * @param peek read the message at each seek's position
 * @param unloadFirst unload the topics before the seeks, so that they read from storage, not from the broker's cache
 * @param timeoutSeconds the longest that the seeks may take; the run fails after it
 */
record SeekSettings(int count, long seed, boolean peek, boolean unloadFirst, int timeoutSeconds) {
    static final int DEFAULT_COUNT = 500;
    static final int DEFAULT_TIMEOUT_SECONDS = 1800;
    private static final List<String> KEYS = List.of("count", "seed", "peek", "unloadFirst", "timeoutSeconds");

    // The section's path in the scenario, which the messages name
    private static final String SECTION = "offload.seeks";

    /** Reads the {@code offload.seeks} section, which may be missing: null then, without seeks. */
    static SeekSettings read(JsonNode seeks) {
        if (seeks.isMissingNode() || seeks.isNull()) {
            return null;
        }
        if (!seeks.isObject()) {
            throw new IllegalArgumentException(SECTION + " must be a mapping with " + KEYS);
        }
        seeks.fieldNames().forEachRemaining(field -> {
            if (!KEYS.contains(field)) {
                throw new IllegalArgumentException(SECTION + "." + field + " isn't a setting; " + SECTION + " has "
                        + KEYS);
            }
        });
        JsonNode count = seeks.path("count");
        if (!count.isMissingNode() && !count.isNull() && (!count.isIntegralNumber() || !count.canConvertToInt()
                || count.intValue() < 1)) {
            throw new IllegalArgumentException(SECTION + ".count must be a whole number, 1 or more");
        }
        JsonNode timeout = seeks.path("timeoutSeconds");
        if (!timeout.isMissingNode() && !timeout.isNull() && (!timeout.isIntegralNumber() || !timeout.canConvertToInt()
                || timeout.intValue() < 1)) {
            throw new IllegalArgumentException(SECTION + ".timeoutSeconds must be a whole number, 1 or more");
        }
        JsonNode seed = seeks.path("seed");
        if (!seed.isMissingNode() && !seed.isNull() && !seed.isIntegralNumber()) {
            throw new IllegalArgumentException(SECTION + ".seed must be a whole number");
        }
        for (String key : List.of("peek", "unloadFirst")) {
            JsonNode value = seeks.path(key);
            if (!value.isMissingNode() && !value.isNull() && !value.isBoolean()) {
                throw new IllegalArgumentException(SECTION + "." + key + " must be true or false");
            }
        }
        return new SeekSettings(count.asInt(DEFAULT_COUNT), seed.asLong(1), seeks.path("peek").asBoolean(true),
                seeks.path("unloadFirst").asBoolean(true), timeout.asInt(DEFAULT_TIMEOUT_SECONDS));
    }
}

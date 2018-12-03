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

package org.apache.pulsar.functions.utils;

import org.apache.pulsar.common.functions.WindowConfig;

public class WindowConfigUtils {

    public static final long DEFAULT_MAX_LAG_MS = 0; // no lag
    public static final long DEFAULT_WATERMARK_EVENT_INTERVAL_MS = 1000; // 1s

    public static void validate(WindowConfig windowConfig) {
        if (windowConfig.getWindowLengthDurationMs() == null && windowConfig.getWindowLengthCount() == null) {
            throw new IllegalArgumentException("Window length is not specified");
        }

        if (windowConfig.getWindowLengthDurationMs() != null && windowConfig.getWindowLengthCount() != null) {
            throw new IllegalArgumentException(
                    "Window length for time and count are set! Please set one or the other.");
        }

        if (windowConfig.getWindowLengthCount() != null) {
            if (windowConfig.getWindowLengthCount() <= 0) {
                throw new IllegalArgumentException(
                        "Window length must be positive [" + windowConfig.getWindowLengthCount() + "]");
            }
        }

        if (windowConfig.getWindowLengthDurationMs() != null) {
            if (windowConfig.getWindowLengthDurationMs() <= 0) {
                throw new IllegalArgumentException(
                        "Window length must be positive [" + windowConfig.getWindowLengthDurationMs() + "]");
            }
        }

        if (windowConfig.getSlidingIntervalCount() != null) {
            if (windowConfig.getSlidingIntervalCount() <= 0) {
                throw new IllegalArgumentException(
                        "Sliding interval must be positive [" + windowConfig.getSlidingIntervalCount() + "]");
            }
        }

        if (windowConfig.getSlidingIntervalDurationMs() != null) {
            if (windowConfig.getSlidingIntervalDurationMs() <= 0) {
                throw new IllegalArgumentException(
                        "Sliding interval must be positive [" + windowConfig.getSlidingIntervalDurationMs() + "]");
            }
        }

        if (windowConfig.getTimestampExtractorClassName() != null) {
            if (windowConfig.getMaxLagMs() != null) {
                if (windowConfig.getMaxLagMs() < 0) {
                    throw new IllegalArgumentException(
                            "Lag duration must be positive [" + windowConfig.getMaxLagMs() + "]");
                }
            }
            if (windowConfig.getWatermarkEmitIntervalMs() != null) {
                if (windowConfig.getWatermarkEmitIntervalMs() <= 0) {
                    throw new IllegalArgumentException(
                            "Watermark interval must be positive [" + windowConfig.getWatermarkEmitIntervalMs() + "]");
                }
            }
        }
    }

    public static void inferMissingArguments(WindowConfig windowConfig) {
        if (windowConfig.getWindowLengthDurationMs() != null && windowConfig.getSlidingIntervalDurationMs() == null) {
            windowConfig.setSlidingIntervalDurationMs(windowConfig.getWindowLengthDurationMs());
        }

        if (windowConfig.getWindowLengthCount() != null && windowConfig.getSlidingIntervalCount() == null) {
            windowConfig.setSlidingIntervalCount(windowConfig.getWindowLengthCount());
        }

        if (windowConfig.getTimestampExtractorClassName() != null) {
            if (windowConfig.getMaxLagMs() == null) {
                windowConfig.setMaxLagMs(DEFAULT_MAX_LAG_MS);
            }
            if (windowConfig.getWatermarkEmitIntervalMs() == null) {
                windowConfig.setWatermarkEmitIntervalMs(DEFAULT_WATERMARK_EVENT_INTERVAL_MS);
            }
        }
    }
}
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
package org.apache.pulsar.functions.windowing;

import org.apache.pulsar.functions.utils.WindowConfig;

public class WindowUtils {
    public static String getFullyQualifiedName(String tenant, String namespace, String name) {
        return String.format("%s/%s/%s", tenant, namespace, name);
    }

    public static void inferDefaultConfigs(WindowConfig windowConfig) {
        if (windowConfig.getWindowLengthDurationMs() != null && windowConfig.getSlidingIntervalDurationMs() == null) {
            windowConfig.setSlidingIntervalDurationMs(windowConfig.getWindowLengthDurationMs());
        }

        if (windowConfig.getWindowLengthCount() != null && windowConfig.getSlidingIntervalCount() == null) {
            windowConfig.setSlidingIntervalCount(windowConfig.getWindowLengthCount());
        }

        if (windowConfig.getTimestampExtractorClassName() != null) {
            if (windowConfig.getMaxLagMs() == null) {
                windowConfig.setMaxLagMs(WindowFunctionExecutor.DEFAULT_MAX_LAG_MS);
            }
            if (windowConfig.getWatermarkEmitIntervalMs() == null) {
                windowConfig.setWatermarkEmitIntervalMs(WindowFunctionExecutor.DEFAULT_WATERMARK_EVENT_INTERVAL_MS);
            }
        }
    }
}

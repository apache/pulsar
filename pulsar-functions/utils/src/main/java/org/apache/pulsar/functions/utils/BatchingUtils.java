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
package org.apache.pulsar.functions.utils;

import org.apache.pulsar.common.functions.BatchingConfig;
import org.apache.pulsar.functions.proto.Function;

public final class BatchingUtils {
    public static Function.BatchingSpec convert(BatchingConfig config) {
        if (config == null) {
            return null;
        }

        Function.BatchingSpec.Builder builder = Function.BatchingSpec.newBuilder()
                .setEnabled(config.isEnabled());

        if (config.getBatchingMaxPublishDelayMs() != null && config.getBatchingMaxPublishDelayMs() > 0) {
            builder.setBatchingMaxPublishDelayMs(config.getBatchingMaxPublishDelayMs());
        }
        if (config.getRoundRobinRouterBatchingPartitionSwitchFrequency() != null
                && config.getRoundRobinRouterBatchingPartitionSwitchFrequency() > 0) {
            builder.setRoundRobinRouterBatchingPartitionSwitchFrequency(
                    config.getRoundRobinRouterBatchingPartitionSwitchFrequency());
        }
        if (config.getBatchingMaxMessages() != null && config.getBatchingMaxMessages() > 0) {
            builder.setBatchingMaxMessages(config.getBatchingMaxMessages());
        }
        if (config.getBatchingMaxBytes() != null && config.getBatchingMaxBytes() > 0) {
            builder.setBatchingMaxBytes(config.getBatchingMaxBytes());
        }
        if (config.getBatchBuilder() != null && !config.getBatchBuilder().isEmpty()) {
            builder.setBatchBuilder(config.getBatchBuilder());
        }

        return builder.build();
    }

    public static BatchingConfig convertFromSpec(Function.BatchingSpec spec) {
        // to keep the backward compatibility, when batchingSpec is null or empty
        // the batching is enabled by default, and the default max publish delay is 10ms
        if (spec == null || spec.toString().equals("")) {
            return BatchingConfig.builder()
                    .enabled(true)
                    .batchingMaxPublishDelayMs(10)
                    .build();
        }

        BatchingConfig.BatchingConfigBuilder builder = BatchingConfig.builder()
                .enabled(spec.getEnabled());

        if (spec.getBatchingMaxPublishDelayMs() > 0) {
            builder.batchingMaxPublishDelayMs(spec.getBatchingMaxPublishDelayMs());
        }
        if (spec.getRoundRobinRouterBatchingPartitionSwitchFrequency() > 0) {
            builder.roundRobinRouterBatchingPartitionSwitchFrequency(
                    spec.getRoundRobinRouterBatchingPartitionSwitchFrequency());
        }
        if (spec.getBatchingMaxMessages() > 0) {
            builder.batchingMaxMessages(spec.getBatchingMaxMessages());
        }
        if (spec.getBatchingMaxBytes() > 0) {
            builder.batchingMaxBytes(spec.getBatchingMaxBytes());
        }
        if (spec.getBatchBuilder() != null && !spec.getBatchBuilder().isEmpty()) {
            builder.batchBuilder(spec.getBatchBuilder());
        }

        return builder.build();
    }
}

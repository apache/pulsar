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
package org.apache.pulsar.broker.web;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;

public class WebExecutorThreadPoolStats implements AutoCloseable {
    // Replaces ['pulsar_web_executor_max_threads', 'pulsar_web_executor_min_threads']
    public static final String LIMIT_COUNTER = "pulsar.web.executor.thread.limit";
    private final ObservableLongUpDownCounter limitCounter;

    // Replaces
    // ['pulsar_web_executor_active_threads', 'pulsar_web_executor_current_threads', 'pulsar_web_executor_idle_threads']
    public static final String USAGE_COUNTER = "pulsar.web.executor.thread.usage";
    private final ObservableLongUpDownCounter usageCounter;

    public static final AttributeKey<String> LIMIT_TYPE_KEY =
            AttributeKey.stringKey("pulsar.web.executor.thread.limit.type");
    @VisibleForTesting
    enum LimitType {
        MAX,
        MIN;
        public final Attributes attributes = Attributes.of(LIMIT_TYPE_KEY, name().toLowerCase());
    }

    public static final AttributeKey<String> USAGE_TYPE_KEY =
            AttributeKey.stringKey("pulsar.web.executor.thread.usage.type");
    @VisibleForTesting
    enum UsageType {
        ACTIVE,
        CURRENT,
        IDLE;
        public final Attributes attributes = Attributes.of(USAGE_TYPE_KEY, name().toLowerCase());
    }

    public WebExecutorThreadPoolStats(Meter meter, WebExecutorThreadPool executor) {
        limitCounter = meter
                .upDownCounterBuilder(LIMIT_COUNTER)
                .setUnit("{thread}")
                .setDescription("The thread limits for the pulsar-web executor pool.")
                .buildWithCallback(measurement -> {
                    measurement.record(executor.getMaxThreads(), LimitType.MAX.attributes);
                    measurement.record(executor.getMinThreads(), LimitType.MIN.attributes);
                });
        usageCounter = meter
                .upDownCounterBuilder(USAGE_COUNTER)
                .setUnit("{thread}")
                .setDescription("The current usage of threads in the pulsar-web executor pool.")
                .buildWithCallback(measurement -> {
                    var idleThreads = executor.getIdleThreads();
                    var currentThreads = executor.getThreads();
                    measurement.record(idleThreads, UsageType.IDLE.attributes);
                    measurement.record(currentThreads, UsageType.CURRENT.attributes);
                    measurement.record(currentThreads - idleThreads, UsageType.ACTIVE.attributes);
                });
    }

    @Override
    public synchronized void close() {
        limitCounter.close();
        usageCounter.close();
    }
}

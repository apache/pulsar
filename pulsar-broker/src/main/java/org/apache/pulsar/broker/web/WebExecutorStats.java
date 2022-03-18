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
package org.apache.pulsar.broker.web;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import java.util.concurrent.atomic.AtomicBoolean;

class WebExecutorStats implements AutoCloseable {
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);

    private final Gauge maxThreads;
    private final Gauge minThreads;
    private final Gauge idleThreads;
    private final Gauge activeThreads;
    private final Gauge currentThreads;
    private final WebExecutorThreadPool executor;

    private static volatile WebExecutorStats instance;

    static synchronized WebExecutorStats getStats(WebExecutorThreadPool executor) {
        if (null == instance) {
            instance = new WebExecutorStats(executor);
        }

        return instance;
    }

    private WebExecutorStats(WebExecutorThreadPool executor) {
        this.executor = executor;

        this.maxThreads = Gauge.build("pulsar_web_executor_max_threads", "-").create()
                .setChild(new Gauge.Child() {
                    public double get() {
                        return WebExecutorStats.this.executor.getMaxThreads();
                    }
                })
                .register();

        this.minThreads = Gauge.build("pulsar_web_executor_min_threads", "-").create()
                .setChild(new Gauge.Child() {
                    public double get() {
                        return WebExecutorStats.this.executor.getMinThreads();
                    }
                })
                .register();

        this.idleThreads = Gauge.build("pulsar_web_executor_idle_threads", "-").create()
                .setChild(new Gauge.Child() {
                    public double get() {
                        return WebExecutorStats.this.executor.getIdleThreads();
                    }
                })
                .register();

        this.activeThreads = Gauge.build("pulsar_web_executor_active_threads", "-").create()
                .setChild(new Gauge.Child() {
                    public double get() {
                        return WebExecutorStats.this.executor.getThreads()
                                - WebExecutorStats.this.executor.getIdleThreads();
                    }
                })
                .register();

        this.currentThreads = Gauge.build("pulsar_web_executor_current_threads", "-").create()
                .setChild(new Gauge.Child() {
                    public double get() {
                        return WebExecutorStats.this.executor.getThreads();
                    }
                })
                .register();
    }

    @Override
    public void close() throws Exception {
        if (CLOSED.compareAndSet(false, true)) {
            CollectorRegistry.defaultRegistry.unregister(this.activeThreads);
            CollectorRegistry.defaultRegistry.unregister(this.maxThreads);
            CollectorRegistry.defaultRegistry.unregister(this.minThreads);
            CollectorRegistry.defaultRegistry.unregister(this.idleThreads);
            CollectorRegistry.defaultRegistry.unregister(this.currentThreads);
        }
    }
}

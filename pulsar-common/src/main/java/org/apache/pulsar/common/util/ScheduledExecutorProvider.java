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
package org.apache.pulsar.common.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScheduledExecutorProvider extends ExecutorProvider {

    public ScheduledExecutorProvider(int numThreads, String poolName) {
        super(numThreads, poolName);
    }

    @Override
    protected ExecutorService createExecutor(ExtendedThreadFactory threadFactory) {
        return newSingleThreadScheduledExecutor(threadFactory);
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(ExtendedThreadFactory threadFactory) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
        ThreadPoolMonitor.registerSingleThreadExecutor(scheduler);
        return scheduler;
    }

    public static ScheduledExecutorService newScheduledThreadPool(int numThreads,
                                                                  ExtendedThreadFactory threadFactory) {
        ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(numThreads, threadFactory) {
            @Override
            protected void beforeExecute(Thread t, Runnable r) {
                ThreadMonitor.refreshThreadState(t, true);
            }

            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                ThreadMonitor.refreshThreadState(Thread.currentThread(), false);
            }
        };
        ThreadPoolMonitor.registerMultiThreadExecutor(scheduler, numThreads);

        return scheduler;
    }
}

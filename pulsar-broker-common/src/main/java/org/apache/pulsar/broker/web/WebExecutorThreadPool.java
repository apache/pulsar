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

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.pulsar.common.util.ThreadMonitor;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;

public class WebExecutorThreadPool extends ExecutorThreadPool {

    private final ThreadFactory threadFactory;

    public WebExecutorThreadPool(int maxThreads, String namePrefix) {
        this(maxThreads, namePrefix, 8192);
    }

    public WebExecutorThreadPool(int maxThreads, String namePrefix, int queueCapacity) {
        super(new ThreadPoolExecutor(maxThreads, maxThreads,
                60, TimeUnit.SECONDS,
                new BlockingArrayQueue<>(queueCapacity, queueCapacity)) {
            @Override
            protected void beforeExecute(Thread t, Runnable r) {
                ThreadMonitor.refreshThreadState(t);
            }

            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                ThreadMonitor.refreshThreadState(Thread.currentThread());
            }
        }, Math.min(8, maxThreads));

        this.threadFactory = new DefaultThreadFactory(namePrefix) {
            @Override
            protected Thread newThread(Runnable r, String name) {
                return new FastThreadLocalThread(threadGroup, r, name) {
                    @Override
                    public void run() {
                        super.run();

                        // execute when thread exit.
                        ThreadMonitor.remove(Thread.currentThread());
                    }
                };
            }
        };
    }

    @Override
    protected Thread newThread(Runnable job) {
        return threadFactory.newThread(job);
    }
}

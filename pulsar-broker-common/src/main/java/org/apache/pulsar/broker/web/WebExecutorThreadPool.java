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
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;

public class WebExecutorThreadPool extends ExecutorThreadPool {

    private static final ThreadLocal<WebExecutorThreadPool> CURRENT_POOL = new ThreadLocal<>();

    private final ThreadFactory threadFactory;
    private volatile boolean startAttempted;

    public WebExecutorThreadPool(int maxThreads, String namePrefix) {
        this(maxThreads, namePrefix, 8192);
    }

    public WebExecutorThreadPool(int maxThreads, String namePrefix, int queueCapacity) {
        super(maxThreads, Math.min(8, maxThreads), new BlockingArrayQueue<>(queueCapacity));
        this.threadFactory = new DefaultThreadFactory(namePrefix);
    }

    @Override
    protected Thread newThread(Runnable job) {
        return threadFactory.newThread(() -> {
            CURRENT_POOL.set(this);
            job.run();
        });
    }

    /**
     * Returns whether the calling thread is one of this pool's threads.
     */
    public boolean isPoolThread() {
        return CURRENT_POOL.get() == this;
    }

    @Override
    protected void doStart() throws Exception {
        startAttempted = true;
        super.doStart();
    }

    /**
     * {@link ExecutorThreadPool#join()} waits without a timeout for the executor to terminate, but the executor
     * is shut down only in {@link #doStop()}, which Jetty skips for a pool that was never started. That happens
     * when {@code Server.start()} fails to open a connector (e.g. the port is already in use), since connectors
     * are opened before the server's beans are started. Joining such a pool would block forever.
     */
    @Override
    public void join() throws InterruptedException {
        if (startAttempted) {
            super.join();
        }
    }
}

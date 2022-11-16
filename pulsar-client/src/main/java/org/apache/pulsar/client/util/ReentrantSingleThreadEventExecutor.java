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
package org.apache.pulsar.client.util;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

public class ReentrantSingleThreadEventExecutor extends SingleThreadEventExecutor {
    private static final int MAX_PENDING_TASKS = Integer.MAX_VALUE;

    public ReentrantSingleThreadEventExecutor() {
        this((EventExecutorGroup) null);
    }

    public ReentrantSingleThreadEventExecutor(ThreadFactory threadFactory) {
        this((EventExecutorGroup) null, (ThreadFactory) threadFactory);
    }

    public ReentrantSingleThreadEventExecutor(Executor executor) {
        this((EventExecutorGroup) null, (Executor) executor);
    }

    public ReentrantSingleThreadEventExecutor(EventExecutorGroup parent) {
        this(parent, (ThreadFactory) (new DefaultThreadFactory(DefaultEventExecutor.class)));
    }

    public ReentrantSingleThreadEventExecutor(EventExecutorGroup parent, ThreadFactory threadFactory) {
        super(parent, threadFactory, true, MAX_PENDING_TASKS, RejectedExecutionHandlers.reject());
    }

    public ReentrantSingleThreadEventExecutor(EventExecutorGroup parent, Executor executor) {
        super(parent, executor, true, MAX_PENDING_TASKS, RejectedExecutionHandlers.reject());
    }

    @Override
    protected void run() {
        do {
            Runnable task = this.takeTask();
            if (task != null) {
                runTask(task);
                this.updateLastExecutionTime();
            }
        } while(!this.confirmShutdown());

    }

    @Override
    public void execute(Runnable task) {
        if (inEventLoop()) {
            task.run();
        } else {
            super.execute(task);
        }
    }
}

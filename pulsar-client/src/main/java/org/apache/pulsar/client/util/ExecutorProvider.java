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
package org.apache.pulsar.client.util;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.Murmur3_32Hash;

@Slf4j
public class ExecutorProvider {
    private final int numThreads;
    private final List<Pair<ExecutorService, ExtendedThreadFactory>> executors;
    private final AtomicInteger currentThread = new AtomicInteger(0);
    private final String poolName;
    private volatile boolean isShutdown;

    public static class ExtendedThreadFactory extends DefaultThreadFactory {
        @Getter
        private volatile Thread thread;
        public ExtendedThreadFactory(String poolName) {
            super(poolName, false);
        }
        public ExtendedThreadFactory(String poolName, boolean daemon) {
            super(poolName, daemon);
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = super.newThread(r);
            thread.setUncaughtExceptionHandler((t, e) ->
                    log.error("Thread {} got uncaught Exception", t.getName(), e));
            this.thread = thread;
            return thread;
        }
    }

    public ExecutorProvider(int numThreads, String poolName) {
        checkArgument(numThreads > 0);
        this.numThreads = numThreads;
        Objects.requireNonNull(poolName);
        executors = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            ExtendedThreadFactory threadFactory = new ExtendedThreadFactory(
                    poolName, Thread.currentThread().isDaemon());
            ExecutorService executor = createExecutor(threadFactory);
            executors.add(Pair.of(executor, threadFactory));
        }
        isShutdown = false;
        this.poolName = poolName;
    }

    protected ExecutorService createExecutor(ExtendedThreadFactory threadFactory) {
       return Executors.newSingleThreadExecutor(threadFactory);
    }

    public ExecutorService getExecutor() {
        return executors.get((currentThread.getAndIncrement() & Integer.MAX_VALUE) % numThreads).getKey();
    }

    public ExecutorService getExecutor(Object object) {
        return getExecutorInternal(object == null ? -1 : object.hashCode() & Integer.MAX_VALUE);
    }

    public ExecutorService getExecutor(byte[] bytes) {
        int keyHash = Murmur3_32Hash.getInstance().makeHash(bytes);
        return getExecutorInternal(keyHash);
    }

    private ExecutorService getExecutorInternal(int hash) {
        return executors.get((hash & Integer.MAX_VALUE) % numThreads).getKey();
    }

    public void shutdownNow() {
        executors.forEach(entry -> {
            ExecutorService executor = entry.getKey();
            ExtendedThreadFactory threadFactory = entry.getValue();
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.warn("Failed to terminate executor with pool name {} within timeout. The following are stack"
                            + " traces of still running threads.\n{}",
                            poolName, getThreadDump(threadFactory.getThread()));
                }
            } catch (InterruptedException e) {
                log.warn("Shutdown of thread pool was interrupted");
            }
        });
        isShutdown = true;
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    private String getThreadDump(Thread thread) {
        StringBuilder dump = new StringBuilder();
        dump.append('\n');
        dump.append(String.format("\"%s\" %s prio=%d tid=%d %s%njava.lang.Thread.State: %s", thread.getName(),
                (thread.isDaemon() ? "daemon" : ""), thread.getPriority(), thread.getId(),
                Thread.State.WAITING.equals(thread.getState()) ? "in Object.wait()" : thread.getState().name(),
                Thread.State.WAITING.equals(thread.getState()) ? "WAITING (on object monitor)"
                        : thread.getState()));
        for (StackTraceElement stackTraceElement : thread.getStackTrace()) {
            dump.append("\n        at ");
            dump.append(stackTraceElement);
        }
        dump.append("\n");
        return dump.toString();
    }
}

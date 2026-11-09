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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.common.util.PulsarExecutors;

@CustomLog
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
                    log.error().attr("thread", t.getName()).exception(e).log("Thread got uncaught Exception"));
            this.thread = thread;
            return thread;
        }
    }

    /**
     * A task that {@link #shutdownNow()} doesn't discard. If it was given to an executor with {@code execute()} and
     * is still queued when the executor is shut down, it runs on the thread calling {@code shutdownNow()} once the
     * executor has terminated, so never concurrently with the executor's other tasks. For cleanup that must happen
     * even if the executor is busy when it is shut down. A {@link ScheduledExecutorProvider} doesn't support it:
     * its executors wrap the tasks they queue.
     */
    public interface CleanupTask extends Runnable {
    }

    public ExecutorProvider(int numThreads, String poolName) {
        this(numThreads, poolName, Thread.currentThread().isDaemon());
    }

    public ExecutorProvider(int numThreads, String poolName, boolean daemon) {
        this(numThreads, poolName, daemon, ExtendedThreadFactory::new);
    }

    @VisibleForTesting
    public ExecutorProvider(
            int numThreads, String poolName, boolean daemon,
            BiFunction<String/* poolName */, Boolean/* daemon */, ExtendedThreadFactory> threadFactoryCreator) {
        checkArgument(numThreads > 0);
        this.numThreads = numThreads;
        Objects.requireNonNull(poolName);
        executors = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            ExtendedThreadFactory threadFactory = threadFactoryCreator.apply(poolName, daemon);
            ExecutorService executor = createExecutor(threadFactory);
            executors.add(Pair.of(executor, threadFactory));
        }
        isShutdown = false;
        this.poolName = poolName;
    }

    protected ExecutorService createExecutor(ExtendedThreadFactory threadFactory) {
        return PulsarExecutors.newSingleThreadExecutor(threadFactory);
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
            List<Runnable> discardedTasks = executor.shutdownNow();
            try {
                if (executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    runCleanupTasks(discardedTasks);
                } else {
                    log.warn().attr("name", poolName)
                            .attr("threads", getThreadDump(threadFactory.getThread()))
                            .log("Failed to terminate executor with pool"
                                    + " name within timeout. The following are"
                                    + " stack traces of still running"
                                    + " threads.\n");
                }
            } catch (InterruptedException e) {
                log.warn("Shutdown of thread pool was interrupted");
            }
        });
        isShutdown = true;
    }

    // Only called once the executor that discarded the tasks has terminated.
    private void runCleanupTasks(List<Runnable> discardedTasks) {
        for (Runnable task : discardedTasks) {
            if (task instanceof CleanupTask) {
                try {
                    task.run();
                } catch (Throwable t) {
                    log.warn().attr("name", poolName).exception(t)
                            .log("Cleanup task failed after the executor was shut down");
                }
            }
        }
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

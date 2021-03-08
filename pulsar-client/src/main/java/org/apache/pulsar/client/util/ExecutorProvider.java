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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.Murmur3_32Hash;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class ExecutorProvider {
    private final int numThreads;
    private final List<ExecutorService> executors;
    private final AtomicInteger currentThread = new AtomicInteger(0);
    private final String poolName;
    private volatile boolean isShutdown;

    public ExecutorProvider(int numThreads, String poolName) {
        checkArgument(numThreads > 0);
        this.numThreads = numThreads;
        checkNotNull(poolName);
        executors = Lists.newArrayListWithCapacity(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executors.add(Executors.newSingleThreadScheduledExecutor(PulsarClientImpl.getThreadFactory(poolName)));
        }
        isShutdown = false;
        this.poolName = poolName;
    }

    public ExecutorService getExecutor() {
        return executors.get((currentThread.getAndIncrement() & Integer.MAX_VALUE) % numThreads);
    }

    public ExecutorService getExecutor(Object object) {
        return getExecutorInternal(object == null ? -1 : object.hashCode() & Integer.MAX_VALUE);
    }

    public ExecutorService getExecutor(byte[] bytes) {
        int keyHash = Murmur3_32Hash.getInstance().makeHash(bytes);
        return getExecutorInternal(keyHash);
    }

    private ExecutorService getExecutorInternal(int hash) {
        return executors.get((hash & Integer.MAX_VALUE) % numThreads);
    }

    public void shutdownNow() {
        executors.forEach(executor -> {
            executor.shutdownNow();
            try {
                if(!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.error("Failed to terminate executor with pool name {} within timeout. The following are stack traces of still running threads.\n{}", poolName, getThreadDump());
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

    private String getThreadDump() {
        List<Map.Entry<Thread, StackTraceElement[]>> activeThreads =
                Thread.getAllStackTraces().entrySet().stream()
                        .filter(threadEntry -> threadEntry.getKey().getName().startsWith(poolName))
                        .collect(Collectors.toList());

        StringBuilder dump = new StringBuilder();
        
        for (Map.Entry<Thread, StackTraceElement[]> e : activeThreads) {
            Thread thread = e.getKey();
            dump.append('\n');
            dump.append(String.format("\"%s\" %s prio=%d tid=%d %s%njava.lang.Thread.State: %s", thread.getName(),
                    (thread.isDaemon() ? "daemon" : ""), thread.getPriority(), thread.getId(),
                    Thread.State.WAITING.equals(thread.getState()) ? "in Object.wait()" : thread.getState().name(),
                    Thread.State.WAITING.equals(thread.getState()) ? "WAITING (on object monitor)"
                            : thread.getState()));
            for (StackTraceElement stackTraceElement : e.getValue()) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n");
        }
        return dump.toString();
    }
}

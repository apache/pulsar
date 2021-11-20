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
package org.apache.pulsar.functions.windowing.triggers;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.ThreadContext;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.windowing.DefaultEvictionContext;
import org.apache.pulsar.functions.windowing.Event;
import org.apache.pulsar.functions.windowing.EvictionPolicy;
import org.apache.pulsar.functions.windowing.TriggerHandler;
import org.apache.pulsar.functions.windowing.TriggerPolicy;
import org.apache.pulsar.functions.windowing.WindowUtils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Invokes {@link TriggerHandler#onTrigger()} after the duration.
 */

@Slf4j
public class TimeTriggerPolicy<T> implements TriggerPolicy<T, Void> {

    private long duration;
    private final TriggerHandler handler;
    private final EvictionPolicy<T, ?> evictionPolicy;
    private ScheduledFuture<?> executorFuture;
    private final ScheduledExecutorService executor;
    private Context context;

    public TimeTriggerPolicy(long millis, TriggerHandler handler, EvictionPolicy<T, ?>
            evictionPolicy, Context context) {
        this.duration = millis;
        this.handler = handler;
        this.evictionPolicy = evictionPolicy;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("time-trigger-policy-%d")
                .setDaemon(true)
                .build();
        this.executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        this.context = context;
    }

    @Override
    public void track(Event<T> event) {
        checkFailures();
    }

    @Override
    public void reset() {

    }

    @Override
    public void start() {
        executorFuture =
                executor.scheduleAtFixedRate(catchingAndLoggingThrowables(newTriggerTask()), duration, duration,
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "TimeTriggerPolicy{" + "duration=" + duration + '}';
    }

    private Runnable newTriggerTask() {
        return new Runnable() {
            @Override
            public void run() {
                // initialize the thread context
                ThreadContext.put("function", WindowUtils.getFullyQualifiedName(
                        context.getTenant(), context.getNamespace(), context.getFunctionName()));
                // do not process current timestamp since tuples might arrive while the trigger is executing
                long now = System.currentTimeMillis() - 1;
                try {
                    /*
                     * set the current timestamp as the reference time for the eviction policy
                     * to evict the events
                     */
                    evictionPolicy.setContext(new DefaultEvictionContext(now, null, null, duration));
                    handler.onTrigger();
                } catch (Throwable th) {
                    log.error("handler.onTrigger failed ", th);
                    /*
                     * propagate it so that task gets canceled and the exception
                     * can be retrieved from executorFuture.get()
                     */
                    throw th;
                }
            }
        };
    }

    private void checkFailures() {
        if (executorFuture != null && executorFuture.isDone()) {
            try {
                executorFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Got exception in timer trigger policy ", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Void getState() {
        return null;
    }

    @Override
    public void restoreState(Void state) {

    }
}

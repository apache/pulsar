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
package org.apache.pulsar.functions.windowing;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.ThreadContext;
import org.apache.pulsar.functions.api.Context;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Tracks tuples across input topics and periodically emits watermark events.
 * Watermark event timestamp is the minimum of the latest tuple timestamps
 * across all the input topics (minus the lag). Once a watermark event is emitted
 * any tuple coming with an earlier timestamp can be considered as late events.
 */
@Slf4j
public class WaterMarkEventGenerator<T> implements Runnable {
    private final WindowManager<T> windowManager;
    private final long eventTsLagMs;
    private final Set<String> inputTopics;
    private final Map<String, Long> topicToTs;
    private final ScheduledExecutorService executorService;
    private final long intervalMs;
    private ScheduledFuture<?> executorFuture;
    private volatile long lastWaterMarkTs;
    private Context context;

    /**
     * Creates a new WatermarkEventGenerator.
     *
     * @param windowManager The window manager this generator will submit watermark events to
     * @param intervalMs The generator will check if it should generate a watermark event with this intervalMs
     * @param eventTsLagMs The max allowed lag behind the last watermark event before an event is considered late
     * @param inputTopics The input topics this generator is expected to handle
     */
    public WaterMarkEventGenerator(WindowManager<T> windowManager, long intervalMs,
                                   long eventTsLagMs, Set<String> inputTopics, Context context) {
        this.windowManager = windowManager;
        topicToTs = new ConcurrentHashMap<>();

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("watermark-event-generator-%d")
                .setDaemon(true)
                .build();
        executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);

        this.intervalMs = intervalMs;
        this.eventTsLagMs = eventTsLagMs;
        this.inputTopics = inputTopics;
        this.context = context;
    }

    /**
     * Tracks the timestamp of the event from a topic, returns
     * true if the event can be considered for processing or
     * false if its a late event.
     */
    public boolean track(String inputTopic, long ts) {
        Long currentVal = topicToTs.get(inputTopic);
        if (currentVal == null || ts > currentVal) {
            topicToTs.put(inputTopic, ts);
        }
        checkFailures();
        return ts >= lastWaterMarkTs;
    }

    @Override
    public void run() {
        // initialize the thread context
        ThreadContext.put("function", WindowUtils.getFullyQualifiedName(
                context.getTenant(), context.getNamespace(), context.getFunctionName()));
        try {
            long waterMarkTs = computeWaterMarkTs();
            if (waterMarkTs > lastWaterMarkTs) {
                windowManager.add(new WaterMarkEvent<>(waterMarkTs));
                lastWaterMarkTs = waterMarkTs;
            }
        } catch (Throwable th) {
            log.error("Failed while processing watermark event ", th);
            throw th;
        }
    }

    /**
     * Computes the min ts across all input topics.
     */
    private long computeWaterMarkTs() {
        long ts = 0;
        // only if some data has arrived on each input topic
        if (topicToTs.size() >= inputTopics.size()) {
            ts = Long.MAX_VALUE;
            for (Map.Entry<String, Long> entry : topicToTs.entrySet()) {
                ts = Math.min(ts, entry.getValue());
            }
        }
        return ts - eventTsLagMs;
    }

    private void checkFailures() {
        if (executorFuture != null && executorFuture.isDone()) {
            try {
                executorFuture.get();
            } catch (InterruptedException | ExecutionException ex) {
                log.error("Got exception ", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    public void start() {
        this.executorFuture =
                executorService.scheduleAtFixedRate(catchingAndLoggingThrowables(this), intervalMs, intervalMs,
                        TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        log.debug("Shutting down WaterMarkEventGenerator");
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

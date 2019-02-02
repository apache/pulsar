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

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.functions.api.*;
import org.apache.pulsar.functions.windowing.evictors.CountEvictionPolicy;
import org.apache.pulsar.functions.windowing.evictors.TimeEvictionPolicy;
import org.apache.pulsar.functions.windowing.evictors.WatermarkCountEvictionPolicy;
import org.apache.pulsar.functions.windowing.evictors.WatermarkTimeEvictionPolicy;
import org.apache.pulsar.functions.windowing.triggers.CountTriggerPolicy;
import org.apache.pulsar.functions.windowing.triggers.TimeTriggerPolicy;
import org.apache.pulsar.functions.windowing.triggers.WatermarkCountTriggerPolicy;
import org.apache.pulsar.functions.windowing.triggers.WatermarkTimeTriggerPolicy;
import org.apache.pulsar.io.core.BatchedSink;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Slf4j
public class BatchedSinkExecutor<T> implements Sink<T> {

    protected WindowConfig windowConfig;
    private WindowManager<Record<T>> windowManager;
    private TimestampExtractor<T> timestampExtractor;
    protected transient WaterMarkEventGenerator<Record<T>> waterMarkEventGenerator;

    protected BatchedSink<T> batchedSink;

    public BatchedSinkExecutor(BatchedSink<T> batchedSink) {
        this.batchedSink = batchedSink;
    }

    private WindowManager<Record<T>> getWindowManager(WindowConfig windowConfig, SinkContext context) {

        WindowLifecycleListener<Event<Record<T>>> lifecycleListener = newWindowLifecycleListener(context);
        WindowManager<Record<T>> manager = new WindowManager<>(lifecycleListener, new ConcurrentLinkedQueue<>());

        if (this.windowConfig.getTimestampExtractorClassName() != null) {
            this.timestampExtractor = getTimeStampExtractor(windowConfig);

            waterMarkEventGenerator = new WaterMarkEventGenerator<>(manager, this.windowConfig
                    .getWatermarkEmitIntervalMs(),
                    this.windowConfig.getMaxLagMs(), new HashSet<>(context.getInputTopics()),
                    context.getTenant(), context.getNamespace(), context.getSinkName());
        } else {
            if (this.windowConfig.getLateDataTopic() != null) {
                throw new IllegalArgumentException(
                        "Late data topic can be defined only when specifying a timestamp extractor class");
            }
        }

        EvictionPolicy<Record<T>, ?> evictionPolicy = getEvictionPolicy(windowConfig);
        TriggerPolicy<Record<T>, ?> triggerPolicy = getTriggerPolicy(windowConfig, manager,
                evictionPolicy, context);
        manager.setEvictionPolicy(evictionPolicy);
        manager.setTriggerPolicy(triggerPolicy);

        return manager;
    }

    private TimestampExtractor<T> getTimeStampExtractor(WindowConfig windowConfig) {

        Class<?> theCls;
        try {
            theCls = Class.forName(windowConfig.getTimestampExtractorClassName(),
                    true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(
                    String.format("Timestamp extractor class %s must be in class path",
                            windowConfig.getTimestampExtractorClassName()), cnfe);
        }

        Object result;
        try {
            Constructor<?> constructor = theCls.getDeclaredConstructor();
            constructor.setAccessible(true);
            result = constructor.newInstance();
        } catch (InstantiationException ie) {
            throw new RuntimeException("User class must be concrete", ie);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("User class doesn't have such method", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("User class must have a no-arg constructor", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("User class constructor throws exception", e);
        }
        Class<?>[] timestampExtractorTypeArgs = TypeResolver.resolveRawArguments(
                TimestampExtractor.class, result.getClass());
        Class<?>[] typeArgs = TypeResolver.resolveRawArguments(Function.class, this.getClass());
        if (!typeArgs[0].equals(timestampExtractorTypeArgs[0])) {
            throw new RuntimeException(
                    "Inconsistent types found between function input type and timestamp extractor type: "
                            + " function type = " + typeArgs[0] + ", timestamp extractor type = "
                            + timestampExtractorTypeArgs[0]);
        }
        return (TimestampExtractor<T>) result;
    }

    private TriggerPolicy<Record<T>, ?> getTriggerPolicy(WindowConfig windowConfig, WindowManager<Record<T>> manager,
                                                 EvictionPolicy<Record<T>, ?> evictionPolicy, SinkContext context) {
        if (windowConfig.getSlidingIntervalCount() != null) {
            if (this.isEventTime()) {
                return new WatermarkCountTriggerPolicy<>(
                        windowConfig.getSlidingIntervalCount(), manager, evictionPolicy, manager);
            } else {
                return new CountTriggerPolicy<>(windowConfig.getSlidingIntervalCount(), manager, evictionPolicy);
            }
        } else {
            if (this.isEventTime()) {
                return new WatermarkTimeTriggerPolicy<>(windowConfig.getSlidingIntervalDurationMs(), manager,
                        evictionPolicy, manager);
            }
            return new TimeTriggerPolicy<>(windowConfig.getSlidingIntervalDurationMs(), manager,
                    evictionPolicy, context.getTenant(), context.getNamespace(), context.getSinkName());
        }
    }

    private EvictionPolicy<Record<T>, ?> getEvictionPolicy(WindowConfig windowConfig) {
        if (windowConfig.getWindowLengthCount() != null) {
            if (this.isEventTime()) {
                return new WatermarkCountEvictionPolicy<>(windowConfig.getWindowLengthCount());
            } else {
                return new CountEvictionPolicy<>(windowConfig.getWindowLengthCount());
            }
        } else {
            if (this.isEventTime()) {
                return new WatermarkTimeEvictionPolicy<>(
                        windowConfig.getWindowLengthDurationMs(), windowConfig.getMaxLagMs());
            } else {
                return new TimeEvictionPolicy<>(windowConfig.getWindowLengthDurationMs());
            }
        }
    }

    protected WindowLifecycleListener<Event<Record<T>>> newWindowLifecycleListener(SinkContext context) {
        return new WindowLifecycleListener<Event<Record<T>>>() {
            @Override
            public void onExpiry(List<Event<Record<T>>> events) {
                for (Event<Record<T>> event : events) {
                    event.getRecord().ack();
                }
            }

            @Override
            public void onActivation(List<Event<Record<T>>> tuples, List<Event<Record<T>>> newTuples, List<Event<Record<T>>>
                    expiredTuples, Long referenceTime) {
                processWindow(
                        context,
                        tuples.stream().map(event -> event.get()).collect(Collectors.toList()),
                        newTuples.stream().map(event -> event.get()).collect(Collectors.toList()),
                        expiredTuples.stream().map(event -> event.get()).collect(Collectors.toList()),
                        referenceTime);
            }
        };
    }

    private void processWindow(SinkContext context, List<Record<T>> tuples, List<Record<T>> newTuples, List<Record<T>>
            expiredTuples, Long referenceTime) {

        try {
            this.batchedSink.write(tuples);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Long getWindowStartTs(Long endTs) {
        Long res = null;
        if (endTs != null && this.windowConfig.getWindowLengthDurationMs() != null) {
            res = endTs - this.windowConfig.getWindowLengthDurationMs();
        }
        return res;
    }

    private void start() {
        if (this.waterMarkEventGenerator != null) {
            log.debug("Starting waterMarkEventGenerator");
            this.waterMarkEventGenerator.start();
        }

        log.debug("Starting trigger policy");
        this.windowManager.triggerPolicy.start();
    }

    @Override
    public void close() {
        if (this.waterMarkEventGenerator != null) {
            this.waterMarkEventGenerator.shutdown();
        }
        if (this.windowManager != null) {
            this.windowManager.shutdown();
        }
    }

    private boolean isEventTime() {
        return this.timestampExtractor != null;
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        if (config.containsKey(WindowConfig.WINDOW_CONFIG_KEY)) {
            this.windowConfig = new Gson().fromJson(
                    (new Gson().toJson(config.get(WindowConfig.WINDOW_CONFIG_KEY))),
                    WindowConfig.class);
            config.remove(WindowConfig.WINDOW_CONFIG_KEY);
        }
        log.info("Window Config: {}", this.windowConfig);
        this.batchedSink.open(config, sinkContext);
        this.windowManager = this.getWindowManager(this.windowConfig, sinkContext);
        this.start();
    }

    @Override
    public void write(Record<T> record) throws Exception {
        if (isEventTime()) {
            long ts = this.timestampExtractor.extractTimestamp(record.getValue());
            if (this.waterMarkEventGenerator.track(record.getTopicName().get(), ts)) {
                this.windowManager.add(record, ts, record);
            } else {
                record.ack();
            }
        } else {
            this.windowManager.add(record, System.currentTimeMillis(), record);
        }
    }
}

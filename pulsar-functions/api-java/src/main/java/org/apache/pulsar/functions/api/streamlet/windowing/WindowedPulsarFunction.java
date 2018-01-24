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
package org.apache.pulsar.functions.api.streamlet.windowing;

import net.jodah.typetools.TypeResolver;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.api.streamlet.windowing.evictors.CountEvictionPolicy;
import org.apache.pulsar.functions.api.streamlet.windowing.evictors.TimeEvictionPolicy;
import org.apache.pulsar.functions.api.streamlet.windowing.evictors.WatermarkCountEvictionPolicy;
import org.apache.pulsar.functions.api.streamlet.windowing.evictors.WatermarkTimeEvictionPolicy;
import org.apache.pulsar.functions.api.streamlet.windowing.triggers.CountTriggerPolicy;
import org.apache.pulsar.functions.api.streamlet.windowing.triggers.TimeTriggerPolicy;
import org.apache.pulsar.functions.api.streamlet.windowing.triggers.WatermarkCountTriggerPolicy;
import org.apache.pulsar.functions.api.streamlet.windowing.triggers.WatermarkTimeTriggerPolicy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Slf4j
public abstract class WindowedPulsarFunction<I, O> implements PulsarFunction<I, O> {

    private boolean initialized;
    private WindowConfig windowConfig;
    private WindowManager<I> windowManager;
    private TimestampExtractor<I> timestampExtractor;
    protected transient WaterMarkEventGenerator<I> waterMarkEventGenerator;

    private static final long DEFAULT_MAX_LAG_MS = 0; // no lag
    private static final long DEFAULT_WATERMARK_EVENT_INTERVAL_MS = 1000; // 1s

    public void initialize(Context context) {
        this.windowConfig = this.getWindowConfigs(context);
        log.info("Window Config: {}", this.windowConfig);
        this.windowManager = this.getWindowManager(this.windowConfig, context);
        this.initialized = true;
        this.start();
    }

    private WindowConfig getWindowConfigs(Context context) {
        WindowConfig windowConfig = new WindowConfig();
        if (context.getUserConfigValue("windowLengthCount") != null) {
            windowConfig.setWindowLengthCount(Integer.parseInt(context.getUserConfigValue("windowLengthCount")));
        }
        if (context.getUserConfigValue("windowLengthDurationMs") != null) {
            windowConfig.setWindowLengthDurationMs(Long.parseLong(
                    context.getUserConfigValue("windowLengthDurationMs")));
        }
        if (context.getUserConfigValue("slidingIntervalCount") != null) {
            windowConfig.setSlidingIntervalCount(Integer.parseInt(context.getUserConfigValue("slidingIntervalCount")));
        }
        if (context.getUserConfigValue("slidingIntervalDurationMs") != null) {
            windowConfig.setSlidingDurationMs(Long.parseLong(context.getUserConfigValue("slidingIntervalDurationMs")));
        }
        if (context.getUserConfigValue("lateDataTopic") != null) {
            windowConfig.setLateDataTopic(context.getUserConfigValue("lateDataTopic"));
        }
        if (context.getUserConfigValue("maxLagMs") != null) {
            windowConfig.setMaxLagMs(Long.parseLong(context.getUserConfigValue("maxLagMs")));
        }
        if (context.getUserConfigValue("watermarkEmitIntervalMs") != null) {
            windowConfig.setWatermarkEmitIntervalMs(Long.parseLong(
                    context.getUserConfigValue("watermarkEmitIntervalMs")));
        }
        if (context.getUserConfigValue("timestampExtractorClassName") != null) {
            windowConfig.setTimestampExtractorClassName(context.getUserConfigValue("timestampExtractorClassName"));
        }

        validateAndSetDefaultsWindowConfig(windowConfig);
        return windowConfig;
    }

    private static void validateAndSetDefaultsWindowConfig(WindowConfig windowConfig) {
        if (windowConfig.getWindowLengthDurationMs() == null && windowConfig.getWindowLengthCount() == null) {
            throw new IllegalArgumentException("Window length is not specified");
        }

        if (windowConfig.getWindowLengthDurationMs() != null && windowConfig.getWindowLengthCount() != null) {
            throw new IllegalArgumentException(
                    "Window length for time and count are set! Please set one or the other.");
        }

        if (windowConfig.getWindowLengthDurationMs() != null && windowConfig.getSlidingDurationMs() == null) {
            windowConfig.setSlidingDurationMs(windowConfig.getWindowLengthDurationMs());
        }

        if (windowConfig.getWindowLengthCount() != null && windowConfig.getSlidingIntervalCount() == null) {
            windowConfig.setSlidingIntervalCount(windowConfig.getWindowLengthCount());
        }

        if (windowConfig.getTimestampExtractorClassName() != null) {
            if (windowConfig.getMaxLagMs() == null) {
                windowConfig.setMaxLagMs(DEFAULT_MAX_LAG_MS);
            }
            if (windowConfig.getWatermarkEmitIntervalMs() == null) {
                windowConfig.setWatermarkEmitIntervalMs(DEFAULT_WATERMARK_EVENT_INTERVAL_MS);
            }
        }
    }

    private WindowManager<I> getWindowManager(WindowConfig windowConfig, Context context) {

        WindowLifecycleListener<Event<I>> lifecycleListener = newWindowLifecycleListener(context);
        WindowManager<I> manager = new WindowManager<>(lifecycleListener, new ConcurrentLinkedQueue<>());

        if (this.windowConfig.getTimestampExtractorClassName() != null) {
            this.timestampExtractor = getTimeStampExtractor(windowConfig);

            waterMarkEventGenerator = new WaterMarkEventGenerator<>(manager, this.windowConfig.getWatermarkEmitIntervalMs(),
                    this.windowConfig.getMaxLagMs(), new HashSet<>(context.getSourceTopics()), context);
        } else {
            if (this.windowConfig.getLateDataTopic() != null) {
                throw new IllegalArgumentException(
                        "Late data topic can be defined only when specifying a timestamp extractor class");
            }
        }

        EvictionPolicy<I, ?> evictionPolicy = getEvictionPolicy(windowConfig);
        TriggerPolicy<I, ?> triggerPolicy = getTriggerPolicy(windowConfig, manager,
                evictionPolicy, context);
        manager.setEvictionPolicy(evictionPolicy);
        manager.setTriggerPolicy(triggerPolicy);

        return manager;
    }

    private TimestampExtractor<I> getTimeStampExtractor(WindowConfig windowConfig) {

        Class<?> theCls;
        try {
            theCls = Class.forName(windowConfig.getTimestampExtractorClassName(),
                    true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Timestamp extractor class must be in class path", cnfe);
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
        Class<?>[] typeArgs = TypeResolver.resolveRawArguments(PulsarFunction.class, this.getClass());
        if (!typeArgs[0].equals(timestampExtractorTypeArgs[0])) {
            throw new RuntimeException(
                    "Inconsistent types found between function input type and timestamp extractor type: "
                            + " function type = " + typeArgs[0] + ", timestamp extractor type = "
                            + timestampExtractorTypeArgs[0]);
        }
        return (TimestampExtractor<I>) result;
    }

    private TriggerPolicy<I, ?> getTriggerPolicy(WindowConfig windowConfig, WindowManager<I> manager,
                                                 EvictionPolicy<I, ?> evictionPolicy, Context context) {
        if (windowConfig.getSlidingIntervalCount() != null) {
            if (this.isEventTime()) {
                return new WatermarkCountTriggerPolicy<>(
                        windowConfig.getSlidingIntervalCount(), manager, evictionPolicy, manager);
            } else {
                return new CountTriggerPolicy<>(windowConfig.getSlidingIntervalCount(), manager, evictionPolicy);
            }
        } else {
            if (this.isEventTime()) {
                return new WatermarkTimeTriggerPolicy<>(windowConfig.getSlidingDurationMs(), manager, evictionPolicy, manager);
            }
            return new TimeTriggerPolicy<>(windowConfig.getSlidingDurationMs(), manager,
                    evictionPolicy, context);
        }
    }

    private EvictionPolicy<I, ?> getEvictionPolicy(WindowConfig windowConfig) {
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

    protected WindowLifecycleListener<Event<I>> newWindowLifecycleListener(Context context) {
        return new WindowLifecycleListener<Event<I>>() {
            @Override
            public void onExpiry(List<Event<I>> events) {
                for (Event<I> event : events) {
                    context.ack(event.getMessageId(), event.getTopic());
                }
            }

            @Override
            public void onActivation(List<Event<I>> tuples, List<Event<I>> newTuples, List<Event<I>>
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

    private void processWindow(Context context, List<I> tuples, List<I> newTuples, List<I>
            expiredTuples, Long referenceTime) {

        O output = null;
        try {
            output = this.handleRequest(
                    new WindowImpl<>(tuples, newTuples, expiredTuples, getWindowStartTs(referenceTime), referenceTime),
                    new WindowContextImpl(context));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (output != null) {
            context.publish(context.getSinkTopic(), output, context.getOutputSerdeClass());
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

    private boolean isEventTime() {
        return this.timestampExtractor != null;
    }

    @Override
    public O process(I input, Context context) throws Exception {
        if (!this.initialized) {
            initialize(context);
        }
        if (isEventTime()) {
            long ts = this.timestampExtractor.extractTimestamp(input);
            if (this.waterMarkEventGenerator.track(context.getTopicName(), ts)) {
                this.windowManager.add(input, ts, context.getMessageId(), context.getTopicName());
            } else {
                if (this.windowConfig.getLateDataTopic() != null) {
                    context.publish(this.windowConfig.getLateDataTopic(), input, context.getOutputSerdeClass());
                } else {
                    log.info(String.format(
                            "Received a late tuple %s with ts %d. This will not be " + "processed"
                                    + ".", input, ts));
                }
                context.ack(context.getMessageId(), context.getTopicName());
            }
        } else {
            this.windowManager.add(input, System.currentTimeMillis(), context.getMessageId(), context.getTopicName());
        }
        return null;
    }

    public abstract O handleRequest(Window<I> inputWindow, WindowContext context) throws Exception;
}

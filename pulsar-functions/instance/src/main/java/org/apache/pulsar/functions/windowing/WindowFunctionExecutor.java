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
package org.apache.pulsar.functions.windowing;

import com.google.gson.Gson;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.WindowContext;
import org.apache.pulsar.functions.api.WindowFunction;
import org.apache.pulsar.functions.windowing.evictors.CountEvictionPolicy;
import org.apache.pulsar.functions.windowing.evictors.TimeEvictionPolicy;
import org.apache.pulsar.functions.windowing.evictors.WatermarkCountEvictionPolicy;
import org.apache.pulsar.functions.windowing.evictors.WatermarkTimeEvictionPolicy;
import org.apache.pulsar.functions.windowing.triggers.CountTriggerPolicy;
import org.apache.pulsar.functions.windowing.triggers.TimeTriggerPolicy;
import org.apache.pulsar.functions.windowing.triggers.WatermarkCountTriggerPolicy;
import org.apache.pulsar.functions.windowing.triggers.WatermarkTimeTriggerPolicy;

@Slf4j
public class WindowFunctionExecutor<T, X> implements Function<T, X> {

    private boolean initialized;
    protected WindowConfig windowConfig;
    private WindowManager<Record<T>> windowManager;
    private TimestampExtractor<T> timestampExtractor;
    protected transient WaterMarkEventGenerator<Record<T>> waterMarkEventGenerator;

    protected java.util.function.Function<Collection<T>, X> bareWindowFunction;
    protected WindowFunction<T, X> windowFunction;

    public void initialize(Context context) {
        this.windowConfig = this.getWindowConfigs(context);
        initializeUserFunction(this.windowConfig);
        log.info("Window Config: {}", this.windowConfig);
        this.windowManager = this.getWindowManager(this.windowConfig, context);
        this.initialized = true;
        this.start();
    }

    private void initializeUserFunction(WindowConfig windowConfig) {
        String actualWindowFunctionClassName = windowConfig.getActualWindowFunctionClassName();
        ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
        Object userClassObject = Reflections.createInstance(
                actualWindowFunctionClassName,
                clsLoader);
        if (userClassObject instanceof java.util.function.Function) {
            Class<?>[] typeArgs = TypeResolver.resolveRawArguments(
                    java.util.function.Function.class, userClassObject.getClass());
            if (typeArgs[0].equals(Collection.class)) {
                bareWindowFunction = (java.util.function.Function) userClassObject;
            } else {
                throw new IllegalArgumentException("Window function must take a collection as input");
            }
        } else if (userClassObject instanceof WindowFunction) {
            windowFunction = (WindowFunction) userClassObject;
        } else {
            throw new IllegalArgumentException("Window function does not implement the correct interface");
        }
    }

    private WindowConfig getWindowConfigs(Context context) {

        if (!context.getUserConfigValue(WindowConfig.WINDOW_CONFIG_KEY).isPresent()) {
            throw new IllegalArgumentException("Window Configs cannot be found");
        }
        WindowConfig windowConfig = new Gson().fromJson(
                (new Gson().toJson(context.getUserConfigValue(WindowConfig.WINDOW_CONFIG_KEY).get())),
                WindowConfig.class);

        return windowConfig;
    }

    private WindowManager<Record<T>> getWindowManager(WindowConfig windowConfig, Context context) {

        WindowLifecycleListener<Event<Record<T>>> lifecycleListener = newWindowLifecycleListener(context);
        WindowManager<Record<T>> manager = new WindowManager<>(lifecycleListener, new ConcurrentLinkedQueue<>());

        if (this.windowConfig.getTimestampExtractorClassName() != null) {
            this.timestampExtractor = getTimeStampExtractor(windowConfig);

            waterMarkEventGenerator = new WaterMarkEventGenerator<>(manager, this.windowConfig
                    .getWatermarkEmitIntervalMs(),
                    this.windowConfig.getMaxLagMs(), new HashSet<>(context.getInputTopics()), context);
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
        } catch (ClassNotFoundException | NoClassDefFoundError cnfe) {
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
                                                         EvictionPolicy<Record<T>, ?> evictionPolicy, Context context) {
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
                    evictionPolicy, context);
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

    protected WindowLifecycleListener<Event<Record<T>>> newWindowLifecycleListener(Context context) {
        return new WindowLifecycleListener<Event<Record<T>>>() {
            @Override
            public void onExpiry(List<Event<Record<T>>> events) {
            }

            @Override
            public void onActivation(List<Event<Record<T>>> tuples, List<Event<Record<T>>> newTuples,
                                     List<Event<Record<T>>>
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

    private void processWindow(Context context, List<Record<T>> tuples, List<Record<T>> newTuples, List<Record<T>>
            expiredTuples, Long referenceTime) {

        X output = null;
        try {
            output = this.process(
                    new WindowImpl<>(tuples, newTuples, expiredTuples, getWindowStartTs(referenceTime), referenceTime),
                    new WindowContextImpl(context));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (output != null) {
            context.publish(context.getOutputTopic(), output, context.getOutputSchemaType()).thenAccept(__ -> {
                if (windowConfig.getProcessingGuarantees() == WindowConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                    for (Record<T> record : tuples) {
                        record.ack();
                    }
                }
            });
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

    public void shutdown() {
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
    public X process(T input, Context context) throws Exception {
        if (!this.initialized) {
            initialize(context);
        }

        // record must be PulsarFunctionRecord.
        Record<T> record = (Record<T>) context.getCurrentRecord();

        // windows function processing semantics requires separate processing
        if (windowConfig.getProcessingGuarantees() == WindowConfig.ProcessingGuarantees.ATMOST_ONCE) {
            record.ack();
        }

        if (isEventTime()) {
            long ts = this.timestampExtractor.extractTimestamp(record.getValue());
            if (this.waterMarkEventGenerator.track(record.getTopicName().get(), ts)) {
                this.windowManager.add(record, ts, record);
            } else {
                if (this.windowConfig.getLateDataTopic() != null) {
                    context.newOutputMessage(this.windowConfig.getLateDataTopic(), null).value(input).sendAsync();
                } else {
                    log.info(String.format(
                            "Received a late tuple %s with ts %d. This will not be " + "processed"
                                    + ".", input, ts));
                }
            }
        } else {
            this.windowManager.add(record, System.currentTimeMillis(), record);
        }
        return null;
    }

    public X process(Window<Record<T>> inputWindow, WindowContext context) throws Exception {
        if (this.bareWindowFunction != null) {
            Collection<T> newCollection = inputWindow.get().stream().map(Record::getValue).collect(Collectors.toList());
            return this.bareWindowFunction.apply(newCollection);
        } else {
            return this.windowFunction.process(inputWindow.get(), context);
        }
    }
}

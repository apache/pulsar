package org.apache.pulsar.functions.api.streamlet.windowing;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.api.streamlet.windowing.evictors.CountEvictionPolicy;
import org.apache.pulsar.functions.api.streamlet.windowing.evictors.TimeEvictionPolicy;
import org.apache.pulsar.functions.api.streamlet.windowing.triggers.CountTriggerPolicy;
import org.apache.pulsar.functions.api.streamlet.windowing.triggers.TimeTriggerPolicy;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Slf4j
public abstract class WindowedPulsarFunction<I, O> implements PulsarFunction<I, O> {

    private boolean initialized;
    private WindowConfig windowConfig;
    private WindowManager<I> windowManager;

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
            windowConfig.setWindowLengthDurationMs(Long.parseLong(context.getUserConfigValue("windowLengthDurationMs")));
        }
        if (context.getUserConfigValue("slidingIntervalCount") != null) {
            windowConfig.setSlidingIntervalCount(Integer.parseInt(context.getUserConfigValue("slidingIntervalCount")));
        }
        if (context.getUserConfigValue("slidingIntervalDurationMs") != null) {
            windowConfig.setSlidingDurationMs(Long.parseLong(context.getUserConfigValue("slidingIntervalDurationMs")));
        }

        validateAndSetDefaultsWindowConfig(windowConfig);
        return windowConfig;
    }

    private static void validateAndSetDefaultsWindowConfig(WindowConfig windowConfig) {
        if (windowConfig.getWindowLengthDurationMs() == null && windowConfig.getWindowLengthCount() == null) {
            throw new IllegalArgumentException("Window length is not specified");
        }

        if (windowConfig.getWindowLengthDurationMs() != null && windowConfig.getWindowLengthCount() != null) {
            throw new IllegalArgumentException("Window length for time and count are set! Please set one or the other.");
        }

        if (windowConfig.getWindowLengthDurationMs() != null && windowConfig.getSlidingDurationMs() == null) {
            windowConfig.setSlidingDurationMs(windowConfig.getWindowLengthDurationMs());
        }

        if (windowConfig.getWindowLengthCount() != null && windowConfig.getSlidingIntervalCount() == null) {
            windowConfig.setSlidingIntervalCount(windowConfig.getWindowLengthCount());
        }
    }

    private WindowManager<I> getWindowManager(WindowConfig windowConfig, Context context) {

        WindowLifecycleListener<Event<I>> lifecycleListener = newWindowLifecycleListener(context);
        WindowManager<I> manager = new WindowManager<>(lifecycleListener, new ConcurrentLinkedQueue<>());

        EvictionPolicy<I, ?> evictionPolicy = getEvictionPolicy(windowConfig);
        TriggerPolicy<I, ?> triggerPolicy = getTriggerPolicy(windowConfig, manager,
                evictionPolicy);
        manager.setEvictionPolicy(evictionPolicy);
        manager.setTriggerPolicy(triggerPolicy);

        return manager;
    }

    @SuppressWarnings("HiddenField")
    private TriggerPolicy<I, ?> getTriggerPolicy(WindowConfig windowConfig, WindowManager<I> manager, EvictionPolicy<I, ?>
                                                             evictionPolicy) {
        if (windowConfig.getSlidingIntervalCount() != null) {
                return new CountTriggerPolicy<>(windowConfig.getSlidingIntervalCount(), manager, evictionPolicy);
        } else {
            return new TimeTriggerPolicy<>(windowConfig.getSlidingDurationMs(), manager,
                    evictionPolicy);
        }
    }

    @SuppressWarnings("HiddenField")
    private EvictionPolicy<I, ?> getEvictionPolicy(WindowConfig windowConfig) {
        if (windowConfig.getWindowLengthCount() != null) {
            return new CountEvictionPolicy<>(windowConfig.getWindowLengthCount());
        } else {
            return new TimeEvictionPolicy<>(windowConfig.getWindowLengthDurationMs());
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
        log.debug("Starting trigger policy");
        this.windowManager.triggerPolicy.start();
    }


    @Override
    public O process(I input, Context context) throws Exception {
        if (!this.initialized) {
            initialize(context);
        }

        this.windowManager.add(input, System.currentTimeMillis(), context.getMessageId(), context.getTopicName());
        return null;
    }

    public abstract O handleRequest(Window<I> inputWindow, WindowContext context) throws Exception;
}

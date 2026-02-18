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
package org.apache.pulsar.broker.stats;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.Optional;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.AdaptivePublishRateLimiter;
import org.apache.pulsar.broker.service.AdaptivePublishThrottleController;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * OpenTelemetry metrics for the adaptive publish throttle feature.
 *
 * <p>Broker-level metrics (3 instruments, no topic label — zero cardinality concern):
 * <ul>
 *   <li>{@value MEMORY_PRESSURE_GAUGE} — current JVM heap pressure factor (0.0–1.0)</li>
 *   <li>{@value ACTIVE_TOPICS_GAUGE} — topics currently being adaptively throttled</li>
 *   <li>{@value TOTAL_ACTIVATIONS_COUNTER} — cumulative activation count since start</li>
 * </ul>
 *
 * <p>Per-topic metrics (6 instruments, topic label — only registered when
 * {@code adaptivePublisherThrottlingPerTopicMetricsEnabled=true}):
 * <ul>
 *   <li>{@value TOPIC_THROTTLE_ACTIVE}</li>
 *   <li>{@value TOPIC_NATURAL_MSG_RATE}</li>
 *   <li>{@value TOPIC_EFFECTIVE_MSG_RATE}</li>
 *   <li>{@value TOPIC_MEMORY_PRESSURE}</li>
 *   <li>{@value TOPIC_BACKLOG_PRESSURE}</li>
 *   <li>{@value TOPIC_RATE_REDUCTION_RATIO}</li>
 * </ul>
 */
public class OpenTelemetryAdaptiveThrottleStats implements AutoCloseable {

    // Broker-level metric names
    public static final String MEMORY_PRESSURE_GAUGE =
            "pulsar.broker.adaptive.throttle.memory.pressure";
    public static final String ACTIVE_TOPICS_GAUGE =
            "pulsar.broker.adaptive.throttle.active.topic.count";
    public static final String TOTAL_ACTIVATIONS_COUNTER =
            "pulsar.broker.adaptive.throttle.activation.count";

    // Controller health metric names
    /** Unix epoch ms of the last completed evaluation cycle; alert when it stops advancing. */
    public static final String CONTROLLER_LAST_EVAL_EPOCH_GAUGE =
            "pulsar.broker.adaptive.throttle.controller.last.evaluation.timestamp";
    /** Wall-clock duration of the last evaluation cycle in ms; alert when it exceeds the interval. */
    public static final String CONTROLLER_EVAL_DURATION_GAUGE =
            "pulsar.broker.adaptive.throttle.controller.evaluation.duration";
    /** Cumulative count of evaluation cycles that threw an uncaught exception. */
    public static final String CONTROLLER_EVAL_FAILURE_COUNTER =
            "pulsar.broker.adaptive.throttle.controller.evaluation.failure.count";

    // Per-topic metric names
    public static final String TOPIC_THROTTLE_ACTIVE =
            "pulsar.broker.topic.adaptive.throttle.active";
    public static final String TOPIC_NATURAL_MSG_RATE =
            "pulsar.broker.topic.adaptive.throttle.natural.publish.rate";
    public static final String TOPIC_EFFECTIVE_MSG_RATE =
            "pulsar.broker.topic.adaptive.throttle.effective.publish.rate";
    public static final String TOPIC_MEMORY_PRESSURE =
            "pulsar.broker.topic.adaptive.throttle.memory.pressure";
    public static final String TOPIC_BACKLOG_PRESSURE =
            "pulsar.broker.topic.adaptive.throttle.backlog.pressure";
    public static final String TOPIC_RATE_REDUCTION_RATIO =
            "pulsar.broker.topic.adaptive.throttle.rate.reduction.ratio";

    private final PulsarService pulsar;
    private final AdaptivePublishThrottleController controller;
    private final BatchCallback brokerCallback;
    private final BatchCallback perTopicCallback;

    private final ObservableDoubleMeasurement memoryPressureGauge;
    private final ObservableLongMeasurement activeTopicsGauge;
    private final ObservableLongMeasurement totalActivationsCounter;
    private final ObservableLongMeasurement controllerLastEvalEpochGauge;
    private final ObservableLongMeasurement controllerEvalDurationGauge;
    private final ObservableLongMeasurement controllerEvalFailureCounter;

    // Per-topic instruments (may be null when perTopicMetrics disabled)
    private final ObservableLongMeasurement topicThrottleActive;
    private final ObservableDoubleMeasurement topicNaturalMsgRate;
    private final ObservableDoubleMeasurement topicEffectiveMsgRate;
    private final ObservableDoubleMeasurement topicMemoryPressure;
    private final ObservableDoubleMeasurement topicBacklogPressure;
    private final ObservableDoubleMeasurement topicRateReductionRatio;

    public OpenTelemetryAdaptiveThrottleStats(PulsarService pulsar,
                                              AdaptivePublishThrottleController controller,
                                              boolean perTopicMetricsEnabled) {
        this.pulsar = pulsar;
        this.controller = controller;
        Meter meter = pulsar.getOpenTelemetry().getMeter();

        // --- broker-level instruments ---
        memoryPressureGauge = meter
                .gaugeBuilder(MEMORY_PRESSURE_GAUGE)
                .setDescription("Current JVM heap pressure factor driving adaptive publish throttling "
                        + "(0.0 = no pressure, 1.0 = maximum pressure).")
                .setUnit("{ratio}")
                .buildObserver();

        activeTopicsGauge = meter
                .upDownCounterBuilder(ACTIVE_TOPICS_GAUGE)
                .setDescription("Number of topics on this broker currently being adaptively throttled.")
                .setUnit("{topic}")
                .buildObserver();

        totalActivationsCounter = meter
                .counterBuilder(TOTAL_ACTIVATIONS_COUNTER)
                .setDescription("Total number of adaptive throttle activations since broker start.")
                .setUnit("{event}")
                .buildObserver();

        controllerLastEvalEpochGauge = meter
                .upDownCounterBuilder(CONTROLLER_LAST_EVAL_EPOCH_GAUGE)
                .setDescription("Unix epoch milliseconds when the last adaptive throttle controller "
                        + "evaluation cycle completed. Zero if no cycle has run yet. "
                        + "Alert when this stops advancing (stalled controller).")
                .setUnit("ms")
                .buildObserver();

        controllerEvalDurationGauge = meter
                .upDownCounterBuilder(CONTROLLER_EVAL_DURATION_GAUGE)
                .setDescription("Wall-clock duration in milliseconds of the most recent adaptive "
                        + "throttle controller evaluation cycle. Alert when this consistently "
                        + "exceeds adaptivePublisherThrottlingIntervalMs.")
                .setUnit("ms")
                .buildObserver();

        controllerEvalFailureCounter = meter
                .counterBuilder(CONTROLLER_EVAL_FAILURE_COUNTER)
                .setDescription("Cumulative number of adaptive throttle controller evaluation cycles "
                        + "that threw an uncaught exception. Any non-zero value means the controller "
                        + "is degraded; check broker logs for '[AdaptiveThrottleController] "
                        + "Evaluation cycle failed'.")
                .setUnit("{error}")
                .buildObserver();

        brokerCallback = meter.batchCallback(
                this::recordBrokerMetrics,
                memoryPressureGauge, activeTopicsGauge, totalActivationsCounter,
                controllerLastEvalEpochGauge, controllerEvalDurationGauge,
                controllerEvalFailureCounter);

        // --- per-topic instruments ---
        if (perTopicMetricsEnabled) {
            topicThrottleActive = meter
                    .upDownCounterBuilder(TOPIC_THROTTLE_ACTIVE)
                    .setDescription("1 if this topic is currently adaptively throttled, 0 otherwise.")
                    .setUnit("{bool}")
                    .buildObserver();
            topicNaturalMsgRate = meter
                    .gaugeBuilder(TOPIC_NATURAL_MSG_RATE)
                    .setDescription("Estimated natural (unthrottled) publish rate for this topic.")
                    .setUnit("{message}/s")
                    .buildObserver();
            topicEffectiveMsgRate = meter
                    .gaugeBuilder(TOPIC_EFFECTIVE_MSG_RATE)
                    .setDescription("Current effective (throttled) publish rate for this topic.")
                    .setUnit("{message}/s")
                    .buildObserver();
            topicMemoryPressure = meter
                    .gaugeBuilder(TOPIC_MEMORY_PRESSURE)
                    .setDescription("Memory pressure factor applied to this topic in the last cycle.")
                    .setUnit("{ratio}")
                    .buildObserver();
            topicBacklogPressure = meter
                    .gaugeBuilder(TOPIC_BACKLOG_PRESSURE)
                    .setDescription("Backlog pressure factor applied to this topic in the last cycle.")
                    .setUnit("{ratio}")
                    .buildObserver();
            topicRateReductionRatio = meter
                    .gaugeBuilder(TOPIC_RATE_REDUCTION_RATIO)
                    .setDescription("Ratio of effective rate to natural rate (1.0 = no throttle, "
                            + "0.1 = throttled to minimum).")
                    .setUnit("{ratio}")
                    .buildObserver();

            perTopicCallback = meter.batchCallback(
                    this::recordPerTopicMetrics,
                    topicThrottleActive, topicNaturalMsgRate, topicEffectiveMsgRate,
                    topicMemoryPressure, topicBacklogPressure, topicRateReductionRatio);
        } else {
            topicThrottleActive = null;
            topicNaturalMsgRate = null;
            topicEffectiveMsgRate = null;
            topicMemoryPressure = null;
            topicBacklogPressure = null;
            topicRateReductionRatio = null;
            perTopicCallback = null;
        }
    }

    private void recordBrokerMetrics() {
        Attributes attrs = Attributes.empty();
        memoryPressureGauge.record(controller.getCurrentMemoryPressureFactor(), attrs);
        activeTopicsGauge.record(controller.getActiveThrottledTopicsCount(), attrs);
        totalActivationsCounter.record(controller.getTotalActivationCount(), attrs);
        controllerLastEvalEpochGauge.record(controller.getLastEvaluationCompletedEpochMs(), attrs);
        controllerEvalDurationGauge.record(controller.getLastEvaluationDurationMs(), attrs);
        controllerEvalFailureCounter.record(controller.getEvaluationFailureCount(), attrs);
    }

    private void recordPerTopicMetrics() {
        pulsar.getBrokerService().getTopics().values().stream()
                .map(f -> f.getNow(Optional.empty()))
                .forEach(opt -> opt.filter(t -> t instanceof PersistentTopic)
                        .ifPresent(t -> {
                            PersistentTopic topic = (PersistentTopic) t;
                            AdaptivePublishRateLimiter limiter = topic.getAdaptivePublishRateLimiter();
                            if (limiter == null) {
                                return;
                            }
                            Attributes attrs = topic.getTopicAttributes().getCommonAttributes();
                            boolean active = limiter.isActive();
                            double natural = limiter.getNaturalMsgRateEstimate();
                            double effective = active ? limiter.getCurrentEffectiveMsgRate() : natural;
                            double ratio = natural > 0 ? effective / natural : 1.0;

                            topicThrottleActive.record(active ? 1L : 0L, attrs);
                            topicNaturalMsgRate.record(natural, attrs);
                            topicEffectiveMsgRate.record(effective, attrs);
                            // Memory and backlog pressure are broker-level / cycle-level values;
                            // the best approximation here is from the controller's last cycle.
                            topicMemoryPressure.record(controller.getCurrentMemoryPressureFactor(), attrs);
                            // Backlog pressure is not stored per-topic in the limiter;
                            // we emit the combined rate reduction as the primary signal.
                            topicBacklogPressure.record(active ? 1.0 - ratio : 0.0, attrs);
                            topicRateReductionRatio.record(ratio, attrs);
                        }));
    }

    @Override
    public void close() {
        brokerCallback.close();
        if (perTopicCallback != null) {
            perTopicCallback.close();
        }
    }
}

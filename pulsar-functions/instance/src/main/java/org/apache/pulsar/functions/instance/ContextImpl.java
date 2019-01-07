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
package org.apache.pulsar.functions.instance;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Summary;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.state.StateContextImpl;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.instance.stats.FunctionStatsManager;
import org.apache.pulsar.functions.instance.stats.SinkStatsManager;
import org.apache.pulsar.functions.instance.stats.SourceStatsManager;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.pulsar.functions.instance.stats.FunctionStatsManager.USER_METRIC_PREFIX;

/**
 * This class implements the Context interface exposed to the user.
 */

class ContextImpl implements Context, SinkContext, SourceContext {
    private InstanceConfig config;
    private Logger logger;

    // Per Message related
    private Record<?> record;

    private Map<String, Producer<?>> publishProducers;
    private ProducerBuilderImpl<?> producerBuilder;

    private final List<String> inputTopics;

    private final TopicSchema topicSchema;

    private final SecretsProvider secretsProvider;
    private final Map<String, Object> secretsMap;

    @Getter
    @Setter
    private StateContextImpl stateContext;
    private Map<String, Object> userConfigs;

    Map<String, String[]> userMetricsLabels = new HashMap<>();
    private final String[] metricsLabels;
    private final Summary userMetricsSummary;

    private final static String[] userMetricsLabelNames;
    static {
        // add label to indicate user metric
        userMetricsLabelNames = Arrays.copyOf(ComponentStatsManager.metricsLabelNames, ComponentStatsManager.metricsLabelNames.length + 1);
        userMetricsLabelNames[ComponentStatsManager.metricsLabelNames.length] = "metric";
    }
    private final Utils.ComponentType componentType;

    public ContextImpl(InstanceConfig config, Logger logger, PulsarClient client, List<String> inputTopics,
                       SecretsProvider secretsProvider, CollectorRegistry collectorRegistry, String[] metricsLabels,
                       Utils.ComponentType componentType) {
        this.config = config;
        this.logger = logger;
        this.publishProducers = new HashMap<>();
        this.inputTopics = inputTopics;
        this.topicSchema = new TopicSchema(client);

        this.producerBuilder = (ProducerBuilderImpl<?>) client.newProducer().blockIfQueueFull(true).enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);

        if (config.getFunctionDetails().getUserConfig().isEmpty()) {
            userConfigs = new HashMap<>();
        } else {
            userConfigs = new Gson().fromJson(config.getFunctionDetails().getUserConfig(),
                    new TypeToken<Map<String, Object>>() {
                    }.getType());
        }
        this.secretsProvider = secretsProvider;
        if (!StringUtils.isEmpty(config.getFunctionDetails().getSecretsMap())) {
            secretsMap = new Gson().fromJson(config.getFunctionDetails().getSecretsMap(),
                    new TypeToken<Map<String, Object>>() {
                    }.getType());
        } else {
            secretsMap = new HashMap<>();
        }

        this.metricsLabels = metricsLabels;
        String prefix;
        switch (componentType) {
            case FUNCTION:
                prefix = FunctionStatsManager.PULSAR_FUNCTION_METRICS_PREFIX;
                break;
            case SINK:
                prefix = SinkStatsManager.PULSAR_SINK_METRICS_PREFIX;
                break;
            case SOURCE:
                prefix = SourceStatsManager.PULSAR_SOURCE_METRICS_PREFIX;
                break;
            default:
                throw new RuntimeException("Unknown component type: " + componentType);
        }
        this.userMetricsSummary = Summary.build()
                .name(prefix + ComponentStatsManager.USER_METRIC_PREFIX)
                .help("User defined metric.")
                .labelNames(userMetricsLabelNames)
                .quantile(0.5, 0.01)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.01)
                .quantile(0.999, 0.01)
                .register(collectorRegistry);
        this.componentType = componentType;
    }

    public void setCurrentMessageContext(Record<?> record) {
        this.record = record;
    }

    @Override
    public Record<?> getCurrentRecord() {
        return record;
    }

    @Override
    public Collection<String> getInputTopics() {
        return inputTopics;
    }

    @Override
    public String getOutputTopic() {
        return config.getFunctionDetails().getSink().getTopic();
    }

    @Override
    public String getOutputSchemaType() {
        SinkSpec sink = config.getFunctionDetails().getSink();
        if (!StringUtils.isEmpty(sink.getSchemaType())) {
            return sink.getSchemaType();
        } else {
            return sink.getSerDeClassName();
        }
    }

    @Override
    public String getTenant() {
        return config.getFunctionDetails().getTenant();
    }

    @Override
    public String getNamespace() {
        return config.getFunctionDetails().getNamespace();
    }

    @Override
    public String getFunctionName() {
        return config.getFunctionDetails().getName();
    }

    @Override
    public String getFunctionId() {
        return config.getFunctionId().toString();
    }

    @Override
    public int getInstanceId() {
        return config.getInstanceId();
    }

    @Override
    public int getNumInstances() {
        return config.getFunctionDetails().getParallelism();
    }

    @Override
    public String getFunctionVersion() {
        return config.getFunctionVersion();
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public Optional<Object> getUserConfigValue(String key) {

        return Optional.ofNullable(userConfigs.getOrDefault(key, null));
    }

    @Override
    public Object getUserConfigValueOrDefault(String key, Object defaultValue) {
        return getUserConfigValue(key).orElse(defaultValue);
    }

    @Override
    public Map<String, Object> getUserConfigMap() {
        return userConfigs;
    }

    @Override
    public String getSecret(String secretName) {
        if (secretsMap.containsKey(secretName)) {
            return secretsProvider.provideSecret(secretName, secretsMap.get(secretName));
        } else {
            return null;
        }
    }

    private void ensureStateEnabled() {
        checkState(null != stateContext, "State is not enabled.");
    }

    @Override
    public void incrCounter(String key, long amount) {
        ensureStateEnabled();
        try {
            stateContext.incr(key, amount);
        } catch (Exception e) {
            throw new RuntimeException("Failed to increment key '" + key + "' by amount '" + amount + "'", e);
        }
    }

    @Override
    public long getCounter(String key) {
        ensureStateEnabled();
        try {
            return stateContext.getAmount(key);
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve counter from key '" + key + "'");
        }
    }

    @Override
    public void putState(String key, ByteBuffer value) {
        ensureStateEnabled();
        try {
            stateContext.put(key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to update the state value for key '" + key + "'");
        }
    }

    @Override
    public ByteBuffer getState(String key) {
        ensureStateEnabled();
        try {
            return stateContext.getValue(key);
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve the state value for key '" + key + "'");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <O> CompletableFuture<Void> publish(String topicName, O object) {
        return publish(topicName, object, "");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <O> CompletableFuture<Void> publish(String topicName, O object, String schemaOrSerdeClassName) {
        return publish(topicName, object, (Schema<O>) topicSchema.getSchema(topicName, object, schemaOrSerdeClassName, false));
    }

    @SuppressWarnings("unchecked")
    public <O> CompletableFuture<Void> publish(String topicName, O object, Schema<O> schema) {
        Producer<O> producer = (Producer<O>) publishProducers.get(topicName);

        if (producer == null) {
            try {
                Producer<O> newProducer = ((ProducerBuilderImpl<O>) producerBuilder.clone())
                        .schema(schema)
                        .topic(topicName)
                        .properties(InstanceUtils.getProperties(componentType,
                                FunctionDetailsUtils.getFullyQualifiedName(
                                        this.config.getFunctionDetails().getTenant(),
                                        this.config.getFunctionDetails().getNamespace(),
                                        this.config.getFunctionDetails().getName()),
                                this.config.getInstanceId()))
                        .create();

                Producer<O> existingProducer = (Producer<O>) publishProducers.putIfAbsent(topicName, newProducer);

                if (existingProducer != null) {
                    // The value in the map was not updated after the concurrent put
                    newProducer.close();
                    producer = existingProducer;
                } else {
                    producer = newProducer;
                }

            } catch (PulsarClientException e) {
                logger.error("Failed to create Producer while doing user publish", e);
                return FutureUtil.failedFuture(e);
            }
        }

        return producer.sendAsync(object).thenApply(msgId -> null);
    }

    @Override
    public void recordMetric(String metricName, double value) {
        String[] userMetricLabels = userMetricsLabels.get(metricName);
        if (userMetricLabels == null) {
            userMetricLabels = Arrays.copyOf(metricsLabels, metricsLabels.length + 1);
            userMetricLabels[userMetricLabels.length - 1] = metricName;
            // set label for metrics before putting into userMetricsLabels map to
            // prevent race condition with getMetrics calls
            userMetricsSummary.labels(userMetricLabels).observe(value);
            userMetricsLabels.put(metricName, userMetricLabels);
        } else {
            userMetricsSummary.labels(userMetricLabels).observe(value);
        }
    }

    public Map<String, Double> getAndResetMetrics() {
        Map<String, Double> retval = getMetrics();
        resetMetrics();
        return retval;
    }

    public void resetMetrics() {
        userMetricsSummary.clear();
    }

    public Map<String, Double> getMetrics() {
        Map<String, Double> metricsMap = new HashMap<>();
        for (Map.Entry<String, String[]> userMetricsLabelsEntry : userMetricsLabels.entrySet()) {
            String metricName = userMetricsLabelsEntry.getKey();
            String[] labels = userMetricsLabelsEntry.getValue();
            Summary.Child.Value summary = userMetricsSummary.labels(labels).get();
            metricsMap.put(String.format("%s%s_sum", USER_METRIC_PREFIX, metricName), summary.sum);
            metricsMap.put(String.format("%s%s_count", USER_METRIC_PREFIX, metricName), summary.count);
            for (Map.Entry<Double, Double> entry : summary.quantiles.entrySet()) {
                Double quantile = entry.getKey();
                Double value = entry.getValue();
                metricsMap.put(String.format("%s%s_%s", USER_METRIC_PREFIX, metricName, quantile), value);
            }
        }
        return metricsMap;
    }
}
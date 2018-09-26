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

import static com.google.common.base.Preconditions.checkState;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

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
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;

/**
 * This class implements the Context interface exposed to the user.
 */
class ContextImpl implements Context, SinkContext, SourceContext {
    private InstanceConfig config;
    private Logger logger;

    // Per Message related
    private Record<?> record;

    @Getter
    @Setter
    private class AccumulatedMetricDatum {
        private double count;
        private double sum;
        private double max;
        private double min;

        AccumulatedMetricDatum() {
            count = 0;
            sum = 0;
            max = Double.MIN_VALUE;
            min = Double.MAX_VALUE;
        }

        public void update(double value) {
            count++;
            sum += value;
            if (max < value) {
                max = value;
            }
            if (min > value) {
                min = value;
            }
        }
    }

    private ConcurrentMap<String, AccumulatedMetricDatum> currentAccumulatedMetrics;
    private ConcurrentMap<String, AccumulatedMetricDatum> accumulatedMetrics;

    private Map<String, Producer<?>> publishProducers;
    private ProducerBuilderImpl<?> producerBuilder;

    private final List<String> inputTopics;

    private final TopicSchema topicSchema;

    @Getter
    @Setter
    private StateContextImpl stateContext;
    private Map<String, Object> userConfigs;

    public ContextImpl(InstanceConfig config, Logger logger, PulsarClient client, List<String> inputTopics) {
        this.config = config;
        this.logger = logger;
        this.currentAccumulatedMetrics = new ConcurrentHashMap<>();
        this.accumulatedMetrics = new ConcurrentHashMap<>();
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
                        .schema(schema).topic(topicName).create();

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
        currentAccumulatedMetrics.putIfAbsent(metricName, new AccumulatedMetricDatum());
        currentAccumulatedMetrics.get(metricName).update(value);
    }

    public MetricsData getAndResetMetrics() {
        MetricsData retval = getMetrics();
        resetMetrics();
        return retval;
    }

    public void resetMetrics() {
        this.accumulatedMetrics.clear();
        this.accumulatedMetrics.putAll(currentAccumulatedMetrics);
        this.currentAccumulatedMetrics.clear();
    }

    public MetricsData getMetrics() {
        MetricsData.Builder metricsDataBuilder = MetricsData.newBuilder();
        for (String metricName : accumulatedMetrics.keySet()) {
            MetricsData.DataDigest.Builder bldr = MetricsData.DataDigest.newBuilder();
            bldr.setSum(accumulatedMetrics.get(metricName).getSum());
            bldr.setCount(accumulatedMetrics.get(metricName).getCount());
            bldr.setMax(accumulatedMetrics.get(metricName).getMax());
            bldr.setMin(accumulatedMetrics.get(metricName).getMax());
            metricsDataBuilder.putMetrics(metricName, bldr.build());
        }
        MetricsData retval = metricsDataBuilder.build();
        return retval;
    }
}
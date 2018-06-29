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
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.instance.state.StateContextImpl;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData;
import org.apache.pulsar.functions.utils.Reflections;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

/**
 * This class implements the Context interface exposed to the user.
 */
class ContextImpl implements Context {
    private InstanceConfig config;
    private Logger logger;

    // Per Message related
    private MessageId messageId;
    private String currentTopicName;

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

    private ConcurrentMap<String, AccumulatedMetricDatum> accumulatedMetrics;

    private Map<String, Producer> publishProducers;
    private Map<String, SerDe> publishSerializers;
    private ProducerConfiguration producerConfiguration;
    private PulsarClient pulsarClient;
    private ClassLoader classLoader;
    Consumer inputConsumer;
    @Getter
    @Setter
    private StateContextImpl stateContext;
    private Map<String, Object> userConfigs;

    public ContextImpl(InstanceConfig config, Logger logger, PulsarClient client,
                       ClassLoader classLoader, Consumer inputConsumer) {
        this.config = config;
        this.logger = logger;
        this.pulsarClient = client;
        this.classLoader = classLoader;
        this.accumulatedMetrics = new ConcurrentHashMap<>();
        this.publishProducers = new HashMap<>();
        this.publishSerializers = new HashMap<>();
        this.inputConsumer = inputConsumer;
        producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setBlockIfQueueFull(true);
        producerConfiguration.setBatchingEnabled(true);
        producerConfiguration.setBatchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
        producerConfiguration.setMaxPendingMessages(1000000);
        if (config.getFunctionDetails().getUserConfig().isEmpty()) {
            userConfigs = new HashMap<>();
        } else {
            userConfigs = new Gson().fromJson(config.getFunctionDetails().getUserConfig(),
                    new TypeToken<Map<String, Object>>(){}.getType());
        }
    }

    public void setCurrentMessageContext(MessageId messageId, String topicName) {
        this.messageId = messageId;
        this.currentTopicName = topicName;
    }

    @Override
    public byte[] getMessageId() {
        return messageId.toByteArray();
    }

    @Override
    public String getCurrentMessageTopicName() {
        return currentTopicName;
    }

    @Override
    public Collection<String> getInputTopics() {
        if (inputConsumer == null) {
            return new LinkedList<>();
        }
        if (inputConsumer instanceof MultiTopicsConsumerImpl) {
            return ((MultiTopicsConsumerImpl) inputConsumer).getTopics();
        } else {
            return Arrays.asList(inputConsumer.getTopic());
        }
    }

    @Override
    public String getOutputTopic() {
        return config.getFunctionDetails().getSink().getTopic();
    }

    @Override
    public String getOutputSerdeClassName() {
        return config.getFunctionDetails().getSink().getSerDeClassName();
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
    public String getInstanceId() {
        return config.getInstanceId().toString();
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

    @Override
    public <O> CompletableFuture<Void> publish(String topicName, O object) {
        return publish(topicName, object, DefaultSerDe.class.getName());
    }

    @Override
    public <O> CompletableFuture<Void> publish(String topicName, O object, String serDeClassName) {
        if (!publishProducers.containsKey(topicName)) {
            try {
                publishProducers.put(topicName, pulsarClient.createProducer(topicName, producerConfiguration));
            } catch (PulsarClientException ex) {
                CompletableFuture<Void> retval = new CompletableFuture<>();
                retval.completeExceptionally(ex);
                return retval;
            }
        }
        if (StringUtils.isEmpty(serDeClassName)) {
            serDeClassName = DefaultSerDe.class.getName();
        }
        if (!publishSerializers.containsKey(serDeClassName)) {
            SerDe serDe;
            if (serDeClassName.equals(DefaultSerDe.class.getName())) {
                if (!DefaultSerDe.IsSupportedType(object.getClass())) {
                    throw new RuntimeException("Default Serializer does not support " + object.getClass());
                }
                serDe = new DefaultSerDe(object.getClass());
            } else {
                try {
                    Class<? extends SerDe> serDeClass = (Class<? extends SerDe>) Class.forName(serDeClassName);
                    serDe = Reflections.createInstance(
                            serDeClassName,
                            serDeClass,
                            classLoader);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            publishSerializers.put(serDeClassName, serDe);
        }

        byte[] bytes = publishSerializers.get(serDeClassName).serialize(object);
        return publishProducers.get(topicName).sendAsync(bytes)
                .thenApply(msgId -> null);
    }

    //TODO remove topic argument
    @Override
    public CompletableFuture<Void> ack(byte[] messageId) {
        // if inputConsumer is null, then ack is a no-op
        if (inputConsumer == null) {
            return CompletableFuture.completedFuture(null);
        }
        MessageId actualMessageId = null;
        try {
            actualMessageId = MessageId.fromByteArray(messageId);

        } catch (IOException e) {
            throw new RuntimeException("Invalid message id to ack", e);
        }
        return  inputConsumer.acknowledgeAsync(actualMessageId);
    }

    @Override
    public void recordMetric(String metricName, double value) {
        accumulatedMetrics.putIfAbsent(metricName, new AccumulatedMetricDatum());
        accumulatedMetrics.get(metricName).update(value);
    }

    public MetricsData getAndResetMetrics() {
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
        accumulatedMetrics.clear();
        return retval;
    }
}
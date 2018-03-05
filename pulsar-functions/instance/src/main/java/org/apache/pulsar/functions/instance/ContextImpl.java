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

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData;
import org.apache.pulsar.functions.instance.state.StateContextImpl;
import org.apache.pulsar.functions.utils.Reflections;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

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
    private Map<String, Consumer> sourceConsumers;
    @Getter
    @Setter
    private StateContextImpl stateContext;

    public ContextImpl(InstanceConfig config, Logger logger, PulsarClient client,
                       ClassLoader classLoader, Map<String, Consumer> sourceConsumers) {
        this.config = config;
        this.logger = logger;
        this.pulsarClient = client;
        this.classLoader = classLoader;
        this.accumulatedMetrics = new ConcurrentHashMap<>();
        this.publishProducers = new HashMap<>();
        this.publishSerializers = new HashMap<>();
        this.sourceConsumers = sourceConsumers;
        producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setBlockIfQueueFull(true);
        producerConfiguration.setBatchingEnabled(true);
        producerConfiguration.setBatchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
        producerConfiguration.setMaxPendingMessages(1000000);
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
    public String getTopicName() {
        return currentTopicName;
    }

    @Override
    public Collection<String> getSourceTopics() {
        return sourceConsumers.keySet();
    }

    @Override
    public String getSinkTopic() {
        return config.getFunctionConfig().getOutput();
    }

    @Override
    public String getOutputSerdeClassName() {
        return config.getFunctionConfig().getOutputSerdeClassName();
    }

    @Override
    public String getTenant() {
        return config.getFunctionConfig().getTenant();
    }

    @Override
    public String getNamespace() {
        return config.getFunctionConfig().getNamespace();
    }

    @Override
    public String getFunctionName() {
        return config.getFunctionConfig().getName();
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
    public String getUserConfigValue(String key) {
        if (config.getFunctionConfig().containsUserConfig(key)) {
            return config.getFunctionConfig().getUserConfigOrDefault(key, null);
        } else {
            return null;
        }
    }

    @Override
    public void incrCounter(String key, long amount) {
        if (null != stateContext) {
            stateContext.incr(key, amount);
        } else {
            throw new RuntimeException("State is not enabled.");
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

    @Override
    public CompletableFuture<Void> ack(byte[] messageId, String topic) {
        if (!sourceConsumers.containsKey(topic)) {
            throw new RuntimeException("No such input topic " + topic);
        }

        MessageId actualMessageId = null;
        try {
            actualMessageId = MessageId.fromByteArray(messageId);
        } catch (IOException e) {
            throw new RuntimeException("Invalid message id to ack", e);
        }
        return sourceConsumers.get(topic).acknowledgeAsync(actualMessageId);
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
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
package org.apache.pulsar.functions.runtime.instance;

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class implements the Context interface exposed to the user.
 */
class ContextImpl implements Context {
    private JavaInstanceConfig config;
    private Logger logger;

    // Per Message related
    private String messageId;
    private String currentTopicName;
    private long startTime;

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

    public ContextImpl(JavaInstanceConfig config, Logger logger) {
        this.config = config;
        this.logger = logger;
        this.accumulatedMetrics = new ConcurrentHashMap<>();
    }

    public void setCurrentMessageContext(String messageId, String topicName) {
        this.messageId = messageId;
        this.currentTopicName = topicName;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public String getMessageId() {
        return messageId;
    }

    @Override
    public String getTopicName() {
        return currentTopicName;
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
    public long getMemoryLimit() {
        return config.getLimitsConfig().getMaxMemoryMb() * 1024 * 1024;
    }

    @Override
    public long getTimeBudgetInMs() {
        return config.getLimitsConfig().getMaxTimeMs();
    }

    @Override
    public long getRemainingTimeInMs() {
        return getTimeBudgetInMs() - (System.currentTimeMillis() - startTime);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public String getUserConfigValue(String key) {
        if (config.getFunctionConfig().getUserConfig().containsKey(key)) {
            return config.getFunctionConfig().getUserConfig().get(key);
        } else {
            return null;
        }
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
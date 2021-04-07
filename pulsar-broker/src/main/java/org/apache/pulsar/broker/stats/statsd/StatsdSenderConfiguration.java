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
package org.apache.pulsar.broker.stats.statsd;

import org.apache.pulsar.broker.ServiceConfiguration;

public class StatsdSenderConfiguration {
    public String hostname;
    public int port;
    public String prefix;
    public boolean enableAggregation;
    public int maxPacketSizeInBytes;
    public int aggregationFlushInterval;
    public int aggregationShards;
    public int metricsGenerationInterval;
    public boolean includeTopicsMetrics;
    public boolean includeConsumersMetrics;
    public boolean includeProducersMetrics;

    public StatsdSenderConfiguration(ServiceConfiguration conf) {
        this.hostname = conf.getStatsdServerHostname();
        this.port = conf.getStatsdServerPort();
        this.prefix = conf.getStatsdMetricsPrefix();
        this.enableAggregation = conf.getStatsdEnableAggregation();
        this.maxPacketSizeInBytes = conf.getStatsdMaxPacketSizeBytes();
        this.aggregationFlushInterval = conf.getStatsdAggregationFlushInterval();
        this.aggregationShards = conf.getStatsdAggregationShards();
        this.metricsGenerationInterval = conf.getStatsMetricsGenerationInterval();
        this.includeTopicsMetrics = conf.getStatsdIncludeTopicsMetrics();
        this.includeConsumersMetrics = conf.getStatsdIncludeConsumersMetrics();
        this.includeProducersMetrics = conf.getStatsdIncludeProducersMetrics();
    }
}

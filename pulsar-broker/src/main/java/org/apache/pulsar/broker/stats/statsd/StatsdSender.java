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

import static org.apache.bookkeeper.util.SafeRunnable.safeRun;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.stats.prometheus.NamespaceStatsAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsdSender implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(StatsdSender.class);

    private PulsarService pulsar;
    private StatsdSenderConfiguration configuration;
    private ScheduledExecutorService statsdSenderExecutor;

    private StatsDClient client;

    public StatsdSender(PulsarService pulsar, StatsdSenderConfiguration conf) {
        this.pulsar = pulsar;
        this.statsdSenderExecutor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("statsd-metrics-sender"));
        this.configuration = conf;
        this.client = new NonBlockingStatsDClientBuilder()
                .prefix(this.configuration.prefix)
                .hostname(this.configuration.hostname)
                .port(this.configuration.port)
                .maxPacketSizeBytes(this.configuration.maxPacketSizeInBytes)
                .enableAggregation(this.configuration.enableAggregation)
                .aggregationFlushInterval(this.configuration.aggregationFlushInterval)
                .aggregationShards(this.configuration.aggregationShards)
                .build();
    }

    public void start() {
        final int initialDelay = 10;
        final int internal = this.configuration.metricsGenerationInterval;
        log.info("Scheduling a thread to send metrics to statsd each [{}], will start in [{}]", internal,
                initialDelay);
        this.statsdSenderExecutor.scheduleAtFixedRate(
                safeRun(this::generateAndSend), initialDelay, internal, TimeUnit.SECONDS);
    }

    public void generateAndSend() {
        NamespaceStatsAggregator.generate(this.pulsar, this.configuration.includeTopicsMetrics,
                this.configuration.includeConsumersMetrics, this.configuration.includeProducersMetrics,
                this.client);
    }

    @Override
    public void close() throws Exception {
        this.client.stop();
        this.client.close();
    }
}

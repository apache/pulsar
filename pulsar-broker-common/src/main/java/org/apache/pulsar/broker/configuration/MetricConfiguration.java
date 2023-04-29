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
package org.apache.pulsar.broker.configuration;

import static org.apache.pulsar.broker.ServiceConfiguration.CATEGORY_METRICS;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

@Getter
@Setter
@ToString
public class MetricConfiguration implements PulsarConfiguration {
    private ServiceConfiguration serviceConfiguration;

    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "If true, export publisher stats when returning topics stats from the admin rest api"
    )
    private boolean exposePublisherStats = true;
    @FieldContext(
            category = CATEGORY_METRICS,
            minValue = 1,
            doc = "Stats update frequency in seconds"
    )
    private int statsUpdateFrequencyInSecs = 60;
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "Stats update initial delay in seconds"
    )
    private int statsUpdateInitialDelayInSecs = 60;
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "If true, aggregate publisher stats of PartitionedTopicStats by producerName"
    )
    private boolean aggregatePublisherStatsByProducerName = false;

    /**** --- Metrics. --- ****/
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "Whether the '/metrics' endpoint requires authentication. Defaults to false."
                    + "'authenticationEnabled' must also be set for this to take effect."
    )
    private boolean authenticateMetricsEndpoint = false;
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "If true, export topic level metrics otherwise namespace level"
    )
    private boolean exposeTopicLevelMetricsInPrometheus = true;
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "If true, export buffered metrics"
    )
    private boolean metricsBufferResponse = false;
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "If true, export consumer level metrics otherwise namespace level"
    )
    private boolean exposeConsumerLevelMetricsInPrometheus = false;
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "If true, export producer level metrics otherwise namespace level"
    )
    private boolean exposeProducerLevelMetricsInPrometheus = false;
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "If true, export managed ledger metrics (aggregated by namespace)"
    )
    private boolean exposeManagedLedgerMetricsInPrometheus = true;
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "If true, export managed cursor metrics"
    )
    private boolean exposeManagedCursorMetricsInPrometheus = false;
    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "Classname of Pluggable JVM GC metrics logger that can log GC specific metrics")
    private String jvmGCMetricsLoggerClassName;

    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "Enable expose the precise backlog stats.\n"
                    + " Set false to use published counter and consumed counter to calculate,\n"
                    + " this would be more efficient but may be inaccurate. Default is false."
    )
    private boolean exposePreciseBacklogInPrometheus = false;

    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "Time in milliseconds that metrics endpoint would time out. Default is 30s.\n"
                    + " Increase it if there are a lot of topics to expose topic-level metrics.\n"
                    + " Set it to 0 to disable timeout."
    )
    private long metricsServletTimeoutMs = 30000;

    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "Enable expose the backlog size for each subscription when generating stats.\n"
                    + " Locking is used for fetching the status so default to false."
    )
    private boolean exposeSubscriptionBacklogSizeInPrometheus = false;

    @FieldContext(
            category = CATEGORY_METRICS,
            doc = "Enable splitting topic and partition label in Prometheus.\n"
                    + " If enabled, a topic name will split into 2 parts, one is topic name without partition index,\n"
                    + " another one is partition index, e.g. (topic=xxx, partition=0).\n"
                    + " If the topic is a non-partitioned topic, -1 will be used for the partition index.\n"
                    + " If disabled, one label to represent the topic and partition, e.g. (topic=xxx-partition-0)\n"
                    + " Default is false."
    )
    private boolean splitTopicAndPartitionLabelInPrometheus = false;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_METRICS,
            doc = "Enable expose the broker bundles metrics."
    )
    private boolean exposeBundlesMetricsInPrometheus = false;

    @ToString.Exclude
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Properties properties = new Properties();

    public Object getProperty(String key) {
        return properties.get(key);
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}

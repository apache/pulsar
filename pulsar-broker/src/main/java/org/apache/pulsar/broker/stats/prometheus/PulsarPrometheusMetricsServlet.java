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
package org.apache.pulsar.broker.stats.prometheus;

import java.io.IOException;
import javax.servlet.ServletOutputStream;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;

public class PulsarPrometheusMetricsServlet extends PrometheusMetricsServlet {

    private static final long serialVersionUID = 1L;

    private final PulsarService pulsar;
    private final ServiceConfiguration config;
    public PulsarPrometheusMetricsServlet(PulsarService pulsar) {
        super(pulsar.getConfiguration().getMetricsServletTimeoutMs(), pulsar.getConfiguration().getClusterName());
        this.pulsar = pulsar;
        this.config = pulsar.getConfiguration();
    }

    @Override
    protected void generateMetrics(String cluster, ServletOutputStream outputStream) throws IOException {
        PrometheusMetricsGenerator.generate(pulsar,
            config.isExposeTopicLevelMetricsInPrometheus(),
            config.isExposeConsumerLevelMetricsInPrometheus(),
            config.isExposeProducerLevelMetricsInPrometheus(),
            config.isSplitTopicAndPartitionLabelInPrometheus(),
            outputStream, metricsProviders);
    }
}

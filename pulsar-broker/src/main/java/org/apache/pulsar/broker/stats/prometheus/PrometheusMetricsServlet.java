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
package org.apache.pulsar.broker.stats.prometheus;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusMetricsServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private final PulsarService pulsar;
    private final boolean shouldExportTopicMetrics;
    private final boolean shouldExportConsumerMetrics;
    private final boolean shouldExportProducerMetrics;
    private final long metricsServletTimeoutMs;
    private final boolean splitTopicAndPartitionLabel;
    private List<PrometheusRawMetricsProvider> metricsProviders;

    private ExecutorService executor = null;

    public PrometheusMetricsServlet(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                    boolean shouldExportProducerMetrics, boolean splitTopicAndPartitionLabel) {
        this.pulsar = pulsar;
        this.shouldExportTopicMetrics = includeTopicMetrics;
        this.shouldExportConsumerMetrics = includeConsumerMetrics;
        this.shouldExportProducerMetrics = shouldExportProducerMetrics;
        this.metricsServletTimeoutMs = pulsar.getConfiguration().getMetricsServletTimeoutMs();
        this.splitTopicAndPartitionLabel = splitTopicAndPartitionLabel;
    }

    @Override
    public void init() throws ServletException {
        executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("prometheus-stats"));
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        AsyncContext context = request.startAsync();
        context.setTimeout(metricsServletTimeoutMs);
        executor.execute(safeRun(() -> {
            boolean useBuffer = true;
            //param for internal tests.
            String param = request.getParameter("useBuffer");
            if (StringUtils.isNotBlank(param)) {
                useBuffer = param.equalsIgnoreCase("true");
            }
            long start = System.currentTimeMillis();
            HttpServletResponse res = (HttpServletResponse) context.getResponse();
            try {
                res.setStatus(HttpStatus.OK_200);
                res.setContentType("text/plain");
                PrometheusMetricsGenerator.generate(pulsar, shouldExportTopicMetrics, shouldExportConsumerMetrics,
                        shouldExportProducerMetrics, splitTopicAndPartitionLabel, res.getOutputStream(),
                        metricsProviders, useBuffer);
            } catch (Exception e) {
                long end = System.currentTimeMillis();
                long time = end - start;
                if (e instanceof EOFException) {
                    // NO STACKTRACE
                    log.error("Failed to send metrics, "
                            + "likely the client or this server closed "
                            + "the connection due to a timeout ({} ms elapsed): {}", time, e + "");
                } else {
                    log.error("Failed to generate prometheus stats, {} ms elapsed", time, e);
                }
                res.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
            } finally {
                long end = System.currentTimeMillis();
                long time = end - start;
                try {
                    context.complete();
                } catch (IllegalStateException e) {
                    // this happens when metricsServletTimeoutMs expires
                    // java.lang.IllegalStateException: AsyncContext completed and/or Request lifecycle recycled
                    log.error("Failed to generate prometheus stats, "
                            + "this is likely due to metricsServletTimeoutMs: {} ms elapsed: {}", time, e + "");
                }
            }
        }));
    }

    @Override
    public void destroy() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    public void addRawMetricsProvider(PrometheusRawMetricsProvider metricsProvider) {
        if (metricsProviders == null) {
            metricsProviders = new LinkedList<>();
        }
        metricsProviders.add(metricsProvider);
    }

    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsServlet.class);
}

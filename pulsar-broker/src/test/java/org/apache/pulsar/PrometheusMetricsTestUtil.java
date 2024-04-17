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

package org.apache.pulsar;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.broker.stats.prometheus.PrometheusRawMetricsProvider;
import org.eclipse.jetty.server.HttpOutput;

public class PrometheusMetricsTestUtil {
    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                boolean includeProducerMetrics, OutputStream out) throws IOException {
        generate(new PrometheusMetricsGenerator(pulsar, includeTopicMetrics, includeConsumerMetrics,
                includeProducerMetrics, false, Clock.systemUTC()), out, null);
    }

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                boolean includeProducerMetrics, boolean splitTopicAndPartitionIndexLabel,
                                OutputStream out) throws IOException {
        generate(new PrometheusMetricsGenerator(pulsar, includeTopicMetrics, includeConsumerMetrics,
                includeProducerMetrics, splitTopicAndPartitionIndexLabel, Clock.systemUTC()), out, null);
    }

    public static void generate(PrometheusMetricsGenerator metricsGenerator, OutputStream out,
                                List<PrometheusRawMetricsProvider> metricsProviders) throws IOException {
        PrometheusMetricsGenerator.MetricsBuffer metricsBuffer =
                metricsGenerator.renderToBuffer(MoreExecutors.directExecutor(), metricsProviders);
        try {
            ByteBuf buffer = null;
            try {
                buffer = metricsBuffer.getBufferFuture().get(5, TimeUnit.SECONDS).getUncompressedBuffer();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (ExecutionException | TimeoutException e) {
                throw new IOException(e);
            }
            if (buffer == null) {
                return;
            }
            if (out instanceof HttpOutput) {
                HttpOutput output = (HttpOutput) out;
                ByteBuffer[] nioBuffers = buffer.nioBuffers();
                for (ByteBuffer nioBuffer : nioBuffers) {
                    output.write(nioBuffer);
                }
            } else {
                int length = buffer.readableBytes();
                if (length > 0) {
                    buffer.duplicate().readBytes(out, length);
                }
            }
        } finally {
            metricsBuffer.release();
        }
    }
}

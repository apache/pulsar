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

import static org.apache.pulsar.broker.web.GzipHandlerUtil.isGzipCompressionEnabledForEndpoint;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.eclipse.jetty.server.HttpOutput;

@Slf4j
public class PulsarPrometheusMetricsServlet extends PrometheusMetricsServlet {
    private static final long serialVersionUID = 1L;
    private static final int EXECUTOR_MAX_THREADS = 4;

    private final PrometheusMetricsGenerator prometheusMetricsGenerator;
    private final boolean gzipCompressionEnabledForMetrics;

    public PulsarPrometheusMetricsServlet(PulsarService pulsar, boolean includeTopicMetrics,
                                          boolean includeConsumerMetrics, boolean includeProducerMetrics,
                                          boolean splitTopicAndPartitionLabel) {
        super(pulsar.getConfiguration().getMetricsServletTimeoutMs(), pulsar.getConfiguration().getClusterName(),
                EXECUTOR_MAX_THREADS);
        MetricsExports.initialize();
        prometheusMetricsGenerator =
                new PrometheusMetricsGenerator(pulsar, includeTopicMetrics, includeConsumerMetrics,
                        includeProducerMetrics, splitTopicAndPartitionLabel, Clock.systemUTC());
        gzipCompressionEnabledForMetrics = isGzipCompressionEnabledForEndpoint(
                pulsar.getConfiguration().getHttpServerGzipCompressionExcludedPaths(), DEFAULT_METRICS_PATH);
    }


    @Override
    public void destroy() {
        super.destroy();
        prometheusMetricsGenerator.close();
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        AsyncContext context = request.startAsync();
        // set hard timeout to 2 * timeout
        if (metricsServletTimeoutMs > 0) {
            context.setTimeout(metricsServletTimeoutMs * 2);
        }
        long startNanos = System.nanoTime();
        AtomicBoolean skipWritingResponse = new AtomicBoolean(false);
        context.addListener(new AsyncListener() {
            @Override
            public void onComplete(AsyncEvent event) throws IOException {
            }

            @Override
            public void onTimeout(AsyncEvent event) throws IOException {
                log.warn("Prometheus metrics request timed out");
                skipWritingResponse.set(true);
                HttpServletResponse res = (HttpServletResponse) context.getResponse();
                if (!res.isCommitted()) {
                    res.setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR_500);
                }
                context.complete();
            }

            @Override
            public void onError(AsyncEvent event) throws IOException {
                skipWritingResponse.set(true);
            }

            @Override
            public void onStartAsync(AsyncEvent event) throws IOException {
            }
        });
        PrometheusMetricsGenerator.MetricsBuffer metricsBuffer =
                prometheusMetricsGenerator.renderToBuffer(executor, metricsProviders);
        if (metricsBuffer == null) {
            log.info("Service is closing, skip writing metrics.");
            response.setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR_500);
            context.complete();
            return;
        }
        boolean compressOutput = gzipCompressionEnabledForMetrics && isGzipAccepted(request);
        metricsBuffer.getBufferFuture().thenCompose(responseBuffer -> {
            if (compressOutput) {
                return responseBuffer.getCompressedBuffer(executor);
            } else {
                return CompletableFuture.completedFuture(responseBuffer.getUncompressedBuffer());
            }
        }).whenComplete((buffer, ex) -> executor.execute(() -> {
            try {
                long elapsedNanos = System.nanoTime() - startNanos;
                // check if the request has been timed out, implement a soft timeout
                // so that response writing can continue to up to 2 * timeout
                if (metricsServletTimeoutMs > 0 && elapsedNanos > TimeUnit.MILLISECONDS.toNanos(
                        metricsServletTimeoutMs)) {
                    log.warn("Prometheus metrics request was too long in queue ({}ms). Skipping sending metrics.",
                            TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
                    if (!response.isCommitted() && !skipWritingResponse.get()) {
                        response.setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR_500);
                    }
                    return;
                }
                if (skipWritingResponse.get()) {
                    log.warn("Response has timed or failed, skip writing metrics.");
                    return;
                }
                if (response.isCommitted()) {
                    log.warn("Response is already committed, cannot write metrics");
                    return;
                }
                if (ex != null) {
                    log.error("Failed to generate metrics", ex);
                    response.setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR_500);
                    return;
                }
                if (buffer == null) {
                    log.error("Failed to generate metrics, buffer is null");
                    response.setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR_500);
                } else {
                    response.setStatus(HTTP_STATUS_OK_200);
                    response.setContentType("text/plain;charset=utf-8");
                    if (compressOutput) {
                        response.setHeader("Content-Encoding", "gzip");
                    }
                    ServletOutputStream outputStream = response.getOutputStream();
                    if (outputStream instanceof HttpOutput) {
                        HttpOutput output = (HttpOutput) outputStream;
                        for (ByteBuffer nioBuffer : buffer.nioBuffers()) {
                            output.write(nioBuffer);
                        }
                    } else {
                        int length = buffer.readableBytes();
                        if (length > 0) {
                            buffer.duplicate().readBytes(outputStream, length);
                        }
                    }
                }
            } catch (EOFException e) {
                log.error("Failed to write metrics to response due to EOFException");
            } catch (IOException e) {
                log.error("Failed to write metrics to response", e);
            } finally {
                metricsBuffer.release();
                context.complete();
            }
        }));
    }

    private boolean isGzipAccepted(HttpServletRequest request) {
        String acceptEncoding = request.getHeader("Accept-Encoding");
        if (acceptEncoding != null) {
            return Arrays.stream(acceptEncoding.split(","))
                    .map(String::trim)
                    .anyMatch(str -> "gzip".equalsIgnoreCase(str));
        }
        return false;
    }
}

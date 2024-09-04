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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

public class PrometheusMetricsGeneratorTest {

    // reproduce issue #22575
    @Test
    public void testReproducingBufferOverflowExceptionAndEOFExceptionBugsInGzipCompression()
            throws ExecutionException, InterruptedException, IOException {
        PulsarService pulsar = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        when(pulsar.getConfiguration()).thenReturn(serviceConfiguration);

        // generate a random byte buffer which is 8 bytes less than the minimum compress buffer size limit
        // this will trigger the BufferOverflowException bug in writing the gzip trailer
        // it will also trigger another bug in finishing the gzip compression stream when the compress buffer is full
        // which results in EOFException
        Random random = new Random();
        byte[] inputBytes = new byte[8192 - 8];
        random.nextBytes(inputBytes);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(inputBytes);

        PrometheusMetricsGenerator generator =
                new PrometheusMetricsGenerator(pulsar, false, false, false, false, Clock.systemUTC()) {
                    // override the generateMetrics method to return the random byte buffer for gzip compression
                    // instead of the actual metrics
                    @Override
                    protected ByteBuf generateMetrics(List<PrometheusRawMetricsProvider> metricsProviders) {
                        return byteBuf;
                    }
                };

        PrometheusMetricsGenerator.MetricsBuffer metricsBuffer =
                generator.renderToBuffer(MoreExecutors.directExecutor(), Collections.emptyList());
        try {
            PrometheusMetricsGenerator.ResponseBuffer responseBuffer = metricsBuffer.getBufferFuture().get();

            ByteBuf compressed = responseBuffer.getCompressedBuffer(MoreExecutors.directExecutor()).get();
            byte[] compressedBytes = new byte[compressed.readableBytes()];
            compressed.readBytes(compressedBytes);
            try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(compressedBytes))) {
                byte[] uncompressedBytes = IOUtils.toByteArray(gzipInputStream);
                assertEquals(uncompressedBytes, inputBytes);
            }
        } finally {
            metricsBuffer.release();
        }
    }
}
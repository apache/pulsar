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

import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.ByteArrayOutputStream;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PrometheusMetricStreamsTest {

    private PrometheusMetricStreams underTest;

    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        underTest = new PrometheusMetricStreams();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        underTest.releaseAll();
    }

    @Test
    public void canWriteSampleWithoutLabels() {
        underTest.writeSample("my-metric", 123);

        String actual = writeToString();

        assertTrue(actual.startsWith("# TYPE my-metric gauge"), "Gauge type line missing");
        assertTrue(actual.contains("my-metric{} 123"), "Metric line missing");
    }

    @Test
    public void canWriteSampleWithLabels() {
        underTest.writeSample("my-other-metric", 123, "cluster", "local");
        underTest.writeSample("my-other-metric", 456, "cluster", "local", "namespace", "my-ns");

        String actual = writeToString();

        assertTrue(actual.startsWith("# TYPE my-other-metric gauge"), "Gauge type line missing");
        assertTrue(actual.contains("my-other-metric{cluster=\"local\"} 123"), "Cluster metric line missing");
        assertTrue(actual.contains("my-other-metric{cluster=\"local\",namespace=\"my-ns\"} 456"),
                "Cluster and Namespace metric line missing");
    }

    private String writeToString() {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.directBuffer();
        try {
            SimpleTextOutputStream stream = new SimpleTextOutputStream(buffer);
            underTest.flushAllToStream(stream);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int readIndex = buffer.readerIndex();
            int readableBytes = buffer.readableBytes();
            for (int i = 0; i < readableBytes; i++) {
                out.write(buffer.getByte(readIndex + i));
            }
            return out.toString();
        } finally {
            buffer.release();
        }
    }
}
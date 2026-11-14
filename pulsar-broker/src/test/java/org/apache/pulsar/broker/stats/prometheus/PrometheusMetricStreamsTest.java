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

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
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

    @DataProvider
    public Object[][] sampleValues() {
        return new Object[][]{
                {Long.MIN_VALUE},
                {Long.MAX_VALUE},
                {9_007_199_254_740_993L},
                {0L},
                {0.0},
                {-0.0},
                {1.0},
                {0.0001},
                {-0.125},
                {12.3456},
                {Double.NaN},
                {Double.NEGATIVE_INFINITY},
                {Double.POSITIVE_INFINITY}
        };
    }

    @Test(dataProvider = "sampleValues")
    public void primitiveSamplesKeepNumberFormatting(Number value) {
        String[] labels = {"cluster", "local"};
        String[] noLabels = {};
        PrometheusMetricStreams boxed = new PrometheusMetricStreams();
        String expectedSample;
        try {
            boxed.writeSample("my-metric", value, labels);
            expectedSample = writeToString(boxed).substring("# TYPE my-metric gauge\n".length());
        } finally {
            boxed.releaseAll();
        }

        if (value instanceof Long) {
            underTest.writeSample("my-metric", value.longValue(), labels);
            underTest.writeSample("my-metric", value.longValue(), noLabels, labels);
            underTest.writeSample("my-metric", value.longValue(), noLabels, "cluster", "local");
        } else {
            underTest.writeSample("my-metric", value.doubleValue(), labels);
            underTest.writeSample("my-metric", value.doubleValue(), noLabels, labels);
            underTest.writeSample("my-metric", value.doubleValue(), noLabels, "cluster", "local");
        }

        assertThat(writeToString()).isEqualTo("# TYPE my-metric gauge\n" + expectedSample.repeat(3));
    }

    @Test
    public void separateLabelArraysPreserveOrderAndEscaping() {
        String[] labels = {"cluster", "lo\"cal", "namespace", null};
        String[] additionalLabels = {"topic", "test\\topic", "custom", "\"value\""};
        underTest.writeSample("my-metric", (Number) 123, labels, additionalLabels);
        underTest.writeSample("my-metric", 456L, labels, additionalLabels);
        underTest.writeSample("my-metric", 12.5, labels, additionalLabels);

        String expectedLabels = "{cluster=\"lo\\\"cal\",namespace=\"\",topic=\"test\\topic\","
                + "custom=\"\\\"value\\\"\"}";
        assertThat(writeToString()).isEqualTo("# TYPE my-metric gauge\n"
                + "my-metric" + expectedLabels + " 123\n"
                + "my-metric" + expectedLabels + " 456\n"
                + "my-metric" + expectedLabels + " 12.500\n");
        assertThat(labels).containsExactly("cluster", "lo\"cal", "namespace", null);
        assertThat(additionalLabels).containsExactly("topic", "test\\topic", "custom", "\"value\"");
    }

    @Test
    public void additionalLabelsHandleEmptyArrays() {
        String[] noLabels = {};
        String[] labels = {"cluster", "local"};
        underTest.writeSample("my-metric", (Number) 123, noLabels, noLabels);
        underTest.writeSample("my-metric", 456L, labels, noLabels);
        underTest.writeSample("my-metric", 12.5, noLabels, labels);
        underTest.writeSample("my-metric", (Number) 789, labels, "topic", "test\"topic");
        underTest.writeSample("my-metric", 987L, labels, "topic", null);
        underTest.writeSample("my-metric", 45.25, labels, "topic", "test");
        underTest.writeSample("my-metric", 654L, labels, (String[]) null);
        underTest.writeSample("my-metric", 32.5, noLabels, (String[]) null);

        assertThat(writeToString()).isEqualTo("# TYPE my-metric gauge\n"
                + "my-metric{} 123\n"
                + "my-metric{cluster=\"local\"} 456\n"
                + "my-metric{cluster=\"local\"} 12.500\n"
                + "my-metric{cluster=\"local\",topic=\"test\\\"topic\"} 789\n"
                + "my-metric{cluster=\"local\",topic=\"\"} 987\n"
                + "my-metric{cluster=\"local\",topic=\"test\"} 45.250\n"
                + "my-metric{cluster=\"local\"} 654\n"
                + "my-metric{} 32.500\n");
    }

    @Test
    public void boxedSamplesKeepTheirNumberType() {
        String[] labels = {"cluster", "local"};
        String[] additionalLabels = {"topic", "test"};
        Float value = 1.25F;
        underTest.writeSample("my-metric", value, labels);
        underTest.writeSample("my-metric", value, labels, additionalLabels);
        underTest.writeSample("my-metric", value, labels, "topic", "test");

        assertThat(writeToString()).isEqualTo("# TYPE my-metric gauge\n"
                + "my-metric{cluster=\"local\"} 1.25\n"
                + "my-metric{cluster=\"local\",topic=\"test\"} 1.25\n"
                + "my-metric{cluster=\"local\",topic=\"test\"} 1.25\n");
    }

    private String writeToString() {
        return writeToString(underTest);
    }

    private String writeToString(PrometheusMetricStreams metricStreams) {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.directBuffer();
        try {
            SimpleTextOutputStream stream = new SimpleTextOutputStream(buffer);
            metricStreams.flushAllToStream(stream);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int readIndex = buffer.readerIndex();
            int readableBytes = buffer.readableBytes();
            for (int i = 0; i < readableBytes; i++) {
                out.write(buffer.getByte(readIndex + i));
            }
            return out.toString(StandardCharsets.UTF_8);
        } finally {
            buffer.release();
        }
    }
}

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
package org.apache.pulsar.client.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ClientMetricsTest extends ProducerConsumerBase {

    InMemoryMetricReader reader;
    OpenTelemetrySdk otel;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        this.reader = InMemoryMetricReader.create();
        SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
                .registerMetricReader(reader)
                .build();
        this.otel = OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (otel != null) {
            otel.close();
            otel = null;
        }
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    private Map<String, MetricData> collectMetrics() {
        Map<String, MetricData> metrics = new TreeMap<>();
        for (MetricData md : reader.collectAllMetrics()) {
            metrics.put(md.getName(), md);
        }
        return metrics;
    }

    private void assertCounterValue(Map<String, MetricData> metrics, String name, long expectedValue,
                                    Attributes expectedAttributes) {
        assertEquals(getCounterValue(metrics, name, expectedAttributes), expectedValue);
    }

    private long getCounterValue(Map<String, MetricData> metrics, String name,
                                    Attributes expectedAttributes) {
        MetricData md = metrics.get(name);
        assertNotNull(md, "metric not found: " + name);
        assertEquals(md.getType(), MetricDataType.LONG_SUM);

        Map<AttributeKey<?>, Object> expectedAttributesMap = expectedAttributes.asMap();
        for (var ex : md.getLongSumData().getPoints()) {
            if (ex.getAttributes().asMap().equals(expectedAttributesMap)) {
                return ex.getValue();
            }
        }

        fail("metric attributes not found: " + expectedAttributes);
        return -1;
    }

    private void assertHistoCountValue(Map<String, MetricData> metrics, String name, long expectedCount,
                                       Attributes expectedAttributes) {
        assertEquals(getHistoCountValue(metrics, name, expectedAttributes), expectedCount);
    }

    private long getHistoCountValue(Map<String, MetricData> metrics, String name,
                                    Attributes expectedAttributes) {
        MetricData md = metrics.get(name);
        assertNotNull(md, "metric not found: " + name);
        assertEquals(md.getType(), MetricDataType.HISTOGRAM);

        Map<AttributeKey<?>, Object> expectedAttributesMap = expectedAttributes.asMap();
        for (var ex : md.getHistogramData().getPoints()) {
            if (ex.getAttributes().asMap().equals(expectedAttributesMap)) {
                return ex.getCount();
            }
        }

        fail("metric attributes not found: " + expectedAttributes);
        return -1;
    }

    @Test
    public void testProducerMetrics() throws Exception {
        String topic = newTopicName();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(otel)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 5; i++) {
            producer.send("Hello");
        }

        Attributes nsAttrs = Attributes.builder()
                .put("pulsar.tenant", "my-property")
                .put("pulsar.namespace", "my-property/my-ns")
                .build();
        Attributes nsAttrsSuccess = nsAttrs.toBuilder()
                .put("pulsar.response.status", "success")
                .build();

        var metrics = collectMetrics();

        assertCounterValue(metrics, "pulsar.client.connection.opened", 1, Attributes.empty());
        assertCounterValue(metrics, "pulsar.client.producer.message.pending.count", 0, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.producer.message.pending.size", 0, nsAttrs);

        assertHistoCountValue(metrics, "pulsar.client.lookup.duration", 1,
                Attributes.builder()
                        .put("pulsar.lookup.transport-type", "binary")
                        .put("pulsar.lookup.type", "topic")
                        .put("pulsar.response.status", "success")
                        .build());
        assertHistoCountValue(metrics, "pulsar.client.lookup.duration", 1,
                Attributes.builder()
                        .put("pulsar.lookup.transport-type", "binary")
                        .put("pulsar.lookup.type", "metadata")
                        .put("pulsar.response.status", "success")
                        .build());

        assertHistoCountValue(metrics, "pulsar.client.producer.message.send.duration", 5, nsAttrsSuccess);
        assertHistoCountValue(metrics, "pulsar.client.producer.rpc.send.duration", 5, nsAttrsSuccess);
        assertCounterValue(metrics, "pulsar.client.producer.message.send.size", "hello".length() * 5, nsAttrs);


        assertCounterValue(metrics, "pulsar.client.producer.opened", 1, nsAttrs);

        producer.close();
        client.close();

        metrics = collectMetrics();
        assertCounterValue(metrics, "pulsar.client.producer.closed", 1, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.connection.closed", 1, Attributes.empty());
    }

    @Test
    public void testConnectionsFailedMetrics() throws Exception {
        String topic = newTopicName();

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://invalid-pulsar-address:1234")
                .operationTimeout(3, TimeUnit.SECONDS)
                .openTelemetry(otel)
                .build();

        Assertions.assertThatThrownBy(() -> {
                    client.newProducer(Schema.STRING)
                            .topic(topic)
                            .create();
                }).isInstanceOf(Exception.class);


        var metrics = collectMetrics();

        Assertions.assertThat(
                getCounterValue(metrics, "pulsar.client.connection.failed",
                        Attributes.builder().put("pulsar.failure.type", "tcp-failed").build()))
                .isGreaterThanOrEqualTo(1L);
    }

    @Test
    public void testPublishFailedMetrics() throws Exception {
        String topic = newTopicName();

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(admin.getServiceUrl())
                .operationTimeout(3, TimeUnit.SECONDS)
                .openTelemetry(otel)
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .sendTimeout(3, TimeUnit.SECONDS)
                .create();

        // Make the client switch to non-existing broker to make publish fail
        client.updateServiceUrl("pulsar://invalid-address:6650");


        try {
            producer.send("Hello");
            fail("Should have failed to publish");
        } catch (Exception e) {
            // expected
        }

        var metrics = collectMetrics();

        Attributes nsAttrs = Attributes.builder()
                .put("pulsar.tenant", "my-property")
                .put("pulsar.namespace", "my-property/my-ns")
                .build();
        Attributes nsAttrsFailure = nsAttrs.toBuilder()
                .put("pulsar.response.status", "failed")
                .build();

        assertCounterValue(metrics, "pulsar.client.producer.message.pending.count", 0, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.producer.message.pending.size", 0, nsAttrs);
        assertHistoCountValue(metrics, "pulsar.client.producer.message.send.duration", 1, nsAttrsFailure);
        assertHistoCountValue(metrics, "pulsar.client.producer.rpc.send.duration", 1, nsAttrsFailure);
    }

    @Test
    public void testConsumerMetrics() throws Exception {
        String topic = newTopicName();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(otel)
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("my-sub")
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("Hello");
        }

        Thread.sleep(1000);

        Attributes nsAttrs = Attributes.builder()
                .put("pulsar.tenant", "my-property")
                .put("pulsar.namespace", "my-property/my-ns")
                .put("pulsar.subscription", "my-sub")
                .build();
        var metrics = collectMetrics();

        assertCounterValue(metrics, "pulsar.client.connection.opened", 1, Attributes.empty());

        assertHistoCountValue(metrics, "pulsar.client.lookup.duration", 2,
                Attributes.builder()
                        .put("pulsar.lookup.transport-type", "binary")
                        .put("pulsar.lookup.type", "topic")
                        .put("pulsar.response.status", "success")
                        .build());
        assertHistoCountValue(metrics, "pulsar.client.lookup.duration", 2,
                Attributes.builder()
                        .put("pulsar.lookup.transport-type", "binary")
                        .put("pulsar.lookup.type", "metadata")
                        .put("pulsar.response.status", "success")
                        .build());

        assertCounterValue(metrics, "pulsar.client.consumer.receive_queue.count", 10, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.consumer.receive_queue.size", "hello".length() * 10, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.consumer.opened", 1, nsAttrs);

        Message<String> msg1 = consumer.receive();
        consumer.acknowledge(msg1);

        Message<String> msg2 = consumer.receive();
        consumer.negativeAcknowledge(msg2);

        /* Message<String> msg3 = */ consumer.receive();

        metrics = collectMetrics();
        assertCounterValue(metrics, "pulsar.client.consumer.receive_queue.count", 7, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.consumer.receive_queue.size", "hello".length() * 7, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.consumer.message.received.count", 3, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.consumer.message.received.size", "hello".length() * 3, nsAttrs);


        // Let msg3 to reach ack-timeout
        Thread.sleep(3000);

        metrics = collectMetrics();
        assertCounterValue(metrics, "pulsar.client.consumer.receive_queue.count", 8, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.consumer.receive_queue.size", "hello".length() * 8, nsAttrs);

        assertCounterValue(metrics, "pulsar.client.consumer.message.ack", 1, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.consumer.message.nack", 1, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.consumer.message.ack.timeout", 1, nsAttrs);

        client.close();

        metrics = collectMetrics();
        assertCounterValue(metrics, "pulsar.client.consumer.closed", 1, nsAttrs);
        assertCounterValue(metrics, "pulsar.client.connection.closed", 1, Attributes.empty());
    }
}

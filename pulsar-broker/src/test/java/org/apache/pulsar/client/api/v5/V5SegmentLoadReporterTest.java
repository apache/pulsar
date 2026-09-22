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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertTrue;
import java.time.Duration;
import lombok.Cleanup;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * End-to-end coverage for the broker-side segment load reporter sweep (PIP-483): after a
 * scalable topic has live segment topics on the broker, the periodic sweep (driven manually
 * here for determinism) must write a {@link org.apache.pulsar.common.scalable.SegmentLoadStats}
 * record to the metadata store for each hosted segment, which is what the controller's auto
 * split/merge reads.
 */
public class V5SegmentLoadReporterTest extends V5ClientBaseTest {

    @Test
    public void testSweepWritesSegmentLoadRecords() throws Exception {
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<byte[]> producer = v5Client.newProducer(Schema.bytes())
                .topic(topic)
                .create();
        // Produce across keys so both initial segments get a live segment topic on the broker.
        for (int i = 0; i < 50; i++) {
            producer.newMessage().key("k-" + i).value(("v-" + i).getBytes()).send();
        }

        ScalableTopicResources resources =
                getPulsar().getPulsarResources().getScalableTopicResources();
        TopicName parent = TopicName.get(topic);

        // Drive the sweep directly rather than waiting for the 10s scheduled tick.
        getPulsar().getBrokerService().runSegmentLoadReportOnceForTest();

        // Both initial segments (0 and 1) should now have a load record.
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            getPulsar().getBrokerService().runSegmentLoadReportOnceForTest();
            assertTrue(resources.getSegmentLoadAsync(parent, 0).get().isPresent(),
                    "segment 0 load record must be written by the sweep");
            assertTrue(resources.getSegmentLoadAsync(parent, 1).get().isPresent(),
                    "segment 1 load record must be written by the sweep");
        });
    }

    @Test
    public void testNonSegmentTopicsProduceNoLoadRecords() throws Exception {
        // A plain persistent topic must not produce any scalable-segment load record.
        @Cleanup
        org.apache.pulsar.client.api.PulsarClient v4 =
                org.apache.pulsar.client.api.PulsarClient.builder()
                        .serviceUrl(getBrokerServiceUrl()).build();
        String plain = "persistent://" + getNamespace() + "/plain-" + System.nanoTime();
        @Cleanup
        org.apache.pulsar.client.api.Producer<byte[]> p =
                v4.newProducer().topic(plain).create();
        p.send("x".getBytes());

        // Sweep must not throw on non-segment topics (and obviously writes nothing for them).
        getPulsar().getBrokerService().runSegmentLoadReportOnceForTest();
    }
}

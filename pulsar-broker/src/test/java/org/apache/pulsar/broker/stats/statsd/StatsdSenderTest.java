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
package org.apache.pulsar.broker.stats.statsd;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class StatsdSenderTest extends MockedPulsarServiceBaseTest {
    public StatsdSenderTest() {
        super();
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setClusterName("clusterTest");
        conf.setAuthenticationEnabled(false);
        conf.setAuthorizationAllowWildcardsMatching(false);
        conf.setSuperUserRoles(Sets.newHashSet("pulsar.super_user"));
        conf.setStatsdSenderEnabled(true);
        conf.setStatsdServerHostname("pond.local");
        conf.setStatsdServerPort(8125);
        conf.setStatsMetricsGenerationInterval(10);
        internalSetup();
    }

    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void testStatsdWellSent() throws IOException, InterruptedException {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        Producer<byte[]> p1 = pulsarClient.newProducer().topic(topic).create();
        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        final int messages = 100;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes(StandardCharsets.UTF_8));
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
        }

        int port = 8125;
        int bufSize = conf.getStatsdMaxPacketSizeBytes();
        byte buffer[] = new byte[bufSize];
        DatagramSocket socket = new DatagramSocket(port);
        boolean hasReceivedGoodData = false;
        while (!hasReceivedGoodData) {
            DatagramPacket data = new DatagramPacket(buffer, buffer.length);
            socket.receive(data);

            if (new String(data.getData())
                    .contains("pulsar_topics_count:1|g|#namespace:my-property/use/my-ns,cluster:clusterTest")) {
                hasReceivedGoodData = true;
            }
        }
    }

    @Test
    public void testMetricsValues() throws IOException, InterruptedException {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        Producer<byte[]> p1 = pulsarClient.newProducer().topic(topic).create();
        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        final int messages = 100;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes(StandardCharsets.UTF_8));
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
        }

        Thread.sleep(10000);
    }
}

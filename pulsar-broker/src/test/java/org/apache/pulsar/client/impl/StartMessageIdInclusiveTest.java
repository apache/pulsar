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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class StartMessageIdInclusiveTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "subscriptionMode")
    public Object[] subscriptionModeProvider() {
        return new Object[]{SubscriptionMode.Durable, SubscriptionMode.NonDurable};
    }

    @Test
    public void testReaderLatestMessage() throws Exception {
        final String topic = "persistent://my-property/my-ns/my-topic";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        for (int i = 0; i < 10; i++) {
            producer.send(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }

        @Cleanup
        Reader<byte[]> readerWithInclusive = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.latest)
                .startMessageIdInclusive()
                .create();

        Message<byte[]> m = readerWithInclusive.readNext(3, TimeUnit.SECONDS);
        assertNotNull(m);
        assertEquals(m.getData(), "9".getBytes(StandardCharsets.UTF_8));

        @Cleanup
        Reader<byte[]> readerWithoutInclusive = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.latest)
                .create();

        m = readerWithoutInclusive.readNext(3, TimeUnit.SECONDS);
        assertNull(m);
    }

    @Test
    public void testReaderEarliestMessage() throws Exception {
        final String topic = "persistent://my-property/my-ns/my-topic";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        for (int i = 0; i < 10; i++) {
            producer.send(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }

        @Cleanup
        Reader<byte[]> readerWithInclusive = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .startMessageIdInclusive()
                .create();

        Message<byte[]> m = readerWithInclusive.readNext(3, TimeUnit.SECONDS);
        assertNotNull(m);
        assertEquals(m.getData(), "0".getBytes(StandardCharsets.UTF_8));

        @Cleanup
        Reader<byte[]> readerWithoutInclusive = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .startMessageIdInclusive()
                .create();

        m = readerWithoutInclusive.readNext(3, TimeUnit.SECONDS);
        assertNotNull(m);
        assertEquals(m.getData(), "0".getBytes(StandardCharsets.UTF_8));
    }

    @Test(dataProvider = "subscriptionMode")
    public void testConsumerLatestMessage(SubscriptionMode subscriptionMode) throws Exception {
        final String topic = "persistent://my-property/my-ns/my-topic";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        for (int i = 0; i < 10; i++) {
            producer.send(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }

        @Cleanup
        Consumer<byte[]> consumerWithInclusive = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscriptionName("consumerWithInclusive")
                .subscriptionMode(subscriptionMode)
                .startMessageIdInclusive()
                .subscribe();

        Message<byte[]> m = consumerWithInclusive.receive(3, TimeUnit.SECONDS);
        assertNotNull(m);
        assertEquals(m.getData(), "9".getBytes(StandardCharsets.UTF_8));

        @Cleanup
        Consumer<byte[]> consumerWithoutInclusive = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscriptionName("consumerWithoutInclusive")
                .subscriptionMode(subscriptionMode)
                .subscribe();

        m = consumerWithoutInclusive.receive(3, TimeUnit.SECONDS);
        assertNull(m);
    }

    @Test(dataProvider = "subscriptionMode")
    public void testConsumerEarliestMessage(SubscriptionMode subscriptionMode) throws Exception {
        final String topic = "persistent://my-property/my-ns/my-topic";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        for (int i = 0; i < 10; i++) {
            producer.send(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }

        @Cleanup
        Consumer<byte[]> consumerWithInclusive = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("consumerWithInclusive")
                .startMessageIdInclusive()
                .subscriptionMode(subscriptionMode)
                .subscribe();

        Message<byte[]> m = consumerWithInclusive.receive(3, TimeUnit.SECONDS);
        assertNotNull(m);
        assertEquals(m.getData(), "0".getBytes(StandardCharsets.UTF_8));

        @Cleanup
        Consumer<byte[]> consumerWithoutInclusive = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("consumerWithoutInclusive")
                .subscriptionMode(subscriptionMode)
                .subscribe();

        m = consumerWithoutInclusive.receive(3, TimeUnit.SECONDS);
        assertNotNull(m);
        assertEquals(m.getData(), "0".getBytes(StandardCharsets.UTF_8));
    }
}

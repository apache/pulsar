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
package org.apache.pulsar.broker.stats;

import org.apache.bookkeeper.client.PulsarMockLedgerHandle;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.stats.metrics.ManagedCursorMetrics;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.stats.Metrics;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = "quarantine")
public class ManagedCursorMetricsTest extends MockedPulsarServiceBaseTest {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    public void testManagedCursorMetrics() throws Exception {
        final String subName = "my-sub";
        final String topicName = "persistent://my-namespace/use/my-ns/my-topic1";
        final int messageSize = 10;

        ManagedCursorMetrics metrics = new ManagedCursorMetrics(pulsar);

        List<Metrics> metricsList = metrics.generate();
        Assert.assertTrue(metricsList.isEmpty());

        metricsList = metrics.generate();
        Assert.assertTrue(metricsList.isEmpty());

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        for(PulsarMockLedgerHandle ledgerHandle : mockBookKeeper.getLedgerMap().values()) {
            ledgerHandle.close();
        }

        for (int i = 0; i < messageSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            consumer.acknowledge(consumer.receive().getMessageId());
        }
        metricsList = metrics.generate();
        Assert.assertFalse(metricsList.isEmpty());
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistLedgerSucceed"), 0L);
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistLedgerErrors"), 0L);
        Assert.assertNotEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistZookeeperSucceed"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_persistZookeeperErrors"), 0L);
        Assert.assertEquals(metricsList.get(0).getMetrics().get("brk_ml_cursor_nonContiguousDeletedMessagesRange"), 0L);
    }

}

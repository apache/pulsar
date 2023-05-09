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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class CustomMessageIdTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider
    public static Object[][] enableBatching() {
        return new Object[][]{
                { true },
                { false }
        };
    }

    @Test
    public void testSeek() throws Exception {
        final var topic = "persistent://my-property/my-ns/test-seek-" + System.currentTimeMillis();
        @Cleanup final var producer = pulsarClient.newProducer(Schema.INT32).topic(topic).create();
        final var msgIds = new ArrayList<SimpleMessageIdImpl>();
        for (int i = 0; i < 10; i++) {
            msgIds.add(new SimpleMessageIdImpl((MessageIdAdv) producer.send(i)));
        }
        @Cleanup final var consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic).subscriptionName("sub").subscribe();
        consumer.seek(msgIds.get(6));
        final var msg = consumer.receive(3, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals(msg.getValue(), 7);
    }

    @Test(dataProvider = "enableBatching")
    public void testAcknowledgment(boolean enableBatching) throws Exception {
        final var topic = "persistent://my-property/my-ns/test-ack-"
                + enableBatching + System.currentTimeMillis();
        final var producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .enableBatching(enableBatching)
                .batchingMaxMessages(10)
                .batchingMaxPublishDelay(300, TimeUnit.MILLISECONDS)
                .create();
        final var consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("sub")
                .enableBatchIndexAcknowledgment(true)
                .isAckReceiptEnabled(true)
                .subscribe();
        for (int i = 0; i < 10; i++) {
            producer.sendAsync(i);
        }
        final var msgIds = new ArrayList<SimpleMessageIdImpl>();
        for (int i = 0; i < 10; i++) {
            final var msg = consumer.receive();
            final var msgId = new SimpleMessageIdImpl((MessageIdAdv) msg.getMessageId());
            msgIds.add(msgId);
            if (enableBatching) {
                assertTrue(msgId.getBatchIndex() >= 0 && msgId.getBatchSize() > 0);
            } else {
                assertFalse(msgId.getBatchIndex() >= 0 && msgId.getBatchSize() > 0);
            }
        }
        consumer.acknowledgeCumulative(msgIds.get(8));
        consumer.redeliverUnacknowledgedMessages();
        final var msg = consumer.receive(3, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals(msg.getValue(), 9);
    }

    private record SimpleMessageIdImpl(long ledgerId, long entryId, int batchIndex, int batchSize)
            implements MessageIdAdv {

        public SimpleMessageIdImpl(MessageIdAdv msgId) {
            this(msgId.getLedgerId(), msgId.getEntryId(), msgId.getBatchIndex(), msgId.getBatchSize());
        }

        @Override
        public byte[] toByteArray() {
            return new byte[0]; // never used
        }

        @Override
        public long getLedgerId() {
            return ledgerId;
        }

        @Override
        public long getEntryId() {
            return entryId;
        }

        @Override
        public int getBatchIndex() {
            return batchIndex;
        }

        @Override
        public int getBatchSize() {
            return batchSize;
        }
    }
}

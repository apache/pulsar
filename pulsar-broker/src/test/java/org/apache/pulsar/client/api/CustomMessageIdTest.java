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
import static org.testng.Assert.assertNotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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

    @Test(timeOut = 30000)
    public void testSeek() throws Exception {
        final String topic = "persistent://my-property/my-ns/test-seek-" + System.currentTimeMillis();
        final var msgIdList = produceMessages(topic);
        @Cleanup final var consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        final int ackIndex = msgIdList.size() / 2 + 1;
        consumer.seek(msgIdList.get(ackIndex));
        final var msg = consumer.receive(3, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals(msg.getValue(), "msg-" + (ackIndex + 1));
    }

    @Test(timeOut = 30000)
    public void testAck() throws Exception {
        final String topic = "persistent://my-property/my-ns/test-ack-" + System.currentTimeMillis();
        @Cleanup final var consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .isAckReceiptEnabled(true)
                .subscribe();
        produceMessages(topic);
        final int ackIndex = 3;
        NonBatchedMessageId messageIdToAck = null;
        for (int i = 0; i < 10; i++) {
            var msg = consumer.receive();
            var msgId = (PulsarApiMessageId) msg.getMessageId();
            if (i == ackIndex) {
                messageIdToAck = new NonBatchedMessageId(msgId.getLedgerId(), msgId.getEntryId());
            }
        }
        assertNotNull(messageIdToAck);
        consumer.acknowledgeCumulative(messageIdToAck);
        consumer.redeliverUnacknowledgedMessages();
        var msg = consumer.receive(3, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals(msg.getValue(), "msg-" + (ackIndex + 1));
    }

    private List<NonBatchedMessageId> produceMessages(String topic) throws PulsarClientException {
        @Cleanup final var producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(false)
                .create();
        final var msgIdList = new ArrayList<NonBatchedMessageId>();
        for (int i = 0; i < 10; i++) {
            final var msgId = (PulsarApiMessageId) producer.send("msg-" + i);
            msgIdList.add(new NonBatchedMessageId(msgId.getLedgerId(), msgId.getEntryId()));
        }
        return msgIdList;
    }

    @AllArgsConstructor
    private static class NonBatchedMessageId implements PulsarApiMessageId {
        // For non-batched message id in a single topic, only ledger id and entry id are required

        private final long ledgerId;
        private final long entryId;

        @Override
        public byte[] toByteArray() {
            return new byte[0]; // dummy implementation
        }

        @Override
        public long getLedgerId() {
            return ledgerId;
        }

        @Override
        public long getEntryId() {
            return entryId;
        }
    }
}

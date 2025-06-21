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
package org.apache.pulsar.compaction;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class CustomCompactionServiceTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setBrokerServiceCompactionMonitorIntervalInSeconds(Integer.MAX_VALUE);
        conf.setRetentionCheckIntervalInSeconds(Integer.MAX_VALUE);
        conf.setCompactionServiceFactoryClassName(CustomCompactionServiceFactory.class.getName());
    }

    @Test
    public void testGetLastMessageIdAfterCompaction() throws Exception {
        final var topic = BrokerTestUtil.newUniqueName("tp");
        @Cleanup final var producer = pulsarClient.newProducer(Schema.STRING).topic(topic)
                .batchingMaxBytes(Integer.MAX_VALUE)
                .batchingMaxMessages(Integer.MAX_VALUE)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        producer.newMessage().key("k0").value("v0").sendAsync();
        producer.newMessage().key("k0").value("v1").sendAsync();
        producer.newMessage().key("k1").value("v0").sendAsync();
        producer.newMessage().key("k1").value(null).sendAsync();
        producer.flush();

        triggerCompactionAndWait(topic);

        @Cleanup final var reader = pulsarClient.newReader(Schema.STRING).topic(topic)
                .startMessageId(MessageId.earliest).readCompacted(true)
                .messagePayloadProcessor(new CustomCompactionServiceFactory.PayloadProcessor())
                .payloadToMessageIdConverter(CustomCompactionServiceFactory::convertPayloadToMessageId)
                .create();
        final var messages = new ArrayList<Message<String>>();
        while (reader.hasMessageAvailable()) {
            final var msg = reader.readNext(3, TimeUnit.SECONDS);
            assertNotNull(msg);
            messages.add(msg);
            log.info("Read {} => {} {{}}", msg.getKey(), msg.getValue(), msg.getMessageId());
        }
        Assert.assertEquals(messages.size(), 1);
        final var msg = messages.get(0);
        Assert.assertEquals(msg.getKey(), "k0");
        Assert.assertEquals(msg.getValue(), "v1");
        Assert.assertEquals(((MessageIdAdv) msg.getMessageId()).getBatchIndex(), 1);
        final var lastMsgId = reader.getLastMessageIds().get(0);
        log.info("Last msg id: {}", lastMsgId);
        Assert.assertEquals(msg.getMessageId(), lastMsgId);
    }

    private void triggerCompactionAndWait(String topicName) throws Exception {
        final var persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).get().get();
        persistentTopic.triggerCompaction();
        Awaitility.await().untilAsserted(() -> {
            Position lastConfirmPos = persistentTopic.getManagedLedger().getLastConfirmedEntry();
            Position markDeletePos = persistentTopic
                    .getSubscription(Compactor.COMPACTION_SUBSCRIPTION).getCursor().getMarkDeletedPosition();
            assertEquals(markDeletePos.getLedgerId(), lastConfirmPos.getLedgerId());
            assertEquals(markDeletePos.getEntryId(), lastConfirmPos.getEntryId());
        });
    }
}

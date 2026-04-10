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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class SimpleProducerConsumerMLInitializeDelayTest extends SharedPulsarBaseTest {

    @Test(timeOut = 30 * 1000)
    public void testConcurrentlyOfPublishAndSwitchLedger() throws Exception {
        final String topicName = newTopicName();
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);
        // Make ledger switches faster.
        PersistentTopic persistentTopic =
                (PersistentTopic) getTopic(topicName, false).join().get();
        ManagedLedgerConfig config = persistentTopic.getManagedLedger().getConfig();
        config.setMaxEntriesPerLedger(2);
        config.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).enableBatching(false)
                .create();
        List<CompletableFuture<MessageId>> sendRequests = new ArrayList<>();
        List<String> msgsSent = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String msg = i + "";
            sendRequests.add(producer.sendAsync(i + ""));
            msgsSent.add(msg);
        }
        // Verify:
        // - All messages were sent.
        // - The order of messages are correct.
        Set<String> msgIds = new LinkedHashSet<>();
        MessageIdImpl previousMsgId = null;
        for (CompletableFuture<MessageId> msgId : sendRequests) {
            Assert.assertNotNull(msgId.join());
            MessageIdImpl messageIdImpl = (MessageIdImpl) msgId.join();
            if (previousMsgId != null) {
                Assert.assertTrue(messageIdImpl.compareTo(previousMsgId) > 0);
            }
            msgIds.add(String.format("%s:%s", messageIdImpl.getLedgerId(), messageIdImpl.getEntryId()));
            previousMsgId = messageIdImpl;
        }
        Assert.assertEquals(msgIds.size(), 100);
        log.info("messages were sent: {}", msgIds.toString());
        List<String> msgsReceived = new ArrayList<>();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionName(subscription).subscribe();
        while (true) {
            Message<String> receivedMsg = consumer.receive(2, TimeUnit.SECONDS);
            if (receivedMsg == null) {
                break;
            }
            msgsReceived.add(receivedMsg.getValue());
        }
        Assert.assertEquals(msgsReceived, msgsSent);

        // cleanup.
        consumer.close();
        producer.close();
    }
}

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
package org.apache.pulsar.broker.service;


import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MessageTTLTest extends BrokerTestBase {

    private static final Logger log = LoggerFactory.getLogger(MessageTTLTest.class);
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setTtlDurationDefaultInSeconds(1);
        this.conf.setBrokerDeleteInactiveTopicsEnabled(false);
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMessageExpiryAfterTopicUnload() throws Exception {
        int numMsgs = 50;
        final String topicName = "persistent://prop/ns-abc/testttl";
        final String subscriptionName = "ttl-sub-1";

        pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscribe()
                .close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false) // this makes the test easier and predictable
                .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
        }
        FutureUtil.waitForAll(sendFutureList).get();
        producer.close();

        // unload a reload the topic
        // this action created a new ledger
        // having a managed ledger with more than one
        // ledger should not impact message expiration
        admin.topics().unload(topicName);
        admin.topics().getStats(topicName);

        AbstractTopic topic = (AbstractTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        // wall clock time, we have to make the message to be considered "expired"
        Thread.sleep(this.conf.getTtlDurationDefaultInSeconds() * 2000);
        log.info("***** run message expiry now");
        this.runMessageExpiryCheck();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscribe();
        Message<byte[]> msg = consumer.receive(10, java.util.concurrent.TimeUnit.SECONDS);
        assertNull(msg);
        consumer.close();
    }


    @Test
    public void testStandardMessageExpiryWithPrefetch() throws Exception {
        int numMsgs = 50;
        final String topicName = "persistent://prop/ns-abc/testttl";
        final String subscriptionName = "ttl-sub-2";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(1) // this makes the test predictable
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false) // this makes the test easier and predictable
                .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
        }
        FutureUtil.waitForAll(sendFutureList).get();
        producer.close();

        Message<byte[]> msg = consumer.receive(10, java.util.concurrent.TimeUnit.SECONDS);
        assertNotNull(msg);
        consumer.acknowledge(msg);

        AbstractTopic topic = (AbstractTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
         // wall clock time, we have to make the message to be considered "expired"
        Thread.sleep(this.conf.getTtlDurationDefaultInSeconds() * 2000);
        this.runMessageExpiryCheck();

        Message<byte[]> msg2 = consumer.receive(1, java.util.concurrent.TimeUnit.SECONDS);
        // the consumer prefetched a message
        assertNotNull(msg);
        consumer.acknowledge(msg2);
        // all messages expired, so we expect to see a null here
        Message<byte[]> msg3 = consumer.receive(1, java.util.concurrent.TimeUnit.SECONDS);
        assertNull(msg3);

        consumer.close();
    }

}

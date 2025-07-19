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
package org.apache.pulsar.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

@Slf4j
public class PublishWithMLPayloadProcessorTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setBrokerEntryPayloadProcessors(
                Collections.singleton(ManagedLedgerPayloadProcessor0.class.getName()));
        super.internalSetup();
        super.producerBaseSetup();
        // Add delay is for testing an OpAddEntry can be finished before the previous one.
        pulsarTestContext.getMockBookKeeper().addEntryDelay(500, TimeUnit.MILLISECONDS);
    }

    @BeforeClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


//    @Test(timeOut = 30_000)
    public void testPublishWithoutDeduplication() throws Exception {
        String topic = "persistent://public/default/testPublishWithoutDeduplication";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topicPolicies().setDeduplicationStatus(topic, false);
        publishAndVerify(topic, false);
    }

//    @Test(timeOut = 30_000)
    public void testPublishWithDeduplication() throws Exception {
        String topic = "persistent://public/default/testPublishWithDeduplication";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topicPolicies().setDeduplicationStatus(topic, true);
        publishAndVerify(topic, true);
    }


    private void publishAndVerify(String topic, boolean enableDeduplication) throws Exception {
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).enableBatching(false).create();

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            producer.sendAsync("message-" + i).whenComplete((ignored, e) -> {
                if (e != null) {
                    log.error("Failed to publish message", e);
                }
                latch.countDown();
            });
        }

        latch.await();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic).subscriptionName("my-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        List<String> messageContents = new ArrayList<>();

        int messageCount = 0;
        for (;;) {
            var msg = consumer.receive(3, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            String content = msg.getValue();
            messageCount++;
            if (!messageContents.contains(content)) {
                messageContents.add(content);
            }
            consumer.acknowledge(msg);
        }

        if (enableDeduplication) {
            Assert.assertEquals(messageCount, 10, "Expected 10 total messages");
        }
        Assert.assertEquals(messageContents.size(), 10);

        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(messageContents.get(i), "message-" + i);
        }
    }
}

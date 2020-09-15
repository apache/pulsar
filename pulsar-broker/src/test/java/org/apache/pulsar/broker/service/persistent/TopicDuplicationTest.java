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
package org.apache.pulsar.broker.service.persistent;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TopicDuplicationTest extends ProducerConsumerBase {
    private final String testTenant = "my-property";
    private final String testNamespace = "my-ns";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/max-unacked-";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(true);
        this.conf.setBrokerDeduplicationEnabled(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 10000)
    public void testDuplicationApi() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        waitCacheInit(topicName);
        admin.topics().createPartitionedTopic(topicName, 3);
        Boolean enabled = admin.topics().getDeduplicationEnabled(topicName);
        assertNull(enabled);

        admin.topics().enableDeduplication(topicName, true);
        for (int i = 0; i < 50; i++) {
            if (admin.topics().getMaxUnackedMessagesOnSubscription(topicName) != null) {
                break;
            }
            Thread.sleep(100);
        }
        Assert.assertEquals(admin.topics().getDeduplicationEnabled(topicName), true);
        admin.topics().disableDeduplication(topicName);
        for (int i = 0; i < 50; i++) {
            if (admin.topics().getDeduplicationEnabled(topicName) == null) {
                break;
            }
            Thread.sleep(100);
        }
        assertNull(admin.topics().getDeduplicationEnabled(topicName));
    }

    @Test(timeOut = 20000)
    public void testDuplicationMethod() throws Exception {
        final String topicName = testTopic + UUID.randomUUID().toString();
        final String producerName = "my-producer";
        final int maxMsgNum = 100;
        waitCacheInit(topicName);
        admin.topics().createPartitionedTopic(testTopic, 3);
        //1) Start up producer and send msg.We specified the max sequenceId
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .producerName(producerName).create();
        long seq = System.currentTimeMillis();
        for (int i = 0; i <= maxMsgNum; i++) {
            producer.newMessage().value("msg-" + i).sequenceId(seq + i).send();
        }
        long maxSeq = seq + maxMsgNum;
        //2) Max sequenceId should be recorded correctly
        CompletableFuture<Optional<Topic>> completableFuture = pulsar.getBrokerService().getTopics().get(topicName);
        Topic topic = completableFuture.get(1, TimeUnit.SECONDS).get();
        PersistentTopic persistentTopic = (PersistentTopic) topic;
        MessageDeduplication messageDeduplication = persistentTopic.getMessageDeduplication();
        messageDeduplication.checkStatus().whenComplete((res, ex) -> {
            if (ex != null) {
                fail("should not fail");
            }
            assertNotNull(messageDeduplication.highestSequencedPersisted);
            assertNotNull(messageDeduplication.highestSequencedPushed);
            long seqId = messageDeduplication.getLastPublishedSequenceId(producerName);
            assertEquals(seqId, maxSeq);
            assertEquals(messageDeduplication.highestSequencedPersisted.get(producerName).longValue(), maxSeq);
            assertEquals(messageDeduplication.highestSequencedPushed.get(producerName).longValue(), maxSeq);
        }).get();
        //3) disable the deduplication check
        admin.topics().enableDeduplication(topicName, false);
        for (int i = 0; i < 50; i++) {
            if (admin.topics().getDeduplicationEnabled(topicName) != null) {
                break;
            }
            Thread.sleep(100);
        }
        for (int i = 0; i < 100; i++) {
            producer.newMessage().value("msg-" + i).sequenceId(maxSeq + i).send();
        }
        //4) Max sequenceId record should be clear
        messageDeduplication.checkStatus().whenComplete((res, ex) -> {
            if (ex != null) {
                fail("should not fail");
            }
            assertEquals(messageDeduplication.getLastPublishedSequenceId(producerName), -1);
            assertEquals(messageDeduplication.highestSequencedPersisted.size(), 0);
            assertEquals(messageDeduplication.highestSequencedPushed.size(), 0);
        }).get();

    }

    private void waitCacheInit(String topicName) throws Exception {
        for (int i = 0; i < 50; i++) {
            //wait for server init
            Thread.sleep(3000);
            try {
                admin.topics().getDeduplicationEnabled(topicName);
                break;
            } catch (Exception e) {
                //ignore
            }
            if (i == 49) {
                throw new RuntimeException("Waiting for cache initialization has timed out");
            }
        }
    }

}

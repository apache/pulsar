/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.api;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.service.BrokerTestBase;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.client.impl.PartitionedBrokerConsumerStatsImpl;

public class BrokerConsumerStatsTest extends BrokerTestBase {
    private static final Logger log = LoggerFactory.getLogger(BrokerConsumerStatsTest.class);
    private String topicName = "persistent://prop/cluster/ns/topic-";

    @BeforeClass
    @Override
    public void setup() throws Exception {
        baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test()
    public void testSharedSubscriptionMessageBacklog() throws PulsarClientException {
        int totalMessages = 50;
        String topicNamePostFix = "testSharedSubscriptionMessageBacklog";
        
        // 1. Create consumers
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer1 = pulsarClient.subscribe(topicName + topicNamePostFix, "my-subscriber-name", conf);
        Consumer consumer2 = pulsarClient.subscribe(topicName + topicNamePostFix, "my-subscriber-name", conf);
        Consumer consumer3 = pulsarClient.subscribe(topicName + topicNamePostFix, "my-subscriber-name", conf);

        // 2. Create producers and produce messages
        Producer producer = pulsarClient.createProducer(topicName + topicNamePostFix, new ProducerConfiguration());
        for (int i = 0; i < totalMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        int count = 0;
        try {
            // 3. Consumer all messages routed to consumer1
            Message msg = null;
            while (true) {
                msg = consumer1.receive(10, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    consumer1.acknowledge(msg);
                    count++;
                } else {
                    break;
                }
            }

            // 4. Check subscription type
            log.debug(consumer1.getBrokerConsumerStatsAsync().get().toString());
            Assert.assertEquals(consumer1.getBrokerConsumerStatsAsync().get().getSubscriptionType(),
                    SubscriptionType.Shared);
            Assert.assertEquals(consumer1.getBrokerConsumerStatsAsync().get().getMsgBacklog(), totalMessages - count);

            // 5. Consumer all messages routed to consumer2
            while (true) {
                msg = consumer2.receive(10, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    consumer2.acknowledge(msg);
                    count++;
                } else {
                    break;
                }
            }

            // 6. Check consumer2 backlog
            log.debug(consumer2.getBrokerConsumerStatsAsync().get().toString());
            Assert.assertEquals(consumer2.getBrokerConsumerStatsAsync().get().getMsgBacklog(), totalMessages - count);

            // 7. Consume all messages routed to consumer2
            while (true) {
                msg = consumer3.receive(10, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    consumer3.acknowledge(msg);
                    count++;
                } else {
                    break;
                }
            }

            // 6. Check consumer3 backlog
            log.debug(consumer3.getBrokerConsumerStatsAsync().get().toString());
            Assert.assertEquals(consumer3.getBrokerConsumerStatsAsync().get().getMsgBacklog(), 0);
        } catch (Exception ex) {
            Assert.fail("Exception:" + ex);
        } finally {
            consumer1.close();
            consumer2.close();
            consumer3.close();
        }
    }

    @Test
    public void testCachingMechanism() throws PulsarClientException {
        int totalMessages = 50;
        String topicNamePostFix = "testCachingMechanism";

        // 1. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setBrokerConsumerStatsCacheTime(3, TimeUnit.SECONDS);
        Consumer consumer = pulsarClient.subscribe(topicName + topicNamePostFix, "my-subscriber-name", conf);

        // 2. Create producer and produce messages
        Producer producer = pulsarClient.createProducer(topicName + topicNamePostFix, new ProducerConfiguration());
        for (int i = 0; i < totalMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        try {
            // 3. Get stats and validate
            BrokerConsumerStats stats = consumer.getBrokerConsumerStatsAsync().get();
            Assert.assertEquals(consumer.getBrokerConsumerStatsAsync().get().getSubscriptionType(),
                    SubscriptionType.Exclusive);
            Assert.assertEquals(consumer.getBrokerConsumerStatsAsync().get().getMsgBacklog(), totalMessages);
            Assert.assertEquals(stats, consumer.getBrokerConsumerStatsAsync().get());
            Assert.assertTrue(stats.isValid());
            
            // 4. Consume all messages
            Message msg = null;
            while (true) {
                msg = consumer.receive(10, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    consumer.acknowledge(msg);
                } else {
                    break;
                }
            }

            Thread.sleep(2 * 1000);
            // 5. Cached results returned
            Assert.assertEquals(consumer.getBrokerConsumerStatsAsync().get().getMsgBacklog(), totalMessages);
            Assert.assertTrue(consumer.getBrokerConsumerStatsAsync().get().isValid());
            Assert.assertEquals(stats, consumer.getBrokerConsumerStatsAsync().get());
            Assert.assertTrue(stats.isValid());

            // 6. Waiting for cache time to expire
            Thread.sleep(2 * 1000);
            Assert.assertEquals(consumer.getBrokerConsumerStatsAsync().get().getMsgBacklog(), 0);
            Assert.assertNotEquals(stats, consumer.getBrokerConsumerStatsAsync().get());
            Assert.assertFalse(stats.isValid());
        } catch (Exception ex) {
            Assert.fail("Exception:" + ex);
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testPartitionedTopicsStats() throws PulsarClientException, PulsarAdminException {
        int totalMessages = 60;
        String topicNamePostFix = "testPartitionedTopicsStats";
        int numberOfPartitions = 3;
        String topicName = this.topicName + topicNamePostFix;
        // 1. Create partitioned topic
        admin.persistentTopics().createPartitionedTopic(topicName, numberOfPartitions);
        
        // 2. Create consumer
        Consumer consumer = pulsarClient.subscribe(topicName, "my-subscription");
       
        // 3. Create producer and produce messages
        ProducerConfiguration conf = new ProducerConfiguration();
        conf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = pulsarClient.createProducer(topicName, conf);
        for (int i = 0; i < totalMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // 4. Get stats 
        try {
            BrokerConsumerStats stats = consumer.getBrokerConsumerStatsAsync().get();
            Assert.assertEquals(stats.getSubscriptionType(), SubscriptionType.Exclusive);
            Assert.assertEquals(stats.getMsgBacklog(), totalMessages);
            
            // 5. check if stats are instance of PartitionedBrokerConsumerStatsImpl
            Assert.assertTrue(stats instanceof PartitionedBrokerConsumerStatsImpl);
            Assert.assertEquals(((PartitionedBrokerConsumerStatsImpl) stats).get(0).getMsgBacklog(), totalMessages / 3);
            Assert.assertEquals(((PartitionedBrokerConsumerStatsImpl) stats).get(1).getMsgBacklog(), totalMessages / 3);
            Assert.assertEquals(((PartitionedBrokerConsumerStatsImpl) stats).get(2).getMsgBacklog(), totalMessages / 3);

            Thread.sleep(2 * 1000);
            Assert.assertTrue(consumer.getBrokerConsumerStatsAsync().get().isValid());
        } catch (Exception ex) {
            Assert.fail("Exception:" + ex);
        } finally {
            consumer.close();
        }
    }
}

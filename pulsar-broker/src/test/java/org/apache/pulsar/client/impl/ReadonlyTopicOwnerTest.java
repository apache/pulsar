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

import com.google.common.collect.Sets;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ReadonlyTopicOwnerTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("readonly-topic-owner-test",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("readonly-topic-owner-test/writable-ns");

        Policies policies = new Policies();
        policies.shadowReaderModeEnabled = true;
        policies.writerNamespace = "readonly-topic-owner-test/writable-ns";
        admin.namespaces().createNamespace("readonly-topic-owner-test/readonly-ns", policies);

        log.info("ReadonlyTopicOwnerTest.setup finished.");
    }

    @Test(timeOut = 10000)
    public void testReadonlyTopic() throws Exception {
        String writerTopic = "persistent://readonly-topic-owner-test/writable-ns/testGetCursor";
        String readerTopic = "persistent://readonly-topic-owner-test/readonly-ns/testGetCursor";
        String subscription = "sub-testReadonlyTopic";


        Producer<byte[]> producer = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(writerTopic) // send msg to writer topic
                .createAsync()
                .get();
        MessageIdImpl sendId = (MessageIdImpl) producer.send("data1".getBytes());
        log.info("send msg.id = {}", sendId);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionName(subscription)
                .topic(readerTopic) // sub from reader topic
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(0)
                .subscribeAsync()
                .get();

        Message<byte[]> msg = consumer.receive();
        log.info("receive msg.id = {}", msg.getMessageId());
        Assert.assertEquals(msg.getMessageId(), sendId);

        consumer.close();
        producer.close();

    }

    @Test
    public void testAutoCreateTopicFromReader() throws Exception {
        String writerTopic = "persistent://readonly-topic-owner-test/writable-ns/testAutoCreateTopicFromReader";
        String readerTopic = "persistent://readonly-topic-owner-test/readonly-ns/testAutoCreateTopicFromReader";
        String subscription = "sub-testAutoCreateTopicFromReader";

        log.info("testAutoCreateTopicFromReader consumer started.");
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionName(subscription)
                .topic(readerTopic) // sub from reader topic
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(0)
                .subscribeAsync()
                .get();

        log.info("testAutoCreateTopicFromReader producer started.");
        Producer<byte[]> producer = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(writerTopic) // send msg to writer topic
                .createAsync()
                .get();

        log.info("testAutoCreateTopicFromReader producer.send started.");
        MessageIdImpl sendId = (MessageIdImpl) producer.send("data1".getBytes());
        log.info("send msg.id = {}", sendId);


        log.info("testAutoCreateTopicFromReader consumer.receive started.");
        Message<byte[]> msg = consumer.receive();
        log.info("receive msg.id = {}", msg.getMessageId());
        Assert.assertEquals(msg.getMessageId(), sendId);

        consumer.close();
        producer.close();

    }

    @Test(timeOut = 10000)
    public void testConsumerRelayFromWriter() throws Exception {
        String writerTopic = "persistent://readonly-topic-owner-test/writable-ns/testConsumerRelayFromWriter";
        String readerTopic = "persistent://readonly-topic-owner-test/readonly-ns/testConsumerRelayFromWriter";
        String subscription = "sub-testConsumerRelayFromWriter";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionName(subscription)
                .topic(writerTopic) // sub from writer topic
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(0)
                .subscribeAsync()
                .get();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(writerTopic) // send msg to writer topic
                .createAsync()
                .get();

        MessageIdImpl sendId = (MessageIdImpl) producer.send("data1".getBytes());
        log.info("send msg.id = {}", sendId);


        Message<byte[]> msg = consumer.receive();
        log.info("receive msg from writer. id = {}", msg.getMessageId());
        Assert.assertEquals(msg.getMessageId(), sendId);
        consumer.acknowledge(msg);

        sendId = (MessageIdImpl) producer.send("data1".getBytes());
        log.info("send msg.id = {}", sendId);
        consumer.close();
        producer.close();


        //this consumer will relay the consume progress from writer topic.
        consumer = pulsarClient.newConsumer()
                .subscriptionName(subscription)
                .topic(readerTopic) // sub from reader topic
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(0)
                .subscribeAsync()
                .get();
        msg = consumer.receive();
        log.info("receive msg from reader. id = {}", msg.getMessageId());
        Assert.assertEquals(msg.getMessageId(), sendId);
    }


    @BeforeClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }
}

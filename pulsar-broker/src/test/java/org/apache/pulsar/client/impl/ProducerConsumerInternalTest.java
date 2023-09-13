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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import java.util.concurrent.CountDownLatch;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Different with {@link org.apache.pulsar.client.api.SimpleProducerConsumerTest}, this class can visit the variables
 * of {@link ConsumerImpl} which are modified `protected`.
 */
@Test(groups = "broker-api")
public class ProducerConsumerInternalTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
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

    @Test
    public void testSameProducerRegisterTwice() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
        admin.topics().createNonPartitionedTopic(topicName);

        // Create producer using default producerName.
        ProducerImpl producer = (ProducerImpl) pulsarClient.newProducer().topic(topicName).create();
        ServiceProducer serviceProducer = getServiceProducer(producer, topicName);

        // Remove producer maintained by server cnx. To make it can register the second time.
        removeServiceProducerMaintainedByServerCnx(serviceProducer);

        // Trigger the client producer reconnect.
        CommandCloseProducer commandCloseProducer = new CommandCloseProducer();
        commandCloseProducer.setProducerId(producer.producerId);
        producer.getClientCnx().handleCloseProducer(commandCloseProducer);

        // Verify the reconnection will be success.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getState().toString(), "Ready");
        });
    }

    @Test
    public void testSameProducerRegisterTwiceWithSpecifiedProducerName() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
        final String pName = "p1";
        admin.topics().createNonPartitionedTopic(topicName);

        // Create producer using default producerName.
        ProducerImpl producer = (ProducerImpl) pulsarClient.newProducer().producerName(pName).topic(topicName).create();
        ServiceProducer serviceProducer = getServiceProducer(producer, topicName);

        // Remove producer maintained by server cnx. To make it can register the second time.
        removeServiceProducerMaintainedByServerCnx(serviceProducer);

        // Trigger the client producer reconnect.
        CommandCloseProducer commandCloseProducer = new CommandCloseProducer();
        commandCloseProducer.setProducerId(producer.producerId);
        producer.getClientCnx().handleCloseProducer(commandCloseProducer);

        // Verify the reconnection will be success.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getState().toString(), "Ready", "The producer registration failed");
        });
    }

    private void removeServiceProducerMaintainedByServerCnx(ServiceProducer serviceProducer) {
        ServerCnx serverCnx = (ServerCnx) serviceProducer.getServiceProducer().getCnx();
        serverCnx.removedProducer(serviceProducer.getServiceProducer());
        Awaitility.await().untilAsserted(() -> {
            assertFalse(serverCnx.getProducers().containsKey(serviceProducer.getServiceProducer().getProducerId()));
        });
    }

    @Test
    public void testExclusiveConsumerWillAlwaysRetryEvenIfReceivedConsumerBusyError() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
        final String subscriptionName = "subscription1";
        admin.topics().createNonPartitionedTopic(topicName);

        final ConsumerImpl consumer = (ConsumerImpl) pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionType(SubscriptionType.Exclusive).subscriptionName(subscriptionName).subscribe();

        ClientCnx clientCnx = consumer.getClientCnx();
        ServerCnx serverCnx = (ServerCnx) pulsar.getBrokerService()
                .getTopic(topicName,false).join().get().getSubscription(subscriptionName)
                .getDispatcher().getConsumers().get(0).cnx();

        // Make a disconnect to trigger broker remove the consumer which related this connection.
        // Make the second subscribe runs after the broker removing the old consumer, then it will receive
        // an error: "Exclusive consumer is already connected"
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        serverCnx.execute(() -> {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        clientCnx.close();
        Thread.sleep(1000);
        countDownLatch.countDown();

        // Verify the consumer will always retry subscribe event received ConsumerBusy error.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(consumer.getState(), HandlerState.State.Ready);
        });

        // cleanup.
        consumer.close();
        admin.topics().delete(topicName, false);
    }
}

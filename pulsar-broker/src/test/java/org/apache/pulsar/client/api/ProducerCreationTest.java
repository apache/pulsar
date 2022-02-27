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
package org.apache.pulsar.client.api;

import static org.testng.Assert.fail;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ProducerCreationTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "topicDomainProvider")
    public Object[][] topicDomainProvider() {
        return new Object[][] {
                { TopicDomain.persistent },
                { TopicDomain.non_persistent }
        };
    }

    @Test(dataProvider = "topicDomainProvider")
    public void testExactlyOnceWithProducerNameSpecified(TopicDomain domain) throws PulsarClientException {
        Producer<byte[]> producer1 = pulsarClient.newProducer()
                .topic(TopicName.get(domain.value(), "public", "default", "testExactlyOnceWithProducerNameSpecified").toString())
                .producerName("p-name-1")
                .create();

        Assert.assertNotNull(producer1);

        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic("testExactlyOnceWithProducerNameSpecified")
                .producerName("p-name-2")
                .create();

        Assert.assertNotNull(producer2);

        try {
            pulsarClient.newProducer()
                    .topic("testExactlyOnceWithProducerNameSpecified")
                    .producerName("p-name-2")
                    .create();
            Assert.fail("should be failed");
        } catch (PulsarClientException.ProducerBusyException e) {
            //ok here
        }
    }

    @Test(dataProvider = "topicDomainProvider")
    public void testGeneratedNameProducerReconnect(TopicDomain domain) throws PulsarClientException, InterruptedException {
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(TopicName.get(domain.value(), "public", "default", "testGeneratedNameProducerReconnect").toString())
                .create();
        Assert.assertTrue(producer.isConnected());
        //simulate create producer timeout.
        Thread.sleep(3000);

        producer.getConnectionHandler().connectionClosed(producer.getConnectionHandler().cnx());
        Assert.assertFalse(producer.isConnected());
        Thread.sleep(3000);
        Assert.assertEquals(producer.getConnectionHandler().getEpoch(), 1);
        Assert.assertTrue(producer.isConnected());
    }

    @Test(dataProvider = "topicDomainProvider")
    public void testInitialSubscriptionCreation(TopicDomain domain) throws PulsarClientException, PulsarAdminException {
        final String initialSubscriptionName = "init-sub";
        final TopicName topic = TopicName.get(domain.value(), "public", "default", "testInitialSubscriptionCreation");

        // Should not create initial subscription when the initialSubscriptionName is null or empty
        Producer<byte[]> nullInitSubProducer = ((ProducerBuilderImpl<byte[]>) pulsarClient.newProducer())
                .initialSubscriptionName(null)
                .topic(topic.toString())
                .create();
        nullInitSubProducer.close();
        Assert.assertFalse(admin.topics().getSubscriptions(topic.toString()).contains(initialSubscriptionName));

        Producer<byte[]> emptyInitSubProducer = ((ProducerBuilderImpl<byte[]>) pulsarClient.newProducer())
                .initialSubscriptionName("")
                .topic(topic.toString())
                .create();
        emptyInitSubProducer.close();
        Assert.assertFalse(admin.topics().getSubscriptions(topic.toString()).contains(initialSubscriptionName));

        Producer<byte[]> producer = ((ProducerBuilderImpl<byte[]>) pulsarClient.newProducer())
                .initialSubscriptionName(initialSubscriptionName)
                .topic(topic.toString())
                .create();
        producer.close();

        // Initial subscription will only be created if the topic is persistent
        Assert.assertEquals(topic.isPersistent(),
                admin.topics().getSubscriptions(topic.toString()).contains(initialSubscriptionName));

        // Existing subscription should not fail the producer creation.
        Producer<byte[]> otherProducer = ((ProducerBuilderImpl<byte[]>) pulsarClient.newProducer())
                .initialSubscriptionName(initialSubscriptionName)
                .topic(topic.toString())
                .create();
        otherProducer.close();

        Assert.assertEquals(topic.isPersistent(),
                admin.topics().getSubscriptions(topic.toString()).contains(initialSubscriptionName));
    }

    @Test
    public void testCreateInitialSubscriptionOnPartitionedTopic() throws PulsarAdminException, PulsarClientException {
        final TopicName topic =
                TopicName.get("persistent", "public", "default", "testCreateInitialSubscriptionOnPartitionedTopic");
        final String initialSubscriptionName = "init-sub";
        admin.topics().createPartitionedTopic(topic.toString(), 10);
        Producer<byte[]> producer = ((ProducerBuilderImpl<byte[]>) pulsarClient.newProducer())
                .initialSubscriptionName(initialSubscriptionName)
                .topic(topic.toString())
                .create();
        producer.close();

        Assert.assertTrue(admin.topics().getSubscriptions(topic.toString()).contains(initialSubscriptionName));
    }

    @Test
    public void testCreateInitialSubscriptionWhenExisting() throws PulsarClientException, PulsarAdminException {
        final TopicName topic =
                TopicName.get("persistent", "public", "default", "testCreateInitialSubscriptionWhenExisting");
        final String initialSubscriptionName = "init-sub";
        admin.topics().createNonPartitionedTopic(topic.toString());
        admin.topics().createSubscription(topic.toString(), initialSubscriptionName, MessageId.earliest);
        Producer<byte[]> producer = ((ProducerBuilderImpl<byte[]>) pulsarClient.newProducer())
                .initialSubscriptionName(initialSubscriptionName)
                .topic(topic.toString())
                .create();
        producer.close();

        Assert.assertTrue(admin.topics().getSubscriptions(topic.toString()).contains(initialSubscriptionName));
    }

    @Test
    public void testInitialSubscriptionCreationWithAutoCreationDisable()
            throws PulsarAdminException, PulsarClientException {
        pulsar.getConfiguration().setAllowAutoSubscriptionCreation(false);

        final TopicName topic =
                TopicName.get("persistent", "public", "default",
                        "testInitialSubscriptionCreationWithAutoCreationDisable");
        final String initialSubscriptionName = "init-sub";
        admin.topics().createNonPartitionedTopic(topic.toString());
        try {
            Producer<byte[]> producer = ((ProducerBuilderImpl<byte[]>) pulsarClient.newProducer())
                    .initialSubscriptionName(initialSubscriptionName)
                    .topic(topic.toString())
                    .create();
            fail("Should not pass");
        } catch (PulsarClientException.NotAllowedException exception) {
            // ok
        }

        Assert.assertFalse(admin.topics().getSubscriptions(topic.toString()).contains(initialSubscriptionName));
    }
}

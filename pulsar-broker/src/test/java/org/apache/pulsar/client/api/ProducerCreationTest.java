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

import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ProducerCreationTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
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

        producer.getConnectionHandler().connectionClosed(producer.getConnectionHandler().getClientCnx());
        Assert.assertFalse(producer.isConnected());
        Thread.sleep(3000);
        Assert.assertEquals(producer.getConnectionHandler().getEpoch(), 1);
        Assert.assertTrue(producer.isConnected());
    }

    @Test(dataProvider = "topicDomainProvider")
    public void testParallelProducersGeneratedNameFails(TopicDomain domain) {
        try {
            pulsarClient.newProducer()
                    .topic("testParallelProducersGeneratedNameFails")
                    .groupMode(ProducerGroupMode.Parallel)
                    .create();
            Assert.fail("previous statement should have failed");
        } catch (PulsarClientException e) {
            // ok here
            String msg = e.getMessage();
            Assert.assertTrue(msg.endsWith("producerName must be specified in non-exclusive group modes"), msg);
        }
    }

    @Test(dataProvider = "topicDomainProvider")
    public void testParallelProducers(TopicDomain domain) throws PulsarClientException {
        Producer<byte[]> producer1 = pulsarClient.newProducer()
                .topic(TopicName.get(domain.value(), "public", "default", "testParallelProducers").toString())
                .producerName("p-name-1")
                .groupMode(ProducerGroupMode.Parallel)
                .create();

        Assert.assertNotNull(producer1);

        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic("testParallelProducers")
                .producerName("p-name-1")
                .groupMode(ProducerGroupMode.Parallel)
                .create();

        Assert.assertNotNull(producer2);
    }

    @Test(dataProvider = "topicDomainProvider")
    public void testParallelProducerCannotJoinExclusiveGroup(TopicDomain domain) throws PulsarClientException {
        Producer<byte[]> producer1 = pulsarClient.newProducer()
                .topic(TopicName.get(domain.value(), "public", "default", "testParallelProducerCannotJoinExclusiveGroup").toString())
                .producerName("p-name-1")
                .create();

        Assert.assertNotNull(producer1);

        try {
            pulsarClient.newProducer()
                    .topic(TopicName.get(domain.value(), "public", "default", "testParallelProducerCannotJoinExclusiveGroup").toString())
                    .producerName("p-name-1")
                    .groupMode(ProducerGroupMode.Parallel)
                    .create();
            Assert.fail("previous statement should have failed");
        } catch (PulsarClientException e) {
            // ok here
            String msg = e.getMessage();
            Assert.assertTrue(msg.endsWith("Exclusive Producer with name 'p-name-1' is already connected to topic"), msg);
        }
    }

    @Test(dataProvider = "topicDomainProvider")
    public void testExclusiveProducerEvictsParallelGroup(TopicDomain domain) throws PulsarClientException {
        Producer<byte[]> producer1 = pulsarClient.newProducer()
                .topic(TopicName.get(domain.value(), "public", "default", "testParallelProducerCannotJoinExclusiveGroup").toString())
                .producerName("p-name-1")
                .groupMode(ProducerGroupMode.Parallel)
                .create();

        Assert.assertNotNull(producer1);

        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic(TopicName.get(domain.value(), "public", "default", "testParallelProducerCannotJoinExclusiveGroup").toString())
                .producerName("p-name-1")
                .groupMode(ProducerGroupMode.Parallel)
                .create();

        Assert.assertNotNull(producer2);

        Assert.assertTrue(producer1.isConnected());
        Assert.assertTrue(producer2.isConnected());

        Producer<byte[]> producer3 = pulsarClient.newProducer()
                .topic(TopicName.get(domain.value(), "public", "default", "testParallelProducerCannotJoinExclusiveGroup").toString())
                .producerName("p-name-1")
                .create();

        Assert.assertNotNull(producer3);

        producer3.send("my-message".getBytes());
    }

}

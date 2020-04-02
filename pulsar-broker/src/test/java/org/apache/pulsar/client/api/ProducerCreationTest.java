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


import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import static org.testng.Assert.fail;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


import com.google.common.collect.Lists;

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

        producer.getConnectionHandler().connectionClosed(producer.getConnectionHandler().cnx());
        Assert.assertFalse(producer.isConnected());
        Thread.sleep(3000);
        Assert.assertEquals(producer.getConnectionHandler().getEpoch(), 1);
        Assert.assertTrue(producer.isConnected());
    }
    
    /**
     * It tests closing producer waits until it receives acks from the broker.
     * 
     * <pre>
     *  a. Create producer successfully
     *  b. stop broker
     *  c. publish messages
     *  d. close producer with wait flag
     *  f. start broker
     *  g. producer will be closed after receiving ack of in flight published message
     * </pre>
     * 
     * @throws Exception
     */
    @Test(timeOut = 30000)
    public void testCloseProducerWaitForInFlightMessages() throws Exception {
        final String topicName = "persistent://public/default/closeProducerWait";
        admin.topics().createPartitionedTopic(topicName, 4);
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .startingBackoffInterval(10, TimeUnit.MILLISECONDS).maxBackoffInterval(10, TimeUnit.MILLISECONDS)
                .build();
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false).sendTimeout(10, TimeUnit.MINUTES).create();
        stopBroker();
        int totalMessages = 10;
        List<CompletableFuture<MessageId>> sendResults = Lists.newArrayList();
        for (int i = 0; i < totalMessages; i++) {
            sendResults.add(producer.sendAsync("test".getBytes()));
        }
        CompletableFuture<Void> closeFuture = producer.closeAsync();
        startBroker();
        Thread.sleep(1000);
        try {
            producer.send("test".getBytes());
            fail("should have failed because producer is closing");
        } catch (Exception e) {
            // Ok.. expected
        }
        closeFuture.get();
        // all messages must be published successfully before it's closed
        FutureUtil.waitForAll(sendResults).get(10, TimeUnit.SECONDS);
    }
}

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

import io.netty.util.HashedWheelTimer;
import lombok.Cleanup;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for not exists topic.
 */
@Test(groups = "broker-impl")
public class TopicDoesNotExistsTest extends ProducerConsumerBase {

    @Override
    @BeforeClass
    public void setup() throws Exception {
        conf.setAllowAutoTopicCreation(false);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testCreateProducerOnNotExistsTopic() throws PulsarClientException, InterruptedException {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).build();
        try {
            pulsarClient.newProducer()
                    .topic("persistent://public/default/" + UUID.randomUUID().toString())
                    .sendTimeout(100, TimeUnit.MILLISECONDS)
                    .create();
            Assert.fail("Create producer should failed while topic does not exists.");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.NotFoundException);
        }
        Thread.sleep(2000);
        HashedWheelTimer timer = (HashedWheelTimer) ((PulsarClientImpl) pulsarClient).timer();
        Assert.assertEquals(((PulsarClientImpl) pulsarClient).producersCount(), 0);
    }

    @Test
    public void testCreateConsumerOnNotExistsTopic() throws PulsarClientException, InterruptedException {
        @Cleanup
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 1);
        try {
            pulsarClient.newConsumer()
                    .topic("persistent://public/default/" + UUID.randomUUID().toString())
                    .subscriptionName("test")
                    .subscribe();
            Assert.fail("Create consumer should failed while topic does not exists.");
        } catch (PulsarClientException ignore) {
        }
        Thread.sleep(2000);
        HashedWheelTimer timer = (HashedWheelTimer) ((PulsarClientImpl) pulsarClient).timer();
        Assert.assertEquals(timer.pendingTimeouts(), 0);
        Assert.assertEquals(((PulsarClientImpl) pulsarClient).consumersCount(), 0);
    }
}

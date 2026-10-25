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
package org.apache.pulsar.client.impl;

import io.netty.util.HashedWheelTimer;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for not exists topic.
 */
@Test(groups = "broker-impl")
public class TopicDoesNotExistsTest extends SharedPulsarBaseTest {

    @BeforeMethod(alwaysRun = true)
    public void disableAutoTopicCreation() throws Exception {
        admin.namespaces().setAutoTopicCreation(getNamespace(),
                AutoTopicCreationOverride.builder().allowAutoTopicCreation(false).build());
    }

    @Test
    public void testCreateProducerOnNotExistsTopic() throws PulsarClientException, InterruptedException {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(getBrokerServiceUrl()).build();
        try {
            client.newProducer()
                    .topic(newTopicName())
                    .sendTimeout(100, TimeUnit.MILLISECONDS)
                    .create();
            Assert.fail("Create producer should failed while topic does not exists.");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.TopicDoesNotExistException);
        }
        Thread.sleep(2000);
        HashedWheelTimer timer = (HashedWheelTimer) ((PulsarClientImpl) client).timer();
        Assert.assertEquals(timer.pendingTimeouts(), 0);
        Assert.assertEquals(((PulsarClientImpl) client).producersCount(), 0);
    }

    @Test
    public void testCreateConsumerOnNotExistsTopic() throws PulsarClientException, InterruptedException {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .operationTimeout(1, TimeUnit.SECONDS)
                .build();
        try {
            client.newConsumer()
                    .topic(newTopicName())
                    .subscriptionName("test")
                    .subscribe();
            Assert.fail("Create consumer should failed while topic does not exists.");
        } catch (PulsarClientException ignore) {
        }
        Thread.sleep(2000);
        HashedWheelTimer timer = (HashedWheelTimer) ((PulsarClientImpl) client).timer();
        Assert.assertEquals(timer.pendingTimeouts(), 0);
        Assert.assertEquals(((PulsarClientImpl) client).consumersCount(), 0);
    }
}

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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ConsumeBaseExceptionTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testClosedConsumer() throws PulsarClientException {
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/topicName")
                .subscriptionName("my-subscription").subscribe();
        consumer.close();
        Assert.assertTrue(consumer.receiveAsync().isCompletedExceptionally());

        try {
            consumer.receiveAsync().exceptionally(e -> {
                Assert.assertTrue(e instanceof PulsarClientException.AlreadyClosedException);
                return null;
            }).get();
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testListener() throws PulsarClientException {

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/topicName")
                .subscriptionName("my-subscription").messageListener((consumer1, msg) -> {

                }).subscribe();
        Assert.assertTrue(consumer.receiveAsync().isCompletedExceptionally());

        try {
            consumer.receiveAsync().exceptionally(e -> {
                Assert.assertTrue(e instanceof PulsarClientException.InvalidConfigurationException);
                return null;
            }).get();
        } catch (Exception e) {
            Assert.fail();
        }
    }
}

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

package com.yahoo.pulsar.client.impl;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.ProducerConsumerBase;
import com.yahoo.pulsar.client.api.PulsarClientException;

public class ConsumeBaseExceptionTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testClosedConsumer() throws PulsarClientException {
        Consumer consumer = null;
        consumer = pulsarClient.subscribe("persistent://prop/cluster/ns/topicName", "my-subscription");
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
        Consumer consumer = null;
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setMessageListener((Consumer c, Message msg) -> {
        });
        consumer = pulsarClient.subscribe("persistent://prop/cluster/ns/topicName", "my-subscription", conf);
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

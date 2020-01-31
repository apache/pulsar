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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 */
public class MessagePublishBufferThrottleTest extends BrokerTestBase {

    @Override
    protected void setup() throws Exception {
        //No-op
    }

    @Override
    protected void cleanup() throws Exception {
        //No-op
    }

    @Test
    public void testMessagePublishBufferThrottleDisabled() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(-1);
        super.baseSetup();
        Assert.assertFalse(pulsar.getBrokerService().increasePublishBufferSizeAndCheckStopRead(1));
        Assert.assertFalse(pulsar.getBrokerService().decreasePublishBufferSizeAndCheckResumeRead(1));
    }

    @Test
    public void testMessagePublishBufferThrottle() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(1);
        super.baseSetup();
        Assert.assertFalse(pulsar.getBrokerService().increasePublishBufferSizeAndCheckStopRead(512 * 1024));
        Assert.assertTrue(pulsar.getBrokerService().increasePublishBufferSizeAndCheckStopRead(512 * 1024));
        Assert.assertTrue(pulsar.getBrokerService().increasePublishBufferSizeAndCheckStopRead( 1024));
        Assert.assertFalse(pulsar.getBrokerService().decreasePublishBufferSizeAndCheckResumeRead(1024));
        Assert.assertFalse(pulsar.getBrokerService().decreasePublishBufferSizeAndCheckResumeRead(512 * 1024));
        Assert.assertTrue(pulsar.getBrokerService().decreasePublishBufferSizeAndCheckResumeRead(1024));
        Assert.assertFalse(pulsar.getBrokerService().decreasePublishBufferSizeAndCheckResumeRead(1024));
        Assert.assertTrue(pulsar.getBrokerService().increasePublishBufferSizeAndCheckStopRead(514 * 1024));
    }

    @Test
    public void testCurrentPublishBufferShouldBeZeroWhenComplete() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(1);
        super.baseSetup();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>();
        final int messages = 200;
        final int producers = 10;

        List<Producer<byte[]>> producerList = new ArrayList<>();
        for (int i = 0; i < producers; i++) {
            Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://prop/ns-abc/testCurrentPublishBufferShouldBeZeroWhenComplete")
                .enableBatching(false)
                .create();
            producerList.add(producer);
        }

        for (Producer<byte[]> producer : producerList) {
            for (int j = 0; j < messages; j++) {
                futures.add(producer.sendAsync(new byte[1024]));
            }
        }

        FutureUtil.waitForAll(futures).get();
        Assert.assertEquals(futures.size(), messages * producers);
        Assert.assertEquals(pulsar.getBrokerService().getCurrentMessagePublishBufferSize().get(), 0);
        Assert.assertTrue(pulsar.getBrokerService().messagePublishBufferThrottleTimes > 0
            && pulsar.getBrokerService().messagePublishBufferThrottleTimes == pulsar.getBrokerService().messagePublishBufferResumeTimes);
    }
}

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

import io.netty.util.HashedWheelTimer;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker-api")
public class ProducerCleanupTest extends ProducerConsumerBase {

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

    @Test
    public void testAllTimerTaskShouldCanceledAfterProducerClosed() throws PulsarClientException, InterruptedException {
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://public/default/" + UUID.randomUUID().toString())
                .sendTimeout(1, TimeUnit.SECONDS)
                .create();
        producer.close();
        Thread.sleep(2000);
        HashedWheelTimer timer = (HashedWheelTimer) ((PulsarClientImpl) pulsarClient).timer();
        Assert.assertEquals(timer.pendingTimeouts(), 0);
    }
}

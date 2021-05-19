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
import lombok.Cleanup;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.UUID;

@Test(groups = "broker-api")
public class ConsumerCleanupTest extends ProducerConsumerBase {

    @BeforeClass
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

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testAllTimerTaskShouldCanceledAfterConsumerClosed(boolean ackReceiptEnabled)
            throws PulsarClientException, InterruptedException {
        @Cleanup
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 1);
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://public/default/" + UUID.randomUUID().toString())
                .subscriptionName("test")
                .isAckReceiptEnabled(ackReceiptEnabled)
                .subscribe();
        consumer.close();
        Thread.sleep(2000);
        HashedWheelTimer timer = (HashedWheelTimer) ((PulsarClientImpl) pulsarClient).timer();
        Assert.assertEquals(timer.pendingTimeouts(), 0);
    }
}

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
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

    @DataProvider(name = "ackResponseTimeout")
    public Object[][] ackResponseTimeout() {
        return new Object[][] { { 0L }, { 3000L } };
    }

    @Test(dataProvider = "ackResponseTimeout")
    public void testAllTimerTaskShouldCanceledAfterConsumerClosed(long ackResponseTimeout)
            throws PulsarClientException, InterruptedException {
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 1);
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://public/default/" + UUID.randomUUID().toString())
                .subscriptionName("test")
                .ackResponseTimeout(ackResponseTimeout, TimeUnit.MILLISECONDS)
                .subscribe();
        consumer.close();
        Awaitility.await().atMost(6000L, TimeUnit.MILLISECONDS).until(() ->
                ((HashedWheelTimer) ((PulsarClientImpl) pulsarClient).timer()).pendingTimeouts() == 0);
    }
}

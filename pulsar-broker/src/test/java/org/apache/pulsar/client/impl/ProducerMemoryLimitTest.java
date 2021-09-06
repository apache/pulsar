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

import lombok.Cleanup;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker-impl")
public class ProducerMemoryLimitTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 10_000)
    public void testProducerTimeoutMemoryRelease() throws Exception {
        initClientWithMemoryLimit();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerMemoryLimit")
                .sendTimeout(5, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .enableBatching(false)
                .create();
        this.stopBroker();
        try {
            producer.send("memory-test".getBytes(StandardCharsets.UTF_8));
            throw new IllegalStateException("can not reach here");
        } catch (PulsarClientException.TimeoutException ex) {
            PulsarClientImpl clientImpl = (PulsarClientImpl) this.pulsarClient;
            final MemoryLimitController memoryLimitController = clientImpl.getMemoryLimitController();
            Assert.assertEquals(memoryLimitController.currentUsage(), 0);
        }

    }

    @Test(timeOut = 10_000)
    public void testProducerCloseMemoryRelease() throws Exception {
        initClientWithMemoryLimit();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerMemoryLimit")
                .sendTimeout(5, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .enableBatching(false)
                .create();
        this.stopBroker();
        producer.sendAsync("memory-test".getBytes(StandardCharsets.UTF_8));
        producer.close();
        PulsarClientImpl clientImpl = (PulsarClientImpl) this.pulsarClient;
        final MemoryLimitController memoryLimitController = clientImpl.getMemoryLimitController();
        Assert.assertEquals(memoryLimitController.currentUsage(), 0);
    }

    private void initClientWithMemoryLimit() throws PulsarClientException {
        pulsarClient = PulsarClient.builder().
                serviceUrl(lookupUrl.toString())
                .memoryLimit(50, SizeUnit.KILO_BYTES)
                .build();
    }

}

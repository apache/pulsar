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

import lombok.Cleanup;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker-impl")
public class ProducerMaxBytesTest extends ProducerConsumerBase {

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
    public void testProduceMaxThanBatchSize() throws Exception {
        final String topic = "testProduceMaxThanBatchSize";
        final int maxBatchSize = 1024;
        final int messageSize = maxBatchSize + 1;
        final String message = RandomStringUtils.randomAlphabetic(messageSize);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) client.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxBytes(maxBatchSize)
                .create();

        try {
            producer.send(message.getBytes(StandardCharsets.UTF_8));
            throw new IllegalStateException("Should have failed to send message");
        } catch (PulsarClientException.InvalidMessageException ex) {
            Assert.assertNotNull(ex);
        }
    }

}

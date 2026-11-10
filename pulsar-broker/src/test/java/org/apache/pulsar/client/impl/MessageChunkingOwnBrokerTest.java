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

import static org.apache.pulsar.client.impl.MessageChunkingTest.createMessagePayload;
import static org.testng.Assert.fail;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Chunking tests that stop the broker, so they run on a broker of their own. {@link MessageChunkingTest} has the
 * tests that share one.
 */
@Test(groups = "broker-impl")
@CustomLog
public class MessageChunkingOwnBrokerTest extends ProducerConsumerBase {

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

    @Test
    public void testPublishWithFailure() throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        this.conf.setMaxMessageSize(50);
        final String topicName = "persistent://my-property/my-ns/my-topic1";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName);

        Producer<byte[]> producer = producerBuilder.enableChunking(true).enableBatching(false)
                .create();

        stopBroker();

        try {
            producer.send(createMessagePayload(1000).getBytes());
            fail("should have failed with timeout exception");
        } catch (PulsarClientException.TimeoutException e) {
            // Ok
        }
        producer.close();
    }
}

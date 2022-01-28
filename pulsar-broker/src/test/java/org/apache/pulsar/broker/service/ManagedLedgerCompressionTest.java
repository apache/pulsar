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

import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * ManagedLedgerInfo compression configuration test.
 */

@Test(groups = {"broker", "broker-jdk8"})
public class ManagedLedgerCompressionTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setManagedLedgerInfoCompressionType(MLDataFormats.CompressionType.NONE.name());
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 1000 * 20)
    public void testRestartBrokerEnableManagedLedgerInfoCompression() throws Exception {
        String topic = newTopicName();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        int messageCnt = 100;
        produceAndConsume(producer, consumer, messageCnt);

        stopBroker();
        conf.setManagedLedgerInfoCompressionType(MLDataFormats.CompressionType.ZSTD.name());
        startBroker();
        produceAndConsume(producer, consumer, messageCnt);

        stopBroker();
        conf.setManagedLedgerInfoCompressionType(MLDataFormats.CompressionType.LZ4.name());
        startBroker();
        produceAndConsume(producer, consumer, messageCnt);

        stopBroker();
        conf.setManagedLedgerInfoCompressionType("INVALID");
        try {
            startBroker();
            Assert.fail("The managedLedgerInfo compression type is invalid, should fail.");
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
            Assert.assertEquals(
                    "No enum constant org.apache.bookkeeper.mledger.proto.MLDataFormats.CompressionType.INVALID",
                    e.getCause().getMessage());
        }

        conf.setManagedLedgerInfoCompressionType(MLDataFormats.CompressionType.NONE.name());
        startBroker();
        produceAndConsume(producer, consumer, messageCnt);
    }

    private void produceAndConsume(Producer<byte[]> producer,
                                   Consumer<byte[]> consumer, int messageCnt) throws PulsarClientException {
        for (int i = 0; i < messageCnt; i++) {
            producer.newMessage().value("test".getBytes()).send();
        }
        for (int i = 0; i < messageCnt; i++) {
            Message<byte[]> message = consumer.receive(1000, TimeUnit.SECONDS);
            consumer.acknowledge(message);
            Assert.assertNotNull(message);
        }
    }

}

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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ProducerEncryptionKeyFailureTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
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

    /** A key reader that cannot find any key, as a misconfigured deployment would. */
    private static final class MissingKeyReader implements CryptoKeyReader {
        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
            return null;
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
            return null;
        }
    }

    @Test(timeOut = 60_000)
    public void testProducerWhoseKeyCannotBeLoadedIsNotLeftBehind() throws Exception {
        String topic = "persistent://my-property/my-ns/crypto-key-failure-" + System.nanoTime();
        // Created up front so that the broker can report its publishers whether or not a producer connects.
        admin.topics().createNonPartitionedTopic(topic);
        PulsarClientImpl client = (PulsarClientImpl) pulsarClient;

        expectThrows(PulsarClientException.CryptoException.class, () -> pulsarClient.newProducer()
                .topic(topic)
                .addEncryptionKey("missing-key")
                .cryptoKeyReader(new MissingKeyReader())
                .create());

        // The connection is opened by another thread: give it time to register the producer, which it must not.
        Awaitility.await().during(3, TimeUnit.SECONDS).atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertTrue(admin.topics().getStats(topic).getPublishers().isEmpty(),
                        "A producer whose creation failed must not stay registered on the broker"));
        assertEquals(client.producersCount(), 0, "A producer whose creation failed must not stay in the client");
    }
}

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
package org.apache.pulsar.client;

import static org.testng.Assert.assertNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.MessageImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EncryptionProducerConsumerTest extends TlsProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(EncryptionProducerConsumerTest.class);

    @Test(timeOut = 30000)
    public void testRSAEncryption() throws Exception {


        internalSetUpForNamespace();
        internalSetUpForClient(true, pulsar.getWebServiceAddressTls());


        String topicName = "persistent://my-property/my-ns/myrsa-topic1-" + System.currentTimeMillis();

        class EncKeyReader implements CryptoKeyReader {

            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        return keyInfo;
                    } catch (IOException e) {
                        Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                    }
                } else {
                    Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
                }
                return null;
            }

            @Override
            public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/private-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        return keyInfo;
                    } catch (IOException e) {
                        Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                    }
                } else {
                    Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
                }
                return null;
            }
        }

        final int totalMsg = 10;

        Set<String> messageSet = new HashSet<>();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/myrsa-topic1")
                .subscriptionName("my-subscriber-name").cryptoKeyReader(new EncKeyReader()).subscribe();
        Consumer<byte[]> normalConsumer = pulsarClient.newConsumer()
                .topic(topicName).subscriptionName("my-subscriber-name-normal")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/myrsa-topic1")
                .addEncryptionKey("client-rsa.pem").cryptoKeyReader(new EncKeyReader()).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic("persistent://my-property/my-ns/myrsa-topic1")
                .addEncryptionKey("client-rsa.pem").cryptoKeyReader(new EncKeyReader()).create();

        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        for (int i = totalMsg; i < totalMsg * 2; i++) {
            String message = "my-message-" + i;
            producer2.send(message.getBytes());
        }

        MessageImpl<byte[]> msg = null;

        msg = (MessageImpl<byte[]>) normalConsumer.receive(500, TimeUnit.MILLISECONDS);
        // should not able to read message using normal message.
        assertNull(msg);

        for (int i = 0; i < totalMsg * 2; i++) {
            msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
            // verify that encrypted message contains encryption-context
            msg.getEncryptionCtx()
                    .orElseThrow(() -> new IllegalStateException("encryption-ctx not present for encrypted message"));
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
    }

    //TODO ECDSA key test
}

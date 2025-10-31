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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Slf4j
@Test(groups = "broker-api")
public class ConsumerDecryptFailListenerTest extends ProducerConsumerBase {

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

    @Test(timeOut = 10000)
    public void testDecryptFailListenerException() {
        final String topic = BrokerTestUtil.newUniqueName(
                "persistent://my-property/my-ns/testReaderBuildExceptionsWithSetReaderDecryptFailure"
        );
        // should throw exception if decryptFailListener is set without setting a messageListener
        assertThatThrownBy(
                () -> pulsarClient.newConsumer().topic(topic)
                        .decryptFailListener(((reader, msg) -> {
                        }))
                        .subscriptionName("my-sub")
                        .subscribe()
        )
                .isInstanceOf(PulsarClientException.class)
                .hasMessageContaining("decryptFailListener must be set with messageListener");

        // should throw exception if decryptFailListener was set with cryptoFailureAction
        assertThatThrownBy(
                () -> pulsarClient.newConsumer().topic(topic)
                        .decryptFailListener(((reader, msg) -> {
                        }))
                        .messageListener((reader, msg) -> {
                        })
                        .subscriptionName("my-sub")
                        .cryptoFailureAction(ConsumerCryptoFailureAction.FAIL)
                        .subscribe()
        )
                .isInstanceOf(PulsarClientException.class)
                .hasMessageContaining("decryptFailListener can't be set with cryptoFailureAction");
    }

    @Test(timeOut = 20000)
    public void testDecryptFailListenerBehaviorWithConsumerImpl() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName(
                "persistent://my-property/my-ns/testDecryptFailListenerBehaviorWithConsumerImpl"
        );
        admin.topics().createNonPartitionedTopic(topic);
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic)
                .decryptFailListener(((reader, msg) -> {
                }))
                .messageListener((reader, msg) -> {
                })
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-sub")
                .subscribe();
        assertNull(consumer1.conf.getCryptoFailureAction());

        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic)
                .messageListener((reader, msg) -> {
                })
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-sub")
                .subscribe();

        // cryptoFailureAction should be null when decryptFailListener is set
        assertNull(consumer1.conf.getCryptoFailureAction());
        // cryptoFailureAction should be FAIL by default when decryptFailListener is not set
        assertEquals(consumer2.conf.getCryptoFailureAction(), ConsumerCryptoFailureAction.FAIL);

        consumer1.close();
        consumer2.close();
    }

    @Test(timeOut = 20000)
    public void testDecryptFailListenerBehaviorWithMultiConsumerImpl() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName(
                "persistent://my-property/my-ns/testDecryptFailListenerBehaviorWithMultiConsumerImpl"
        );
        admin.topics().createPartitionedTopic(topic, 3);
        MultiTopicsConsumerImpl<byte[]> consumer1 = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic)
                .decryptFailListener(((reader, msg) -> {
                }))
                .messageListener((reader, msg) -> {
                })
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-sub")
                .subscribe();
        assertNull(consumer1.conf.getCryptoFailureAction());

        MultiTopicsConsumerImpl<byte[]> consumer2 = (MultiTopicsConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topic)
                .messageListener((reader, msg) -> {
                })
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-sub")
                .subscribe();

        // cryptoFailureAction should be null when decryptFailListener is set
        assertNull(consumer1.conf.getCryptoFailureAction());
        // cryptoFailureAction should be FAIL by default when decryptFailListener is not set
        assertEquals(consumer2.conf.getCryptoFailureAction(), ConsumerCryptoFailureAction.FAIL);

        consumer1.close();
        consumer2.close();
    }

    @Test(timeOut = 30000)
    public void testDecryptFailListenerReceiveMessage() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName(
                "persistent://my-property/my-ns/testDecryptFailListenerReceiveMessage"
        );
        admin.topics().createNonPartitionedTopic(topic);
        int totalMessages = 10;
        CountDownLatch countDownLatch = new CountDownLatch(10);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .decryptFailListener(((c, msg) -> {
                    // all messages should come into this listener due to no crypto key is set in this consumer
                    assertTrue(msg.getEncryptionCtx().isPresent());
                    assertTrue(msg.getEncryptionCtx().get().isEncrypted());
                    countDownLatch.countDown();
                }))
                .messageListener((c, msg) -> {
                })
                .subscriptionName("my-sub")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .addEncryptionKey("client-rsa.pem")
                .cryptoKeyReader(new EncKeyReader1())
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        for (int i = 0; i < totalMessages; i++) {
            producer.send(("msg-" + i).getBytes());
        }
        countDownLatch.await();

        consumer.close();
        producer.close();
    }

    /**
     * Test both decryptFailListener and messageListener receive messages.
     */
    @Test(timeOut = 30000)
    public void testBothDecryptFailListenerAndMessageListenerReceiveMessage() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName(
                "persistent://my-property/my-ns/testBothDecryptFailListenerAndMessageListenerReceiveMessage"
        );
        int totalMessages = 10;
        CountDownLatch decryptSuccessCount = new CountDownLatch(5);
        CountDownLatch decryptFailCount = new CountDownLatch(5);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .decryptFailListener(((c, msg) -> {
                    decryptFailCount.countDown();
                }))
                .cryptoKeyReader(new EncKeyReader1())
                .messageListener((c, msg) -> {
                    decryptSuccessCount.countDown();
                })
                .subscriptionName("my-sub")
                .subscribe();

        Producer<byte[]> producer1 = pulsarClient.newProducer()
                .topic(topic)
                .addEncryptionKey("client-rsa.pem")
                .cryptoKeyReader(new EncKeyReader1())
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic(topic)
                .addEncryptionKey("client-rsa.pem")
                .cryptoKeyReader(new EncKeyReader2())
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        for (int i = 0; i < totalMessages; i++) {
            if (i % 2 == 0) {
                producer1.send(("msg-" + i).getBytes());
            } else {
                producer2.send(("msg-" + i).getBytes());
            }
        }

        decryptSuccessCount.await();
        decryptFailCount.await();

        consumer.close();
        producer1.close();
        producer2.close();
    }

    static class EncKeyReader1 implements CryptoKeyReader {

        final EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
            String certFilePath = "./src/test/resources/certificate/public-key.client-rsa.pem";
            if (Files.isReadable(Paths.get(certFilePath))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(certFilePath)));
                    return keyInfo;
                } catch (IOException e) {
                    log.error("Failed to read certificate from {}", certFilePath);
                }
            }
            return null;
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
            String certFilePath = "./src/test/resources/certificate/private-key.client-rsa.pem";
            if (Files.isReadable(Paths.get(certFilePath))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(certFilePath)));
                    return keyInfo;
                } catch (IOException e) {
                    log.error("Failed to read certificate from {}", certFilePath);
                }
            }
            return null;
        }
    }

    static class EncKeyReader2 implements CryptoKeyReader {

        final EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
            String certFilePath = "./src/test/resources/certificate/public-key.client-ecdsa.pem";
            if (Files.isReadable(Paths.get(certFilePath))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(certFilePath)));
                    return keyInfo;
                } catch (IOException e) {
                    log.error("Failed to read certificate from {}", certFilePath);
                }
            }
            return null;
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
            String certFilePath = "./src/test/resources/certificate/private-key.client-ecdsa.pem";
            if (Files.isReadable(Paths.get(certFilePath))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(certFilePath)));
                    return keyInfo;
                } catch (IOException e) {
                    log.error("Failed to read certificate from {}", certFilePath);
                }
            }
            return null;
        }
    }


}

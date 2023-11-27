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
package org.apache.pulsar.security.tls.ec;


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.security.tls.MockedPulsarStandalone;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class TlsWithECCertificateFileTest extends MockedPulsarStandalone {

    @BeforeClass(alwaysRun = true)
    public void suitSetup() {
        loadECTlsCertificateWithFile();
        enableTlsAuthentication();
        super.start(); // start standalone service
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void suitShutdown() {
        super.close(); // close standalone service
    }


    @Test(expectedExceptions = PulsarClientException.class)
    @SneakyThrows
    public void testConnectionFailWithoutCertificate() {
        @Cleanup final PulsarClient client = PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrlTls())
                .build();
        @Cleanup final Producer<byte[]> producer = client.newProducer()
                .topic("should_be_failed")
                .create();
    }


    @Test
    @SneakyThrows
    public void testConnectionSuccessWithCertificate() {
        final AuthenticationTls authentication = new AuthenticationTls(TLS_EC_CLIENT_CERT_PATH, TLS_EC_CLIENT_KEY_PATH);
        final String topicName = "persistent://public/default/" + UUID.randomUUID();
        final int testMsgNum = 10;
        @Cleanup final PulsarAdmin admin = PulsarAdmin.builder()
                .authentication(authentication)
                .serviceHttpUrl(getPulsarService().getWebServiceAddressTls())
                .tlsTrustCertsFilePath(TLS_EC_TRUSTED_CERT_PATH)
                .build();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, "sub-1", MessageId.earliest);
        @Cleanup final PulsarClient client = PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrlTls())
                .authentication(authentication)
                .tlsTrustCertsFilePath(TLS_EC_TRUSTED_CERT_PATH)
                .build();
        @Cleanup final Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .create();
        @Cleanup final Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName("sub-1")
                .consumerName("cons-1")
                .subscribe();
        for (int i = 0; i < testMsgNum; i++) {
            producer.send((i + "").getBytes(StandardCharsets.UTF_8));
        }

        for (int i = 0; i < testMsgNum; i++) {
            final Message<byte[]> message = consumer.receive();
            assertNotNull(message);
            final byte[] b = message.getValue();
            final String s = new String(b, StandardCharsets.UTF_8);
            assertEquals(s, i + "");
        }
    }
}

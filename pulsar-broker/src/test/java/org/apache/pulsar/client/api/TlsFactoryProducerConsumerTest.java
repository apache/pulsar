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
package org.apache.pulsar.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 2b: verifies the broker serves TLS end-to-end over the new {@code PulsarTlsFactory} path
 * (selected by {@code tlsFactoryClassName=default}, which wires the built-in {@code DefaultBrokerTlsFactory})
 * for both the binary listener (purpose BROKER) and the web server (purpose WEB), including mTLS client-auth
 * in REQUIRE mode. The legacy PIP-337 path is exercised by {@link TlsProducerConsumerTest}.
 */
@Test(groups = "broker-api")
public class TlsFactoryProducerConsumerTest extends TlsProducerConsumerBase {

    @Override
    protected void internalSetUpForBroker() {
        super.internalSetUpForBroker();
        // Select the new PIP-478 TLS SPI (built-in DefaultBrokerTlsFactory) for BROKER/PROXY/WEB purposes.
        conf.setTlsFactoryClassName("default");
    }

    @Test(timeOut = 60000)
    public void testTlsFactoryProduceConsumeOverBinaryAndHttps() throws Exception {
        // Admin over HTTPS exercises the web server on the new WEB-purpose path.
        internalSetUpForNamespace();

        // Producer/consumer over binary TLS with mTLS client-auth exercises the BROKER-purpose path.
        internalSetUpForClient(true, pulsar.getBrokerServiceUrlTls());
        final String topic = "persistent://my-property/my-ns/tls-factory-topic";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("tls-factory-sub").subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        final int numMessages = 10;
        for (int i = 0; i < numMessages; i++) {
            producer.send(("msg-" + i).getBytes());
        }

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertThat(msg).isNotNull();
            assertThat(new String(msg.getData())).isEqualTo("msg-" + i);
            consumer.acknowledge(msg);
        }
    }

    @Test(timeOut = 60000)
    public void testTlsFactoryBinaryProtocolRequiresClientCert() throws Exception {
        internalSetUpForNamespace();
        final String topic = "persistent://my-property/my-ns/tls-factory-noauth-topic";

        // Without client certs, the REQUIRE-mode broker must fail the TLS handshake.
        internalSetUpForClient(false, pulsar.getBrokerServiceUrlTls());
        assertThat(catchSubscribeFailure(topic)).isTrue();

        // With client certs, the connection succeeds.
        internalSetUpForClient(true, pulsar.getBrokerServiceUrlTls());
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("tls-factory-sub").subscribe();
        assertThat(consumer.isConnected()).isTrue();
    }

    private boolean catchSubscribeFailure(String topic) {
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("tls-factory-sub").subscriptionType(SubscriptionType.Exclusive).subscribe()) {
            return false;
        } catch (Exception ex) {
            return true;
        }
    }
}

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

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import lombok.Cleanup;

@Test(groups = "broker-api")
public class ProxyProtocolTest extends TlsProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyProtocolTest.class);

    @Test
    public void testSniProxyProtocol() throws Exception {

        // Client should try to connect to proxy and pass broker-url as SNI header
        String proxyUrl = pulsar.getBrokerServiceUrlTls();
        String brokerServiceUrl = "pulsar+ssl://unresolvable-address:6651";
        String topicName = "persistent://my-property/use/my-ns/my-topic1";

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(brokerServiceUrl)
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH).enableTls(true).allowTlsInsecureConnection(false)
                .proxyServiceUrl(proxyUrl, ProxyProtocol.SNI).operationTimeout(1000, TimeUnit.MILLISECONDS);
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        clientBuilder.authentication(AuthenticationTls.class.getName(), authParams);

        @Cleanup
        PulsarClient pulsarClient = clientBuilder.build();
        // should be able to create producer successfully
        pulsarClient.newProducer().topic(topicName).create();
    }

    @Test
    public void testSniProxyProtocolWithInvalidProxyUrl() throws Exception {

        // Client should try to connect to proxy and pass broker-url as SNI header
        String brokerServiceUrl = "pulsar+ssl://1.1.1.1:6651";
        String proxyHost = "invalid-url";
        String proxyUrl = "pulsar+ssl://" + proxyHost + ":5555";
        String topicName = "persistent://my-property/use/my-ns/my-topic1";

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(brokerServiceUrl)
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH).enableTls(true).allowTlsInsecureConnection(false)
                .proxyServiceUrl(proxyUrl, ProxyProtocol.SNI).operationTimeout(1000, TimeUnit.MILLISECONDS);
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        clientBuilder.authentication(AuthenticationTls.class.getName(), authParams);

        @Cleanup
        PulsarClient pulsarClient = clientBuilder.build();

        try {
            pulsarClient.newProducer().topic(topicName).create();
            fail("should have failed due to invalid url");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains(proxyHost));
        }
    }

    @Test
    public void testSniProxyProtocolWithoutTls() throws Exception {
        // Client should try to connect to proxy and pass broker-url as SNI header
        String proxyUrl = pulsar.getBrokerServiceUrl();
        String brokerServiceUrl = "pulsar+ssl://1.1.1.1:6651";
        String topicName = "persistent://my-property/use/my-ns/my-topic1";

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(brokerServiceUrl)
                .proxyServiceUrl(proxyUrl, ProxyProtocol.SNI).operationTimeout(1000, TimeUnit.MILLISECONDS);

        @Cleanup
        PulsarClient pulsarClient = clientBuilder.build();

        try {
            pulsarClient.newProducer().topic(topicName).create();
            fail("should have failed due to non-tls url");
        } catch (PulsarClientException e) {
            // Ok
        }
    }
}

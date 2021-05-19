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

import java.net.InetAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.testng.annotations.Test;

import lombok.Cleanup;

@Test(groups = "broker-api")
public class TlsSniTest extends TlsProducerConsumerBase {

    /**
     * Verify that using an IP-address in the broker service URL will work with using the SNI capabilities
     * of the client. If we try to create an {@link javax.net.ssl.SSLEngine} with a peer host that is an
     * IP address, the peer host is ignored, see for example
     * {@link io.netty.handler.ssl.ReferenceCountedOpenSslEngine}.
     *
     */
    @Test
    public void testIpAddressInBrokerServiceUrl() throws Exception {
        String topicName = "persistent://my-property/use/my-ns/my-topic1";

        URI brokerServiceUrlTls = new URI(pulsar.getBrokerServiceUrlTls());

        String brokerServiceIpAddressUrl = String.format("pulsar+ssl://%s:%d",
                    InetAddress.getByName(brokerServiceUrlTls.getHost()).getHostAddress(),
                    brokerServiceUrlTls.getPort());

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(brokerServiceIpAddressUrl)
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH).allowTlsInsecureConnection(false)
                .enableTlsHostnameVerification(false)
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        clientBuilder.authentication(AuthenticationTls.class.getName(), authParams);

        @Cleanup
        PulsarClient pulsarClient = clientBuilder.build();
        // should be able to create producer successfully
        pulsarClient.newProducer().topic(topicName).create();
    }
}


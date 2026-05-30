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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.api.v5.config.TlsPolicy;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

/**
 * Service-URL validation on the V5 client builder. The v5 client only speaks the
 * broker binary protocol, so {@code pulsar://} / {@code pulsar+ssl://} are the
 * only valid schemes — anything else (especially the admin/web service URL) gets
 * rejected at configure-time with a message that points to the right URL.
 */
public class PulsarClientBuilderV5Test {

    @Test
    public void testAcceptsPulsarScheme() {
        // Must not throw — these are the valid forms.
        PulsarClient.builder().serviceUrl("pulsar://localhost:6650");
        PulsarClient.builder().serviceUrl("pulsar+ssl://localhost:6651");
        PulsarClient.builder().serviceUrl("pulsar://h1:6650,h2:6650,h3:6650");
    }

    @Test
    public void testRejectsHttpWithGuidance() {
        IllegalArgumentException e = assertThrowsIAE(() ->
                PulsarClient.builder().serviceUrl("http://localhost:8080"));
        assertTrue(e.getMessage().contains("pulsar://"),
                "error must point at the correct scheme: " + e.getMessage());
        assertTrue(e.getMessage().toLowerCase().contains("admin")
                        || e.getMessage().toLowerCase().contains("web"),
                "error must call out the http→admin-URL confusion: " + e.getMessage());
        assertTrue(e.getMessage().contains("6650"),
                "error must hint at the broker port: " + e.getMessage());
    }

    @Test
    public void testRejectsHttpsWithGuidance() {
        IllegalArgumentException e = assertThrowsIAE(() ->
                PulsarClient.builder().serviceUrl("https://localhost:8443"));
        assertTrue(e.getMessage().contains("pulsar+ssl://"),
                "error must mention the TLS broker scheme: " + e.getMessage());
    }

    @Test
    public void testRejectsUnknownScheme() {
        IllegalArgumentException e = assertThrowsIAE(() ->
                PulsarClient.builder().serviceUrl("ws://localhost:6650"));
        assertTrue(e.getMessage().contains("pulsar://"),
                "error must point at the correct scheme: " + e.getMessage());
    }

    @Test
    public void testRejectsNullAndBlank() {
        assertThrows(IllegalArgumentException.class,
                () -> PulsarClient.builder().serviceUrl(null));
        assertThrows(IllegalArgumentException.class,
                () -> PulsarClient.builder().serviceUrl(""));
        assertThrows(IllegalArgumentException.class,
                () -> PulsarClient.builder().serviceUrl("   "));
    }

    @Test
    public void testProxyServiceUrlIsValidatedToo() {
        PulsarClientBuilder builder = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650");

        ConnectionPolicy badProxy = ConnectionPolicy.builder()
                .proxy("http://proxy:8080", null)
                .build();

        IllegalArgumentException e = assertThrowsIAE(() -> builder.connectionPolicy(badProxy));
        assertTrue(e.getMessage().contains("proxyServiceUrl"),
                "error must name the offending field: " + e.getMessage());
    }

    @Test
    public void testTlsPolicyFieldsPropagate() {
        // Build a fully-populated TlsPolicy and confirm every field lands on the underlying
        // v4 ClientConfigurationData. Used to be a stub that only set useTls=true.
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        TlsPolicy policy = TlsPolicy.builder()
                .trustCertsFilePath("/path/to/ca.pem")
                .keyFilePath("/path/to/client.key")
                .certificateFilePath("/path/to/client.cert")
                .allowInsecureConnection(true)
                .enableHostnameVerification(false)
                .build();

        builder.tlsPolicy(policy);

        ClientConfigurationData conf = builder.getConfForTesting();
        assertTrue(conf.isUseTls());
        assertEquals(conf.getTlsTrustCertsFilePath(), "/path/to/ca.pem");
        assertEquals(conf.getTlsKeyFilePath(), "/path/to/client.key");
        assertEquals(conf.getTlsCertificateFilePath(), "/path/to/client.cert");
        assertTrue(conf.isTlsAllowInsecureConnection());
        assertFalse(conf.isTlsHostnameVerificationEnable());
    }

    @Test
    public void testTlsPolicyInsecureShortcut() {
        // TlsPolicy.ofInsecure() is the dev convenience that disables verification.
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.tlsPolicy(TlsPolicy.ofInsecure());

        ClientConfigurationData conf = builder.getConfForTesting();
        assertTrue(conf.isUseTls());
        assertTrue(conf.isTlsAllowInsecureConnection());
        assertFalse(conf.isTlsHostnameVerificationEnable());
    }

    @Test
    public void testAuthenticationPluginAndParamsInstantiatesAuthentication() throws Exception {
        // Regression for a bug where authentication(plugin, params) only set the strings and
        // never called conf.setAuthentication(...) with the instantiated Authentication object.
        // PulsarClientImpl reads the Authentication instance via conf.getAuthentication() at
        // connect time — without it, the client connects with no credentials and the broker
        // rejects the handshake. Use the v4 AuthenticationDisabled stub which always exists on
        // the classpath; we only care that *some* instance lands on the conf.
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        builder.authentication(
                "org.apache.pulsar.client.impl.auth.AuthenticationDisabled", "");

        ClientConfigurationData conf = builder.getConfForTesting();
        assertEquals(conf.getAuthPluginClassName(),
                "org.apache.pulsar.client.impl.auth.AuthenticationDisabled");
        assertNotNull(conf.getAuthentication(),
                "Authentication instance must be created and attached to the conf");
    }

    @Test
    public void testAuthenticationPluginNotFoundIsWrapped() {
        // A bad plugin class name should surface as V5 PulsarClientException (not a v4 exception
        // type leaking through the surface).
        PulsarClientBuilderV5 builder = new PulsarClientBuilderV5();
        assertThrows(PulsarClientException.class, () ->
                builder.authentication("com.example.NoSuchAuth", ""));
    }

    private static IllegalArgumentException assertThrowsIAE(Runnable r) {
        try {
            r.run();
            fail("expected IllegalArgumentException");
            return null; // unreachable
        } catch (IllegalArgumentException e) {
            return e;
        }
    }
}

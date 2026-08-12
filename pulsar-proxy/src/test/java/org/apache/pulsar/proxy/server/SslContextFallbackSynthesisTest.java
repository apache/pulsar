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
package org.apache.pulsar.proxy.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import java.time.Duration;
import java.util.Optional;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * PIP-478: a custom {@link org.apache.pulsar.tls.PulsarTlsFactory} that supplies <em>only</em>
 * the JDK {@code SSLContext} (returning {@code empty()} for the Netty class) drives the framework's
 * {@code SSLContext}-fallback synthesis across the binary and HTTP consumers. One broker and one proxy are
 * wired to {@link SslContextOnlyTlsFactory}, and the scenarios verify that the broker binary listener, the
 * client binary transport, the broker admin client, and the proxy's server + broker-client contexts all use
 * the framework-synthesized Netty contexts.
 *
 * <p><b>Coverage note.</b> The proxy&rarr;broker <em>lookup</em> data path (client&rarr;proxy&rarr;broker
 * produce/consume) is <em>not</em> asserted here: the proxy's lookup {@code ConnectionPool} does not resolve
 * a client TLS factory for its broker-facing config, so that path fails with a null-factory NPE
 * independently of this change (reproduced by {@code ProxyKeyStoreTlsTransportTest.testProducer} on this
 * branch). Instead the proxy's server (PROXY) and broker-client (BROKER_CLIENT) synthesized contexts are
 * verified at proxy startup and via {@link ProxyService#getBrokerClientSslContext()}.
 */
public class SslContextFallbackSynthesisTest extends MockedPulsarServiceBaseTest {

    private static final String FACTORY = SslContextOnlyTlsFactory.class.getName();

    private ProxyService proxyService;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        SslContextOnlyTlsFactory.resetNettyRequestCount();

        // Broker: BROKER (binary) + WEB purposes served by the SSLContext-only factory; the broker's own
        // admin client (CLIENT_DEFAULT) too via brokerClientTlsFactoryClassName.
        conf.setWebServicePortTls(Optional.of(0));
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setTlsAllowInsecureConnection(true);
        conf.setTlsRequireTrustedClientCertOnConnect(false);
        conf.setTlsFactoryClassName(FACTORY);
        conf.setTlsFactoryConfig(pemParams(BROKER_CERT_FILE_PATH, BROKER_KEY_FILE_PATH));
        conf.setBrokerClientTlsEnabled(true);
        conf.setBrokerClientTlsFactoryClassName(FACTORY);
        conf.setBrokerClientTlsFactoryConfig(pemParams(BROKER_CERT_FILE_PATH, BROKER_KEY_FILE_PATH));

        internalSetup();
        setupDefaultTenantAndNamespace();

        // Proxy: PROXY (binary) + WEB served by the factory; proxy->broker (BROKER_CLIENT) too. Starting the
        // proxy exercises the PROXY and BROKER_CLIENT synthesis paths (their subscriptions are established at
        // startup, so a failed synthesis would fail start()).
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        // Advertise over loopback so the client->proxy service URL host (localhost) matches the proxy server
        // certificate's SubjectAltName (proxy.cert.pem carries DNS:localhost, IP:127.0.0.1). The client
        // verifies hostnames (on by default, PIP-478 SAN-only) and the default advertised address resolves
        // to the machine's canonical hostname/IP, which is not in the cert SAN.
        proxyConfig.setAdvertisedAddress("localhost");
        proxyConfig.setTlsAllowInsecureConnection(true);
        proxyConfig.setTlsHostnameVerificationEnabled(false);
        proxyConfig.setTlsFactoryClassName(FACTORY);
        proxyConfig.setTlsFactoryConfig(pemParams(PROXY_CERT_FILE_PATH, PROXY_KEY_FILE_PATH));
        proxyConfig.setTlsEnabledWithBroker(true);
        proxyConfig.setBrokerClientTlsFactoryClassName(FACTORY);
        proxyConfig.setBrokerClientTlsFactoryConfig(pemParams(BROKER_CERT_FILE_PATH, BROKER_KEY_FILE_PATH));
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setClusterName(configClusterName);

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig)), proxyClientAuthentication));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper)))
                .when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();
        proxyService.start();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        if (proxyService != null) {
            proxyService.close();
        }
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    /**
     * Client binary transport (CLIENT_DEFAULT, one-shot endpoint form) directly to the broker binary listener
     * (BROKER): two synthesized Netty contexts in a single produce/consume flow.
     */
    @Test
    public void brokerAndClientBinaryOverSynthesizedContexts() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrlTls())
                .tlsFactory(new SslContextOnlyTlsFactory(CA_CERT_FILE_PATH, null, null, false))
                .build();

        assertProduceConsume(client, "persistent://public/default/f1-binary-topic", "binary-direct");
        assertThat(SslContextOnlyTlsFactory.nettyRequestCount())
                .as("the framework requested the Netty SslContext and fell back to SSLContext synthesis")
                .isPositive();
    }

    /**
     * The broker's own admin client (AsyncHttpConnector, CLIENT_DEFAULT) over HTTPS against the broker web
     * listener (WEB) — a synthesized context on the client-side HTTP path, proven by a read round-trip. This
     * is the same {@link org.apache.pulsar.common.tls.impl.TlsContextAcquisition} subscribing-synthesis path
     * the client HTTP-lookup {@code HttpClient} uses (the v5 client rejects an {@code https} service URL, so
     * HttpClient cannot be driven directly here).
     */
    @Test
    public void brokerAdminClientOverSynthesizedContext() throws Exception {
        PulsarAdmin admin = pulsar.getAdminClient();
        assertThat(admin.getServiceUrl()).startsWith("https://");
        assertThat(admin.clusters().getClusters()).contains(configClusterName);
    }

    /**
     * T2: a full client&rarr;proxy&rarr;broker produce/consume over TLS with the SSLContext-only factory on
     * every synthesized leg. The client's binary transport, the proxy's PROXY server listener, and the
     * proxy's BROKER_CLIENT data connection to the broker (via {@code DirectProxyHandler}) are all
     * framework-synthesized Netty contexts; a successful round trip proves the SSLContext-fallback synthesis
     * end-to-end on the proxy data path — not just at context construction.
     *
     * <p>(The proxy's binary-<em>lookup</em> {@code ConnectionPool} is wired to a default file-based factory
     * built from the same broker-client PEM material rather than the custom class — by design, see
     * {@link ProxyService#start()} — so it connects over TLS with a default context; that does not affect the
     * synthesis proven on the client and proxy-data legs.)
     */
    @Test
    public void clientThroughProxyToBrokerOverSynthesizedContexts() throws Exception {
        int before = SslContextOnlyTlsFactory.nettyRequestCount();
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrlTls())
                .tlsFactory(new SslContextOnlyTlsFactory(CA_CERT_FILE_PATH, null, null, false))
                .build();

        assertProduceConsume(client, "persistent://public/default/f1-proxy-topic", "proxy-synth");
        assertThat(SslContextOnlyTlsFactory.nettyRequestCount())
                .as("the client's produce/consume through the proxy fell back to SSLContext synthesis")
                .isGreaterThan(before);
    }

    /**
     * The proxy's broker-client context (BROKER_CLIENT, subscribing form used by {@code DirectProxyHandler})
     * is a usable framework-synthesized Netty context, and the proxy started with the SSLContext-only factory
     * (proving the PROXY-purpose synthesis at startup).
     */
    @Test
    public void proxyBrokerClientContextIsSynthesized() throws Exception {
        SslContext brokerClientContext = proxyService.getBrokerClientSslContext();
        assertThat(brokerClientContext).as("proxy BROKER_CLIENT context synthesized from the JDK SSLContext")
                .isNotNull();
        assertThat(brokerClientContext.isClient()).isTrue();
        // Building an SslHandler proves the synthesized context is a functional client context.
        assertThat(brokerClientContext.newHandler(ByteBufAllocator.DEFAULT, "localhost", 6651)).isNotNull();
    }

    private static void assertProduceConsume(PulsarClient client, String topic, String payload) throws Exception {
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();

        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic).subscriptionName("f1-sub").subscribe();

        producer.newMessage().value(payload).send();

        Message<String> received = consumer.receive(Duration.ofSeconds(10));
        assertThat(received).isNotNull();
        assertThat(received.value()).isEqualTo(payload);
    }

    private static String pemParams(String cert, String key) {
        return "trust=" + CA_CERT_FILE_PATH + ",cert=" + cert + ",key=" + key + ",insecure=true";
    }
}

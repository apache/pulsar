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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
public class ProxyEnableHAProxyProtocolTest extends MockedPulsarServiceBaseTest {

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        conf.setHaProxyProtocolEnabled(true);
        internalSetup();
        setupDefaultTenantAndNamespace();

        proxyConfig.setServicePort(Optional.ofNullable(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setHaProxyProtocolEnabled(true);
        proxyConfig.setClusterName(configClusterName);

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig)), proxyClientAuthentication));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper))).when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();

        proxyService.start();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();

        proxyService.close();
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    @DataProvider
    public Object[][] proxyProtocolHeaders() {
        return new Object[][] {
                {"v1-unknown", "PROXY UNKNOWN\r\n".getBytes(US_ASCII), HAProxyProxiedProtocol.UNKNOWN},
                {"v2-local", HexFormat.of().parseHex("0d0a0d0a000d0a515549540a20000000"),
                        HAProxyProxiedProtocol.UNKNOWN},
                {"v2-unknown", HexFormat.of().parseHex("0d0a0d0a000d0a515549540a21000000"),
                        HAProxyProxiedProtocol.UNKNOWN},
                {"v1-tcp4", "PROXY TCP4 198.51.100.22 203.0.113.7 35646 80\r\n".getBytes(US_ASCII),
                        HAProxyProxiedProtocol.TCP4},
                {"v1-tcp6", "PROXY TCP6 2001:db8::1 2001:db8::2 35646 80\r\n".getBytes(US_ASCII),
                        HAProxyProxiedProtocol.TCP6},
                {"v2-tcp4-tlv", HexFormat.of().parseHex(
                        "0d0a0d0a000d0a515549540a21110011c6336416cb0071078b3e00500100026832"),
                        HAProxyProxiedProtocol.TCP4},
        };
    }

    @Test(dataProvider = "proxyProtocolHeaders")
    public void testProxyProtocolForwarding(String name, byte[] header, HAProxyProxiedProtocol expectedProtocol)
            throws Exception {
        ClientBuilderImpl builder = (ClientBuilderImpl) PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrl()).operationTimeout(5, TimeUnit.SECONDS);
        // Supply only the bytes a load balancer would prepend; use the real client/proxy/broker handshake.
        @Cleanup
        PulsarClient client = InjectedClientCnxClientBuilder.create(builder,
                (clientConf, eventLoopGroup) -> new ClientCnx(InstrumentProvider.NOOP, clientConf, eventLoopGroup) {
                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        ctx.writeAndFlush(Unpooled.wrappedBuffer(header));
                        super.channelActive(ctx);
                    }
                });
        String topic = "persistent://public/default/proxy-protocol-" + name;
        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer().topic(topic)
                .subscriptionName("sub").subscribe();
        HAProxyMessage forwarded = pulsar.getBrokerService().getTopicReference(topic).orElseThrow()
                .getSubscription("sub").getConsumers().get(0).cnx().getHAProxyMessage();
        assertThat(forwarded).isNotNull();
        assertThat(forwarded.protocolVersion()).isEqualTo(HAProxyProtocolVersion.V1);
        assertThat(forwarded.command()).isEqualTo(HAProxyCommand.PROXY);
        assertThat(forwarded.proxiedProtocol()).isEqualTo(expectedProtocol);
        if (expectedProtocol == HAProxyProxiedProtocol.UNKNOWN) {
            assertThat(forwarded.sourceAddress()).isNull();
            assertThat(forwarded.destinationAddress()).isNull();
            assertThat(forwarded.sourcePort()).isZero();
            assertThat(forwarded.destinationPort()).isZero();
        } else {
            boolean ipv6 = expectedProtocol == HAProxyProxiedProtocol.TCP6;
            assertThat(forwarded.sourceAddress()).isEqualTo(ipv6 ? "2001:db8::1" : "198.51.100.22");
            assertThat(forwarded.destinationAddress()).isEqualTo(ipv6 ? "2001:db8::2" : "203.0.113.7");
            assertThat(forwarded.sourcePort()).isEqualTo(35646);
            assertThat(forwarded.destinationPort()).isEqualTo(80);
        }
    }

    @Test
    public void testSimpleProduceAndConsume() throws PulsarClientException, PulsarAdminException {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();

        final String topicName = "persistent://public/default/testSimpleProduceAndConsume";
        final String subName = "my-subscriber-name";
        final int messages = 100;

        @Cleanup
        org.apache.pulsar.client.api.Consumer<byte[]> consumer =
                client.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        @Cleanup
        org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer().topic(topicName).create();
        for (int i = 0; i < messages; i++) {
            producer.send(("Message-" + i).getBytes());
        }

        int received = 0;
        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
            received++;
        }

        Assert.assertEquals(received, messages);

        TopicStats topicStats = admin.topics().getStats(topicName);
        Assert.assertEquals(topicStats.getSubscriptions().size(), 1);
        SubscriptionStats subscriptionStats = topicStats.getSubscriptions().get(subName);
        Assert.assertEquals(subscriptionStats.getConsumers().size(), 1);
        Assert.assertEquals(subscriptionStats.getConsumers().get(0).getAddress(),
                ((ConsumerImpl) consumer).getClientCnx().ctx().channel().localAddress().toString()
                        .replaceFirst("/", ""));

        topicStats = admin.topics().getStats(topicName);
        Assert.assertEquals(topicStats.getPublishers().size(), 1);
        Assert.assertEquals(topicStats.getPublishers().get(0).getAddress(),
                ((ProducerImpl) producer).getClientCnx().ctx().channel().localAddress().toString()
                        .replaceFirst("/", ""));
    }
}

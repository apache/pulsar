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
package org.apache.pulsar.client.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.socks5.Socks5Server;
import org.apache.pulsar.socks5.config.Socks5Config;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.internal.thread.ThreadTimeoutException;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;

@Test
@Slf4j
public class ClientWithSocks5ProxyTest extends BrokerTestBase {

    private Socks5Server server;

    final String topicName = "persistent://public/default/socks5";

    @BeforeMethod
    public void setup() throws Exception {
        baseSetup();
        initData();
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.socks5ProxyAddress(new InetSocketAddress("localhost", 11080))
                .socks5ProxyUsername("socks5")
                .socks5ProxyPassword("pulsar");
    }

    private void startSocks5Server(boolean enableAuth) {
        Socks5Config config = new Socks5Config();
        config.setPort(11080);
        config.setEnableAuth(enableAuth);
        server = new Socks5Server(config);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.start();
                } catch (Exception e) {
                    log.error("start socks5 server error", e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        server.shutdown();
        System.clearProperty("socks5Proxy.address");
    }

    private void initData() throws PulsarAdminException {
        admin.tenants().createTenant("public", new TenantInfo() {
            @Override
            public Set<String> getAdminRoles() {
                return Collections.emptySet();
            }

            @Override
            public Set<String> getAllowedClusters() {
                Set<String> clusters = new HashSet<>();
                clusters.add("test");
                return clusters;
            }
        });
        admin.namespaces().createNamespace("public/default");
        admin.topics().createNonPartitionedTopic(topicName);
    }

    @Test
    public void testSendAndConsumer() throws PulsarClientException {
        startSocks5Server(true);
        // init consumer
        final String subscriptionName = "socks5-subscription";
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        //init producer
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        String msg = "abc";
        producer.send(msg.getBytes());
        Message<byte[]> message = consumer.receive();

        assertEquals(new String(message.getData()), msg);

        consumer.unsubscribe();
    }

    @Test
    public void testDisableAuth() throws PulsarClientException {
        startSocks5Server(false);
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .socks5ProxyAddress(new InetSocketAddress("localhost", 11080));
        PulsarClient pulsarClient = replacePulsarClient(clientBuilder);
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        String msg = "abc";
        producer.send(msg.getBytes());
    }

    @Test
    public void testSetFromSystemProperty() throws PulsarClientException {
        startSocks5Server(false);
        System.setProperty("socks5Proxy.address", "http://localhost:11080");
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl());
        PulsarClient pulsarClient = replacePulsarClient(clientBuilder);
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        String msg = "abc";
        producer.send(msg.getBytes());
    }

    @Test(expectedExceptions = PulsarClientException.class)
    public void testSetErrorProxyAddress() throws PulsarClientException {
        startSocks5Server(false);
        System.setProperty("socks5Proxy.address", "localhost:11080"); // with no scheme
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl());
        PulsarClient pulsarClient = replacePulsarClient(clientBuilder);
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        String msg = "abc";
        producer.send(msg.getBytes());
    }

    @Test(timeOut = 5000, expectedExceptions = {ThreadTimeoutException.class})
    public void testWithErrorPassword() throws PulsarClientException {
        startSocks5Server(true);
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .socks5ProxyAddress(new InetSocketAddress("localhost", 11080))
                .socks5ProxyUsername("socks5")
                .socks5ProxyPassword("error-password");
        PulsarClient pulsarClient = replacePulsarClient(clientBuilder);
        pulsarClient.newProducer()
                .topic(topicName)
                .create();
    }
}

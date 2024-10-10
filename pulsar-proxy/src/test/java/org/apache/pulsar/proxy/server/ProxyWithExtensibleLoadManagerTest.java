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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.ServiceNameResolver;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.net.ServiceURI;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class ProxyWithExtensibleLoadManagerTest extends MultiBrokerBaseTest {

    private static final int TEST_TIMEOUT_MS = 30_000;

    private Authentication proxyClientAuthentication;
    private ProxyService proxyService;

    @Override
    public int numberOfAdditionalBrokers() {
        return 1;
    }

    @Override
    public void doInitConf() throws Exception {
        super.doInitConf();
        configureExtensibleLoadManager(conf);
    }

    @Override
    protected ServiceConfiguration createConfForAdditionalBroker(int additionalBrokerIndex) {
        return configureExtensibleLoadManager(getDefaultConf());
    }

    private ServiceConfiguration configureExtensibleLoadManager(ServiceConfiguration config) {
        config.setNumIOThreads(8);
        config.setLoadBalancerInFlightServiceUnitStateWaitingTimeInMillis(5 * 1000);
        config.setLoadBalancerServiceUnitStateMonitorIntervalInSeconds(1);
        config.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        config.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        config.setLoadBalancerSheddingEnabled(false);
        return config;
    }

    private ProxyConfiguration initializeProxyConfig() {
        var proxyConfig = new ProxyConfiguration();
        proxyConfig.setNumIOThreads(8);
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setClusterName(configClusterName);
        return proxyConfig;
    }

    private <T> T spyField(Object target, String fieldName) throws IllegalAccessException {
        T t = (T) FieldUtils.readDeclaredField(target, fieldName, true);
        var fieldSpy = spy(t);
        FieldUtils.writeDeclaredField(target, fieldName, fieldSpy, true);
        return fieldSpy;
    }

    private PulsarClientImpl createClient(ProxyService proxyService) {
        try {
            return Mockito.spy((PulsarClientImpl) PulsarClient.builder().
                    serviceUrl(proxyService.getServiceUrl()).
                    build());
        } catch (PulsarClientException e) {
            throw new CompletionException(e);
        }
    }

    @NotNull
    private InetSocketAddress getSourceBrokerInetAddress(TopicName topicName) throws PulsarAdminException {
        var srcBrokerUrl = admin.lookups().lookupTopic(topicName.toString());
        var serviceUri = ServiceURI.create(srcBrokerUrl);
        var uri = serviceUri.getUri();
        return InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());
    }

    private String getDstBrokerLookupUrl(TopicName topicName) throws Exception {
        var srcBrokerUrl = admin.lookups().lookupTopic(topicName.toString());
        return getAllBrokers().stream().
                filter(pulsarService -> !Objects.equals(srcBrokerUrl, pulsarService.getBrokerServiceUrl())).
                map(PulsarService::getBrokerId).
                findAny().orElseThrow(() -> new Exception("Could not determine destination broker lookup URL"));
    }

    @BeforeMethod(alwaysRun = true)
    public void proxySetup() throws Exception {
        var proxyConfig = initializeProxyConfig();
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

    @AfterMethod(alwaysRun = true)
    public void proxyCleanup() throws Exception {
        if (proxyService != null) {
            proxyService.close();
        }
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    @Test(timeOut = TEST_TIMEOUT_MS)
    public void testProxyProduceConsume() throws Exception {
        var namespaceName = NamespaceName.get("public", "default");
        var topicName = TopicName.get(TopicDomain.persistent.toString(), namespaceName,
                BrokerTestUtil.newUniqueName("testProxyProduceConsume"));

        @Cleanup("shutdownNow")
        var threadPool = Executors.newCachedThreadPool();

        var producerClientFuture = CompletableFuture.supplyAsync(() -> createClient(proxyService), threadPool);
        var consumerClientFuture = CompletableFuture.supplyAsync(() -> createClient(proxyService), threadPool);

        @Cleanup
        var producerClient = producerClientFuture.get();
        @Cleanup
        var producer = producerClient.newProducer(Schema.INT32).topic(topicName.toString()).create();
        LookupService producerLookupServiceSpy = spyField(producerClient, "lookup");

        @Cleanup
        var consumerClient = consumerClientFuture.get();
        @Cleanup
        var consumer = consumerClient.newConsumer(Schema.INT32).topic(topicName.toString()).
                subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).
                subscriptionName(BrokerTestUtil.newUniqueName("my-sub")).
                ackTimeout(1000, TimeUnit.MILLISECONDS).
                subscribe();
        LookupService consumerLookupServiceSpy = spyField(consumerClient, "lookup");

        var bundleRange = admin.lookups().getBundleRange(topicName.toString());

        var semSend = new Semaphore(0);
        var messagesBeforeUnload = 100;
        var messagesAfterUnload = 100;

        var pendingMessageIds = Collections.synchronizedSet(new HashSet<Integer>());
        var producerFuture = CompletableFuture.runAsync(() -> {
            try {
                for (int i = 0; i < messagesBeforeUnload + messagesAfterUnload; i++) {
                    semSend.acquire();
                    pendingMessageIds.add(i);
                    producer.send(i);
                }
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, threadPool).orTimeout(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        var consumerFuture = CompletableFuture.runAsync(() -> {
            while (!producerFuture.isDone() || !pendingMessageIds.isEmpty()) {
                try {
                    var recvMessage = consumer.receive(1_500, TimeUnit.MILLISECONDS);
                    if (recvMessage != null) {
                        consumer.acknowledge(recvMessage);
                        pendingMessageIds.remove(recvMessage.getValue());
                    }
                } catch (PulsarClientException e) {
                    // Retry
                }
            }
        }, threadPool).orTimeout(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        var dstBrokerLookupUrl = getDstBrokerLookupUrl(topicName);
        semSend.release(messagesBeforeUnload);
        admin.namespaces().unloadNamespaceBundle(namespaceName.toString(), bundleRange, dstBrokerLookupUrl);
        semSend.release(messagesAfterUnload);

        // Verify all futures completed successfully.
        producerFuture.get();
        consumerFuture.get();

        verify(producerClient, times(1)).getProxyConnection(any(), anyInt());
        verify(producerLookupServiceSpy, never()).getBroker(topicName);

        verify(consumerClient, times(1)).getProxyConnection(any(), anyInt());
        verify(consumerLookupServiceSpy, never()).getBroker(topicName);
    }

    @Test(timeOut = TEST_TIMEOUT_MS)
    public void testClientReconnectsToBrokerOnProxyClosing() throws Exception {
        var namespaceName = NamespaceName.get("public", "default");
        var topicName = TopicName.get(TopicDomain.persistent.toString(), namespaceName,
                BrokerTestUtil.newUniqueName("testClientReconnectsToBrokerOnProxyClosing"));

        @Cleanup("shutdownNow")
        var threadPool = Executors.newCachedThreadPool();

        var producerClientFuture = CompletableFuture.supplyAsync(() -> createClient(proxyService), threadPool);
        var consumerClientFuture = CompletableFuture.supplyAsync(() -> createClient(proxyService), threadPool);

        @Cleanup
        var producerClient = producerClientFuture.get();
        @Cleanup
        var producer = (ProducerImpl<Integer>) producerClient.newProducer(Schema.INT32).topic(topicName.toString()).
                create();
        LookupService producerLookupServiceSpy = spyField(producerClient, "lookup");
        when(((ServiceNameResolver) spyField(producerLookupServiceSpy, "serviceNameResolver")).resolveHost()).
                thenCallRealMethod().then(invocation -> getSourceBrokerInetAddress(topicName));

        @Cleanup
        var consumerClient = consumerClientFuture.get();
        @Cleanup
        var consumer = (ConsumerImpl<Integer>) consumerClient.newConsumer(Schema.INT32).topic(topicName.toString()).
                subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).
                subscriptionName(BrokerTestUtil.newUniqueName("my-sub")).
                ackTimeout(1000, TimeUnit.MILLISECONDS).
                subscribe();
        LookupService consumerLookupServiceSpy = spyField(consumerClient, "lookup");
        when(((ServiceNameResolver) spyField(consumerLookupServiceSpy, "serviceNameResolver")).resolveHost()).
                thenCallRealMethod().then(invocation -> getSourceBrokerInetAddress(topicName));

        var bundleRange = admin.lookups().getBundleRange(topicName.toString());

        var semSend = new Semaphore(0);
        var messagesPerPhase = 100;
        var phases = 4;
        var totalMessages = messagesPerPhase * phases;
        var cdlSentMessages = new CountDownLatch(messagesPerPhase * 2);

        var pendingMessageIds = Collections.synchronizedSet(new HashSet<Integer>());
        var producerFuture = CompletableFuture.runAsync(() -> {
            try {
                for (int i = 0; i < totalMessages; i++) {
                    semSend.acquire();
                    pendingMessageIds.add(i);
                    producer.send(i);
                    cdlSentMessages.countDown();
                }
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, threadPool).orTimeout(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        var consumerFuture = CompletableFuture.runAsync(() -> {
            while (!producerFuture.isDone() || !pendingMessageIds.isEmpty()) {
                try {
                    var recvMessage = consumer.receive(1_500, TimeUnit.MILLISECONDS);
                    if (recvMessage != null) {
                        consumer.acknowledge(recvMessage);
                        pendingMessageIds.remove(recvMessage.getValue());
                    }
                } catch (PulsarClientException e) {
                    // Retry
                }
            }
        }, threadPool).orTimeout(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        var dstBrokerLookupUrl = getDstBrokerLookupUrl(topicName);
        semSend.release(messagesPerPhase);
        admin.namespaces().unloadNamespaceBundle(namespaceName.toString(), bundleRange, dstBrokerLookupUrl);
        semSend.release(messagesPerPhase);

        cdlSentMessages.await();
        assertEquals(FieldUtils.readDeclaredField(producer.getConnectionHandler(), "useProxy", true), Boolean.TRUE);
        assertEquals(FieldUtils.readDeclaredField(consumer.getConnectionHandler(), "useProxy", true), Boolean.TRUE);
        semSend.release(messagesPerPhase);
        proxyService.close();
        proxyService = null;
        semSend.release(messagesPerPhase);

        // Verify produce/consume futures completed successfully.
        producerFuture.get();
        consumerFuture.get();

        assertEquals(FieldUtils.readDeclaredField(producer.getConnectionHandler(), "useProxy", true), Boolean.FALSE);
        assertEquals(FieldUtils.readDeclaredField(consumer.getConnectionHandler(), "useProxy", true), Boolean.FALSE);

        verify(producerClient, times(1)).getProxyConnection(any(), anyInt());
        verify(producerLookupServiceSpy, times(1)).getBroker(topicName);

        verify(consumerClient, times(1)).getProxyConnection(any(), anyInt());
        verify(consumerLookupServiceSpy, times(1)).getBroker(topicName);
    }
}

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
package org.apache.pulsar.broker.intercept;

import lombok.Cleanup;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

@Test(groups = "broker")
public class BrokerInterceptorTest extends ProducerConsumerBase {

    private static final String listenerName1 = "listener1";
    private BrokerInterceptor listener1;
    private NarClassLoader ncl1;
    private static final String listenerName2 = "listener2";
    private BrokerInterceptor listener2;
    private NarClassLoader ncl2;

    private Map<String, BrokerInterceptorWithClassLoader> listenerMap;
    private BrokerInterceptors listeners;

    @BeforeMethod
    public void setup() throws Exception {
        this.conf.setDisableBrokerInterceptors(false);

        this.listener1 = mock(BrokerInterceptor.class);
        this.ncl1 = mock(NarClassLoader.class);
        this.listener2 = mock(BrokerInterceptor.class);
        this.ncl2 = mock(NarClassLoader.class);

        this.listenerMap = new HashMap<>();
        this.listenerMap.put(
                listenerName1,
                new BrokerInterceptorWithClassLoader(listener1, ncl1));
        this.listenerMap.put(
                listenerName2,
                new BrokerInterceptorWithClassLoader(listener2, ncl2));
        this.listeners = new BrokerInterceptors(this.listenerMap);
        this.enableBrokerInterceptor = true;
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void cleanup() throws Exception {
        teardown();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        this.listeners.close();

        verify(listener1, times(1)).close();
        verify(listener2, times(1)).close();
        verify(ncl1, times(1)).close();
        verify(ncl2, times(1)).close();
        super.internalCleanup();
    }

    @Test
    public void testInitialize() throws Exception {
        listeners.initialize(pulsar);
        verify(listener1, times(1)).initialize(same(pulsar));
        verify(listener2, times(1)).initialize(same(pulsar));
    }

    @Test
    public void testWebserviceRequest() throws PulsarAdminException {
        BrokerInterceptor listener = pulsar.getBrokerInterceptor();
        Assert.assertTrue(listener instanceof CounterBrokerInterceptor);
        admin.namespaces().createNamespace("public/test", 4);
        Awaitility.await().until(() -> ((CounterBrokerInterceptor) listener).getCount() >= 1);
    }

    @Test
    public void testPulsarCommand() throws PulsarClientException {
        BrokerInterceptor listener = pulsar.getBrokerInterceptor();
        Assert.assertTrue(listener instanceof CounterBrokerInterceptor);
        pulsarClient.newProducer(Schema.BOOL).topic("test").create();
        // CONNECT and PRODUCER
        Awaitility.await().until(() -> ((CounterBrokerInterceptor) listener).getCount() >= 2);
    }

    @Test
    public void testConnectionCreation() throws PulsarClientException {
        BrokerInterceptor listener = pulsar.getBrokerInterceptor();
        Assert.assertTrue(listener instanceof CounterBrokerInterceptor);
        pulsarClient.newProducer(Schema.BOOL).topic("test").create();
        pulsarClient.newConsumer(Schema.STRING).topic("test1").subscriptionName("test-sub").subscribe();
        // single connection for both producer and consumer
        Awaitility.await().until(() -> ((CounterBrokerInterceptor) listener).getConnectionCreationCount() == 1);
    }

    @Test
    public void testProducerCreation() throws PulsarClientException {
        BrokerInterceptor listener = pulsar.getBrokerInterceptor();
        Assert.assertTrue(listener instanceof CounterBrokerInterceptor);
        Assert.assertTrue(((CounterBrokerInterceptor)listener).getProducerCount() == 0);
        pulsarClient.newProducer(Schema.BOOL).topic("test").create();
        Awaitility.await().until(() -> ((CounterBrokerInterceptor) listener).getProducerCount() == 1);
    }

    @Test
    public void testConsumerCreation() throws PulsarClientException {
        BrokerInterceptor listener = pulsar.getBrokerInterceptor();
        Assert.assertTrue(listener instanceof CounterBrokerInterceptor);
        Assert.assertTrue(((CounterBrokerInterceptor)listener).getConsumerCount() == 0);
        pulsarClient.newConsumer(Schema.STRING).topic("test1").subscriptionName("test-sub").subscribe();
        Awaitility.await().until(() -> ((CounterBrokerInterceptor) listener).getConsumerCount() == 1);
    }

    @Test
    public void testBeforeSendMessage() throws PulsarClientException {
        BrokerInterceptor listener = pulsar.getBrokerInterceptor();
        Assert.assertTrue(listener instanceof CounterBrokerInterceptor);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
            .topic("test-before-send-message")
            .create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
            .topic("test-before-send-message")
            .subscriptionName("test")
            .subscribe();

        assertEquals(((CounterBrokerInterceptor)listener).getMessagePublishCount(),0);
        assertEquals(((CounterBrokerInterceptor)listener).getMessageDispatchCount(),0);
        producer.send("hello world");
        assertEquals(((CounterBrokerInterceptor)listener).getMessagePublishCount(),1);

        Message<String> msg = consumer.receive();

        assertEquals(msg.getValue(), "hello world");

        Awaitility.await().until(() -> ((CounterBrokerInterceptor) listener).getBeforeSendCount() == 1);
        Awaitility.await().until(() -> ((CounterBrokerInterceptor) listener).getMessageDispatchCount() == 1);
    }

    @Test
    public void asyncResponseFilterTest() throws Exception {
        Assert.assertTrue(pulsar.getBrokerInterceptor() instanceof CounterBrokerInterceptor);
        CounterBrokerInterceptor interceptor = (CounterBrokerInterceptor) pulsar.getBrokerInterceptor();
        interceptor.clearResponseList();

        OkHttpClient client = new OkHttpClient();
        String url = "http://127.0.0.1:" + conf.getWebServicePort().get() + "/admin/v3/test/asyncGet/my-topic/1000";
        final Request request = new Request.Builder()
                .url(url)
                .get()
                .build();
        Call call = client.newCall(request);
        CompletableFuture<Response> future = new CompletableFuture<>();
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                future.complete(response);
            }
        });
        future.get();
        Awaitility.await().until(() -> !interceptor.getResponseList().isEmpty());
        CounterBrokerInterceptor.ResponseEvent responseEvent = interceptor.getResponseList().get(0);
        Assert.assertEquals(responseEvent.getRequestUri(), "/admin/v3/test/asyncGet/my-topic/1000");

        Assert.assertEquals(responseEvent.getResponseStatus(),
                javax.ws.rs.core.Response.noContent().build().getStatus());
    }

    public void requestInterceptorFailedTest() {
        Set<String> allowedClusters = new HashSet<>();
        allowedClusters.add(configClusterName);
        TenantInfoImpl tenantInfo = new TenantInfoImpl(new HashSet<>(), allowedClusters);
        try {
            admin.tenants().createTenant("test-interceptor-failed-tenant", tenantInfo);
            Assert.fail("Create tenant because interceptor should fail");
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getHttpError(), "Create tenant failed");
        }

        try {
            admin.namespaces().createNamespace("public/test-interceptor-failed-namespace");
            Assert.fail("Create namespace because interceptor should fail");
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getHttpError(), "Create namespace failed");
        }

        try {
            admin.topics().createNonPartitionedTopic("persistent://public/default/test-interceptor-failed-topic");
            Assert.fail("Create topic because interceptor should fail");
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getHttpError(), "Create topic failed");
        }
    }

}

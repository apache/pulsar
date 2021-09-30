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
package org.apache.pulsar.client.cli;

import static org.testng.Assert.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PulsarClientToolTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testInitialization() throws InterruptedException, ExecutionException, PulsarAdminException {

        Properties properties = new Properties();
        properties.setProperty("serviceUrl", brokerUrl.toString());
        properties.setProperty("useTls", "false");

        String tenantName = UUID.randomUUID().toString();

        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant(tenantName, tenantInfo);

        String topicName = String.format("persistent://%s/ns/topic-scale-ns-0/topic", tenantName);

        int numberOfMessages = 10;

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();

        CompletableFuture<Void> future = new CompletableFuture<Void>();
        executor.execute(() -> {
            PulsarClientTool pulsarClientToolConsumer;
            try {
                pulsarClientToolConsumer = new PulsarClientTool(properties);
                String[] args = { "consume", "-t", "Exclusive", "-s", "sub-name", "-n",
                        Integer.toString(numberOfMessages), "--hex", "-r", "30", topicName };
                Assert.assertEquals(pulsarClientToolConsumer.run(args), 0);
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        Awaitility.await()
                .ignoreExceptions()
                .until(()->admin.topics().getSubscriptions(topicName).size() == 1);

        PulsarClientTool pulsarClientToolProducer = new PulsarClientTool(properties);

        String[] args = { "produce", "--messages", "Have a nice day", "-n", Integer.toString(numberOfMessages), "-r",
                "20", "-p", "key1=value1", "-p", "key2=value2", "-k", "partition_key", topicName };
        Assert.assertEquals(pulsarClientToolProducer.run(args), 0);

        future.get();
    }

    @Test(timeOut = 20000)
    public void testNonDurableSubscribe() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("serviceUrl", brokerUrl.toString());
        properties.setProperty("useTls", "false");

        final String topicName = "persistent://prop/ns-abc/test/topic-" + UUID.randomUUID().toString();

        int numberOfMessages = 10;
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                PulsarClientTool pulsarClientToolConsumer = new PulsarClientTool(properties);
                String[] args = {"consume", "-t", "Exclusive", "-s", "sub-name", "-n",
                        Integer.toString(numberOfMessages), "--hex", "-m", "NonDurable", "-r", "30", topicName};
                Assert.assertEquals(pulsarClientToolConsumer.run(args), 0);
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });

        // Make sure subscription has been created
        retryStrategically((test) -> {
            try {
                return admin.topics().getSubscriptions(topicName).size() == 1;
            } catch (Exception e) {
                return false;
            }
        }, 10, 500);

        assertEquals(admin.topics().getSubscriptions(topicName).size(), 1);
        PulsarClientTool pulsarClientToolProducer = new PulsarClientTool(properties);

        String[] args = {"produce", "--messages", "Have a nice day", "-n", Integer.toString(numberOfMessages), "-r",
                "20", "-p", "key1=value1", "-p", "key2=value2", "-k", "partition_key", topicName};
        Assert.assertEquals(pulsarClientToolProducer.run(args), 0);
        Assert.assertFalse(future.isCompletedExceptionally());
        future.get();

        Awaitility.await()
                .ignoreExceptions()
                .atMost(Duration.ofMillis(20000))
                .until(()->admin.topics().getSubscriptions(topicName).size() == 0);
    }

    @Test(timeOut = 60000)
    public void testDurableSubscribe() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("serviceUrl", brokerUrl.toString());
        properties.setProperty("useTls", "false");

        final String topicName = "persistent://prop/ns-abc/test/topic-" + UUID.randomUUID().toString();

        int numberOfMessages = 10;
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                PulsarClientTool pulsarClientToolConsumer = new PulsarClientTool(properties);
                String[] args = {"consume", "-t", "Exclusive", "-s", "sub-name", "-n",
                        Integer.toString(numberOfMessages), "--hex", "-m", "Durable", "-r", "30", topicName};
                Assert.assertEquals(pulsarClientToolConsumer.run(args), 0);
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        // Make sure subscription has been created
        Awaitility.await()
                .atMost(Duration.ofMillis(60000))
                .ignoreExceptions()
                .until(() -> admin.topics().getSubscriptions(topicName).size() == 1);

        PulsarClientTool pulsarClientToolProducer = new PulsarClientTool(properties);

        String[] args = {"produce", "--messages", "Have a nice day", "-n", Integer.toString(numberOfMessages), "-r",
                "20", "-p", "key1=value1", "-p", "key2=value2", "-k", "partition_key", topicName};
        Assert.assertEquals(pulsarClientToolProducer.run(args), 0);

        try {
            future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.fail("consumer was unable to receive messages", e);
        }
    }

    @Test(timeOut = 20000)
    public void testEncryption() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("serviceUrl", brokerUrl.toString());
        properties.setProperty("useTls", "false");

        final String topicName = "persistent://prop/ns-abc/test/topic-" + UUID.randomUUID().toString();
        final String keyUriBase = "file:../pulsar-broker/src/test/resources/certificate/";
        final int numberOfMessages = 10;

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                PulsarClientTool pulsarClientToolConsumer = new PulsarClientTool(properties);
                String[] args = {"consume", "-s", "sub-name", "-n", Integer.toString(numberOfMessages), "-ekv",
                        keyUriBase + "private-key.client-rsa.pem", topicName};
                Assert.assertEquals(pulsarClientToolConsumer.run(args), 0);
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });

        // Make sure subscription has been created
        Awaitility.await()
                .atMost(Duration.ofMillis(20000))
                .ignoreExceptions()
                .until(() -> admin.topics().getSubscriptions(topicName).size() == 1);

        PulsarClientTool pulsarClientToolProducer = new PulsarClientTool(properties);
        String[] args = {"produce", "-m", "Have a nice day", "-n", Integer.toString(numberOfMessages), "-ekn",
                "my-app-key", "-ekv", keyUriBase + "public-key.client-rsa.pem", topicName};
        Assert.assertEquals(pulsarClientToolProducer.run(args), 0);

        try {
            future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.fail("consumer was unable to decrypt messages", e);
        }
    }

}

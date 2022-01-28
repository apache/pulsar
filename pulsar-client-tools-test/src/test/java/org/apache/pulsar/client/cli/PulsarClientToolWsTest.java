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

import java.time.Duration;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class PulsarClientToolWsTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.resetConfig();
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testWebSocketNonDurableSubscriptionMode() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("serviceUrl", brokerUrl.toString());
        properties.setProperty("useTls", "false");

        final String topicName = "persistent://my-property/my-ns/test/topic-" + UUID.randomUUID();

        int numberOfMessages = 10;
        {
            @Cleanup("shutdown")
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
            Awaitility.await()
                    .pollInterval(Duration.ofMillis(200))
                    .ignoreExceptions().untilAsserted(() -> {
                Assert.assertEquals(admin.topics().getSubscriptions(topicName).size(), 1);
            });

            PulsarClientTool pulsarClientToolProducer = new PulsarClientTool(properties);

            String[] args = {"produce", "--messages", "Have a nice day", "-n", Integer.toString(numberOfMessages), "-r",
                    "20", "-p", "key1=value1", "-p", "key2=value2", "-k", "partition_key", topicName};
            Assert.assertEquals(pulsarClientToolProducer.run(args), 0);
            future.get();
            Assert.assertFalse(future.isCompletedExceptionally());
        }

        Awaitility.await()
                .ignoreExceptions().untilAsserted(() -> {
            Assert.assertEquals(admin.topics().getSubscriptions(topicName).size(), 0);
        });
    }

    @Test(timeOut = 30000)
    public void testWebSocketDurableSubscriptionMode() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("serviceUrl", brokerUrl.toString());
        properties.setProperty("useTls", "false");

        final String topicName = "persistent://my-property/my-ns/test/topic-" + UUID.randomUUID();

        int numberOfMessages = 10;
        {
            @Cleanup("shutdown")
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
                    .pollInterval(Duration.ofMillis(200))
                    .ignoreExceptions().untilAsserted(() -> {
                Assert.assertEquals(admin.topics().getSubscriptions(topicName).size(), 1);
            });

            PulsarClientTool pulsarClientToolProducer = new PulsarClientTool(properties);

            String[] args = {"produce", "--messages", "Have a nice day", "-n", Integer.toString(numberOfMessages), "-r",
                    "20", "-p", "key1=value1", "-p", "key2=value2", "-k", "partition_key", topicName};
            Assert.assertEquals(pulsarClientToolProducer.run(args), 0);
            future.get();
            Assert.assertFalse(future.isCompletedExceptionally());
        }

        //wait for close
        Thread.sleep(2000);
        List<String> subscriptions = admin.topics().getSubscriptions(topicName);
        Assert.assertNotNull(subscriptions);
        Assert.assertEquals(subscriptions.size(), 1);
    }
}
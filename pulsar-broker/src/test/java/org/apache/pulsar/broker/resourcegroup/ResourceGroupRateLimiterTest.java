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
package org.apache.pulsar.broker.resourcegroup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.Cleanup;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.RateLimiter;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ResourceGroupRateLimiterTest extends BrokerTestBase {

    final String rgName = "testRG";
    org.apache.pulsar.common.policies.data.ResourceGroup testAddRg =
    new org.apache.pulsar.common.policies.data.ResourceGroup();
    final String namespaceName = "prop/ns-abc";
    final String persistentTopicString = "persistent://prop/ns-abc/test-topic";
    final int MESSAGE_SIZE = 10;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setMaxPendingPublishRequestsPerConnection(0);
        super.baseSetup();
        prepareData();

    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    public void createResourceGroup(String rgName, org.apache.pulsar.common.policies.data.ResourceGroup rg) throws PulsarAdminException {
        admin.resourcegroups().createResourceGroup(rgName, rg);

        Awaitility.await().untilAsserted(() -> {
            final org.apache.pulsar.broker.resourcegroup.ResourceGroup resourceGroup = pulsar
                .getResourceGroupServiceManager().resourceGroupGet(rgName);
            assertNotNull(resourceGroup);
            assertEquals(rgName, resourceGroup.resourceGroupName);
        });

    }

    public void deleteResourceGroup(String rgName) throws PulsarAdminException {
        admin.resourcegroups().deleteResourceGroup(rgName);
        Awaitility.await().atMost(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName)));
    }

    private void testRateLimit() throws PulsarAdminException, PulsarClientException,
      InterruptedException, ExecutionException, TimeoutException {
        createResourceGroup(rgName, testAddRg);
        admin.namespaces().setNamespaceResourceGroup(namespaceName, rgName);

        Awaitility.await().untilAsserted(() ->
          assertNotNull(pulsar.getResourceGroupServiceManager()
            .getNamespaceResourceGroup(NamespaceName.get(namespaceName))));

        Awaitility.await().untilAsserted(() ->
          assertNotNull(pulsar.getResourceGroupServiceManager()
            .resourceGroupGet(rgName).getResourceGroupPublishLimiter()));

        Producer<byte[]> producer = null;
        try {
            producer = pulsarClient.newProducer()
              .topic(persistentTopicString)
              .create();
        } catch (PulsarClientException p) {
            final String errMesg = String.format("Got exception while building producer: ex=%s", p.getMessage());
            Assert.fail(errMesg);
        }

        MessageId messageId = null;
        try {
            // first will be success
            messageId = producer.sendAsync(new byte[MESSAGE_SIZE]).get(100, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(messageId);
        } catch (TimeoutException e) {
            Assert.fail("should not fail");
        }

        // Second message should fail with timeout.
        Producer<byte[]> finalProducer = producer;
        Assert.assertThrows(TimeoutException.class, () -> {
            finalProducer.sendAsync(new byte[MESSAGE_SIZE]).get(500, TimeUnit.MILLISECONDS);});

        // In the next interval, the above message will be accepted. Wait for one more second (total 2s),
        // to publish the next message.
        Thread.sleep(2000);

        try {
            // third one should succeed
            messageId = producer.sendAsync(new byte[MESSAGE_SIZE]).get(100, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(messageId);
        } catch (TimeoutException e) {
            Assert.fail("should not fail");
        }

        // Now detach the namespace
        admin.namespaces().removeNamespaceResourceGroup(namespaceName);
        deleteResourceGroup(rgName);

        // No rate limits should be applied.
        for (int i = 0; i < 5; i++) {
            messageId = producer.sendAsync(new byte[MESSAGE_SIZE]).get(100, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(messageId);
        }
        producer.close();
    }

    @Test
    public void testResourceGroupPublishRateLimit() throws Exception {
        testRateLimit();
        testRateLimit();
    }

    @Test
    public void testWithConcurrentUpdate() throws Exception {
        cleanup();
        setup();
        createResourceGroup(rgName, testAddRg);
        admin.namespaces().setNamespaceResourceGroup(namespaceName, rgName);

        Awaitility.await().untilAsserted(() ->
                assertNotNull(pulsar.getResourceGroupServiceManager()
                        .getNamespaceResourceGroup(NamespaceName.get(namespaceName))));

        Awaitility.await().untilAsserted(() ->
                assertNotNull(pulsar.getResourceGroupServiceManager()
                        .resourceGroupGet(rgName).getResourceGroupPublishLimiter()));

        ResourceGroupPublishLimiter resourceGroupPublishLimiter = Mockito.spy(pulsar.getResourceGroupServiceManager()
                .resourceGroupGet(rgName).getResourceGroupPublishLimiter());

        AtomicBoolean blocking = new AtomicBoolean(false);
        BiFunction<Function<Long, Boolean>, Long, Boolean> blockFunc = (function, acquirePermit) -> {
            blocking.set(true);
            while (blocking.get()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return function.apply(acquirePermit);
        };

        Mockito.doAnswer(invocation -> {
            RateLimiter publishRateLimiterOnMessage =
                    (RateLimiter) FieldUtils.readDeclaredField(resourceGroupPublishLimiter,
                            "publishRateLimiterOnMessage", true);
            RateLimiter publishRateLimiterOnByte =
                    (RateLimiter) FieldUtils.readDeclaredField(resourceGroupPublishLimiter,
                            "publishRateLimiterOnByte", true);
            int numbers = invocation.getArgument(0);
            long bytes = invocation.getArgument(1);
            return (publishRateLimiterOnMessage == null || publishRateLimiterOnMessage.tryAcquire(numbers))
            && (publishRateLimiterOnByte == null || blockFunc.apply(publishRateLimiterOnByte::tryAcquire, bytes));
        }).when(resourceGroupPublishLimiter).tryAcquire(Mockito.anyInt(), Mockito.anyLong());

        ConcurrentHashMap resourceGroupsMap =
                (ConcurrentHashMap) FieldUtils.readDeclaredField(pulsar.getResourceGroupServiceManager(),
                        "resourceGroupsMap", true);
        FieldUtils.writeDeclaredField(resourceGroupsMap.get(rgName), "resourceGroupPublishLimiter",
                resourceGroupPublishLimiter, true);
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic(namespaceName + "/test-topic")
                    .create();

        CompletableFuture<MessageId> sendFuture = producer.sendAsync(new byte[MESSAGE_SIZE]);

        Awaitility.await().untilAsserted(() -> Assert.assertTrue(blocking.get()));

        testAddRg.setPublishRateInBytes(Long.valueOf(MESSAGE_SIZE) + 1);
        admin.resourcegroups().updateResourceGroup(rgName, testAddRg);
        blocking.set(false);

        sendFuture.join();

        // Now detach the namespace
        admin.namespaces().removeNamespaceResourceGroup(namespaceName);
        deleteResourceGroup(rgName);
    }


    private void prepareData() {
        testAddRg.setPublishRateInBytes(Long.valueOf(MESSAGE_SIZE));
        testAddRg.setPublishRateInMsgs(1);
        testAddRg.setDispatchRateInMsgs(-1);
        testAddRg.setDispatchRateInBytes(Long.valueOf(-1));
    }
}

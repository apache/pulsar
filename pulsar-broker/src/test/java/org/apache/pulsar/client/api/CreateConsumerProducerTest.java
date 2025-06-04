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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;

import java.util.UUID;

import org.apache.pulsar.tests.ThreadDumpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class CreateConsumerProducerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(CreateConsumerProducerTest.class);
    private PulsarClient pulsarClientWithUnavailableNodes;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        // Create a Pulsar client with some unavailable nodes
        String serviceUrl = pulsar.getBrokerServiceUrl() + ",127.0.0.1:5678,127.0.0.1:6789";
        pulsarClientWithUnavailableNodes = newPulsarClient(serviceUrl, 0);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanupAfterMethod() throws Exception {
        try {
            pulsar.getConfiguration().setForceDeleteTenantAllowed(true);
            pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);

            for (String tenant : admin.tenants().getTenants()) {
                for (String namespace : admin.namespaces().getNamespaces(tenant)) {
                    deleteNamespaceWithRetry(namespace, true);
                }
                admin.tenants().deleteTenant(tenant, true);
            }

            for (String cluster : admin.clusters().getClusters()) {
                admin.clusters().deleteCluster(cluster);
            }

            pulsar.getConfiguration().setForceDeleteTenantAllowed(false);
            pulsar.getConfiguration().setForceDeleteNamespaceAllowed(false);
            super.producerBaseSetup();
        } catch (Exception | AssertionError e) {
            log.warn("Failed to clean up state. Restarting broker.", e);
            log.warn("Thread dump:\n{}", ThreadDumpUtil.buildThreadDiagnosticString());
            cleanup();
            setup();
        }
    }

    @DataProvider
    public static Object[][] variationsForExpectedPos() {
        return new Object[][] {
                // batching / start-inclusive / num-of-messages
                {true, true, 10 },
                {true, false, 10 },
                {false, true, 10 },
                {false, false, 10 },

                {true, true, 100 },
                {true, false, 100 },
                {false, true, 100 },
                {false, false, 100 },
        };
    }

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @DataProvider(name = "ackReceiptEnabledAndSubscriptionTypes")
    public Object[][] ackReceiptEnabledAndSubscriptionTypes() {
        return new Object[][] {
                {true, SubscriptionType.Shared},
                {true, SubscriptionType.Key_Shared},
                {false, SubscriptionType.Shared},
                {false, SubscriptionType.Key_Shared},
        };
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (pulsarClientWithUnavailableNodes != null) {
            pulsarClientWithUnavailableNodes.close();
        }
    }

    @Test
    public void testCreateConsumerWithUnavailableBrokerNodes() throws Exception {
        String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        int createCount = 10;
        int successCount = 0;
        for (int i = 0; i < createCount; i++) {
            String subName = "my-sub" + UUID.randomUUID();
            try {
                Consumer<byte[]> consumer = pulsarClientWithUnavailableNodes.newConsumer()
                    .subscriptionMode(SubscriptionMode.Durable)
                    .topic(topic).receiverQueueSize(1).subscriptionName(subName).subscribe();
                consumer.close();
                successCount++;
            } catch(PulsarClientException e) {
                log.warn("Failed to create subscription {} for topic {}: {}", subName, topic, e.getMessage());
            }
        }
        assertEquals(createCount, successCount,
                "Expected all subscription creations to succeed, but only " + successCount + " succeeded.");
    }

    @Test
    public void testCreateProducerWithUnavailableBrokerNodes() throws Exception {
        String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        int createCount = 10;
        int successCount = 0;
        for (int i = 0; i < createCount; i++) {
            try {
                Producer<byte[]> producer = pulsarClientWithUnavailableNodes.newProducer()
                .topic(topic)
                .create();
                producer.close();
                successCount++;
            } catch(PulsarClientException e) {
                log.warn("Failed to create producer for topic {}: {}", topic, e.getMessage());
            }
        }
        assertEquals(createCount, successCount,
                "Expected all producer creations to succeed, but only " + successCount + " succeeded.");
    }
}

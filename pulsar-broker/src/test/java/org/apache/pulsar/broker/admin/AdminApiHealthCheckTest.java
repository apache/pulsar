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
package org.apache.pulsar.broker.admin;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.compaction.Compactor;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.springframework.util.CollectionUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Test(groups = "broker-admin")
@Slf4j
public class AdminApiHealthCheckTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        resetConfig();
        super.internalSetup();
        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(
                Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("pulsar", tenantInfo);
        admin.namespaces().createNamespace("pulsar/system", Sets.newHashSet("test"));
        admin.tenants().createTenant("public", tenantInfo);
        admin.namespaces().createNamespace("public/default", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testHealthCheckup() throws Exception {
        final int times = 30;
        CompletableFuture<Void> future = new CompletableFuture<>();
        pulsar.getExecutor().execute(() -> {
            try {
                for (int i = 0; i < times; i++) {
                    admin.brokers().healthcheck();
                }
                future.complete(null);
            }catch (PulsarAdminException e) {
                future.completeExceptionally(e);
            }
        });
        for (int i = 0; i < times; i++) {
            admin.brokers().healthcheck();
        }
        // To ensure we don't have any subscription
        final String testHealthCheckTopic = String.format("persistent://pulsar/test/localhost:%s/healthcheck",
                pulsar.getConfig().getWebServicePort().get());
        Awaitility.await().untilAsserted(() -> {
            Assert.assertFalse(future.isCompletedExceptionally());
        });
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(CollectionUtils.isEmpty(admin.topics()
                        .getSubscriptions(testHealthCheckTopic).stream()
                        // All system topics are using compaction, even though is not explicitly set in the policies.
                        .filter(v -> !v.equals(Compactor.COMPACTION_SUBSCRIPTION))
                        .collect(Collectors.toList())
                ))
        );
    }

    @Test
    public void testHealthCheckupV1() throws Exception {
        final int times = 30;
        CompletableFuture<Void> future = new CompletableFuture<>();
        pulsar.getExecutor().execute(() -> {
            try {
                for (int i = 0; i < times; i++) {
                    admin.brokers().healthcheck(TopicVersion.V1);
                }
                future.complete(null);
            }catch (PulsarAdminException e) {
                future.completeExceptionally(e);
            }
        });
        for (int i = 0; i < times; i++) {
            admin.brokers().healthcheck(TopicVersion.V1);
        }
        final String testHealthCheckTopic = String.format("persistent://pulsar/test/localhost:%s/healthcheck",
                pulsar.getConfig().getWebServicePort().get());
        Awaitility.await().untilAsserted(() -> {
            Assert.assertFalse(future.isCompletedExceptionally());
        });
        // To ensure we don't have any subscription
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(CollectionUtils.isEmpty(admin.topics()
                        .getSubscriptions(testHealthCheckTopic).stream()
                        // All system topics are using compaction, even though is not explicitly set in the policies.
                        .filter(v -> !v.equals(Compactor.COMPACTION_SUBSCRIPTION))
                        .collect(Collectors.toList())
                ))
        );
    }

    @Test
    public void testHealthCheckupV2() throws Exception {
        final int times = 30;
        CompletableFuture<Void> future = new CompletableFuture<>();
        pulsar.getExecutor().execute(() -> {
            try {
                for (int i = 0; i < times; i++) {
                    admin.brokers().healthcheck(TopicVersion.V2);
                }
                future.complete(null);
            }catch (PulsarAdminException e) {
                future.completeExceptionally(e);
            }
        });
        for (int i = 0; i < times; i++) {
            admin.brokers().healthcheck(TopicVersion.V2);
        }
        final String testHealthCheckTopic = String.format("persistent://pulsar/localhost:%s/healthcheck",
                pulsar.getConfig().getWebServicePort().get());
        Awaitility.await().untilAsserted(() -> {
            Assert.assertFalse(future.isCompletedExceptionally());
        });
        // To ensure we don't have any subscription
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(CollectionUtils.isEmpty(admin.topics()
                        .getSubscriptions(testHealthCheckTopic).stream()
                        // All system topics are using compaction, even though is not explicitly set in the policies.
                        .filter(v -> !v.equals(Compactor.COMPACTION_SUBSCRIPTION))
                        .collect(Collectors.toList())
                ))
        );
    }
}

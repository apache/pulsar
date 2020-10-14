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
package org.apache.pulsar.client.api;

import com.google.common.collect.Sets;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TenantTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMaxTenant() throws Exception {
        conf.setMaxTenants(2);
        super.internalSetup();
        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("testTenant1", tenantInfo);
        admin.tenants().createTenant("testTenant2", tenantInfo);
        try {
            admin.tenants().createTenant("testTenant3", tenantInfo);
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
            Assert.assertEquals(e.getHttpError(), "Exceed the maximum number of tenants");
        }
        //unlimited
        conf.setMaxTenants(0);
        super.internalSetup();
        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        for (int i = 0; i < 10; i++) {
            admin.tenants().createTenant("testTenant-unlimited" + i, tenantInfo);
        }
    }
    
    @Test
    public void testExceedMaxTenant() throws Exception {
        conf.setMaxTenants(2);
        super.internalSetup();
        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        ExecutorService executor = new ThreadPoolExecutor(10, 10, 30
                , TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new DefaultThreadFactory("testExceedMaxTenant"));
        CountDownLatch countDownLatch = new CountDownLatch(10);
        AtomicInteger counter = new AtomicInteger(0);
        for (int i = 0; i < 10; i++) {
            executor.execute(() -> {
                try {
                    admin.tenants().createTenant("tenant" + UUID.randomUUID(), tenantInfo);
                    counter.getAndIncrement();
                } catch (PulsarAdminException ignored) {
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        Assert.assertTrue(counter.get() > 2);
        executor.shutdownNow();
    }

}

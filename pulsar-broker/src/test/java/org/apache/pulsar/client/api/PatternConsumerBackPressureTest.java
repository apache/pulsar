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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class PatternConsumerBackPressureTest extends MockedPulsarServiceBaseTest {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        isTcpLookup = true;
        conf.setEnableBrokerSideSubscriptionPatternEvaluation(false);
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        conf.setConnectionMaxPendingWriteBytes(5 * 1024 * 1024);
    }

    @Test(timeOut = 60 * 1000)
    public void testBrokerOOM() throws PulsarAdminException, InterruptedException {
        final int topicCount = 8192;
        final int requests = 10_000;
        final String topicName = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topicName, topicCount);
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        final PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        final AtomicInteger success = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(requests);
        for (int i = 0; i < requests; i++) {
            executorService.execute(() -> {
                pulsarClientImpl.getLookup()
                    .getTopicsUnderNamespace(NamespaceName.get("public", "default"),
                            CommandGetTopicsOfNamespace.Mode.PERSISTENT, ".*", "")
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            success.incrementAndGet();
                        }
                        log.info("latch-count: {}, succeed: {}", latch.getCount(), success.get());
                        latch.countDown();
                    });
            });
        }
        latch.await();
        Assert.assertEquals(success.get(), requests);
    }
}

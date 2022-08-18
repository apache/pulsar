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

import com.google.common.collect.Sets;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ClientCnxTest extends MockedPulsarServiceBaseTest {

    public static final String CLUSTER_NAME = "test";
    public static final String TENANT = "tnx";
    public static final String NAMESPACE = TENANT + "/ns1";
    public static String persistentTopic = "persistent://" + NAMESPACE + "/test";
    ExecutorService executorService = Executors.newFixedThreadPool(20);

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(TENANT,
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        this.executorService.shutdown();
    }

    @Test
    public void testRemoveAndHandlePendingRequestInCnx() throws Exception {

        String subName = "sub";
        int operationTimes = 5000;
        CountDownLatch countDownLatch = new CountDownLatch(operationTimes);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(persistentTopic)
                .subscriptionName(subName)
                .subscribe();

        new Thread(() -> {
            for (int i = 0; i < operationTimes; i++) {
                executorService.submit(() -> {
                    consumer.getLastMessageIdAsync().whenComplete((ignore, exception) -> {
                        countDownLatch.countDown();
                    });
                });
            }
        }).start();

        for (int i = 0; i < operationTimes; i++) {
            ClientCnx cnx = ((ConsumerImpl<?>) consumer).getClientCnx();
            if (cnx != null) {
                ChannelHandlerContext context = cnx.ctx();
                if (context != null) {
                    cnx.ctx().close();
                }
            }
        }

        Awaitility.await().until(() -> {
            countDownLatch.await();
            return true;
        });

    }
}

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
package org.apache.pulsar.testclient;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Slf4j
public class PerformanceProducerTest extends MockedPulsarServiceBaseTest {
    private final String testTenant = "prop-xyz";
    private final String testNamespace = "ns1";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/test-";
    private final AtomicInteger lastExitCode = new AtomicInteger(0);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        PerfClientUtils.setExitProcedure(code -> {
            log.error("JVM exit code is {}", code);
            if (code != 0) {
                throw new RuntimeException("JVM should exit with code " + code);
            }
        });
        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(testTenant, tenantInfo);
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        int exitCode = lastExitCode.get();
        if (exitCode != 0) {
            fail("Unexpected JVM exit code "+exitCode);
        }
    }

    @Test(timeOut = 20000)
    public void testMsgKey() throws Exception {
        String argString = "%s -r 10 -u %s -m 500";
        String topic = testTopic + UUID.randomUUID().toString();
        String args = String.format(argString, topic, pulsar.getBrokerServiceUrl());
        Thread thread = new Thread(() -> {
            try {
                PerformanceProducer.main(args.split(" "));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topic).subscriptionName("sub-1")
                .subscriptionType(SubscriptionType.Key_Shared).subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topic).subscriptionName("sub-1")
                .subscriptionType(SubscriptionType.Key_Shared).subscribe();

        thread.start();

        int count1 = 0;
        int count2 = 0;
        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = consumer1.receive(1, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            count1++;
            consumer1.acknowledge(message);
        }
        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = consumer2.receive(1, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            count2++;
            consumer2.acknowledge(message);
        }
        //in key_share mode, only one consumer can get msg
        Assert.assertTrue(count1 == 0 || count2 == 0);

        consumer1.close();
        consumer2.close();
        thread.interrupt();
        while (thread.isAlive()) {
            Thread.sleep(1000);
        }

        //use msg key generator,so every consumer can get msg
        String newArgString = "%s -r 10 -u %s -m 500 -mk autoIncrement";
        String topic2 = testTopic + UUID.randomUUID().toString();
        String newArgs = String.format(newArgString, topic2, pulsar.getBrokerServiceUrl());
        Thread thread2 = new Thread(() -> {
            try {
                PerformanceProducer.main(newArgs.split(" "));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Consumer<byte[]> newConsumer1 = pulsarClient.newConsumer().topic(topic2).subscriptionName("sub-2")
                .subscriptionType(SubscriptionType.Key_Shared).subscribe();
        Consumer<byte[]> newConsumer2 = pulsarClient.newConsumer().topic(topic2).subscriptionName("sub-2")
                .subscriptionType(SubscriptionType.Key_Shared).subscribe();

        thread2.start();

        Awaitility.await()
                .untilAsserted(() -> {
                    Message<byte[]> message = newConsumer1.receive(1, TimeUnit.SECONDS);
                    if (message != null) {
                        newConsumer1.acknowledge(message);
                    }
                    assertNotNull(message);
                });

        Awaitility.await()
                .untilAsserted(() -> {
                    Message<byte[]> message = newConsumer2.receive(1, TimeUnit.SECONDS);
                    if (message != null) {
                        newConsumer2.acknowledge(message);
                    }
                    assertNotNull(message);
                });

        thread2.interrupt();
        newConsumer1.close();
        newConsumer2.close();
    }

    @Test(timeOut = 20000)
    public void testCreatePartitions() throws Exception {
        String argString = "%s -r 10 -u %s -au %s -m 5 -np 10";
        String topic = testTopic + UUID.randomUUID().toString();
        String args = String.format(argString, topic, pulsar.getBrokerServiceUrl(), pulsar.getWebServiceAddress());
        Thread thread = new Thread(() -> {
            try {
                PerformanceProducer.main(args.split(" "));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        thread.join();
        Assert.assertEquals(10, pulsar.getAdminClient().topics().getPartitionedTopicMetadata(topic).partitions);
    }

    @Test
    public void testNotExistIMessageFormatter() {
        IMessageFormatter msgFormatter = PerformanceProducer.getMessageFormatter("org.apache.pulsar.testclient.NonExistentFormatter");
        Assert.assertNull(msgFormatter);
    }

    @Test
    public void testDefaultIMessageFormatter() {
        IMessageFormatter msgFormatter = PerformanceProducer.getMessageFormatter("org.apache.pulsar.testclient.DefaultMessageFormatter");
        Assert.assertTrue(msgFormatter instanceof DefaultMessageFormatter);
    }

    @Test
    public void testMaxOutstanding() throws Exception {
        String argString = "%s -r 10 -u %s -au %s -m 5 -o 10000";
        String topic = testTopic + UUID.randomUUID().toString();
        String args = String.format(argString, topic, pulsar.getBrokerServiceUrl(), pulsar.getWebServiceAddress());
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub")
                .subscriptionType(SubscriptionType.Key_Shared).subscribe();
        new Thread(() -> {
            try {
                PerformanceProducer.main(args.split(" "));
            } catch (Exception e) {
                log.error("Failed to start perf producer");
            }
        }).start();
        Awaitility.await()
                .untilAsserted(() -> {
                    Message<byte[]> message = consumer.receive(3, TimeUnit.SECONDS);
                    assertNotNull(message);
                });
        consumer.close();
    }
}

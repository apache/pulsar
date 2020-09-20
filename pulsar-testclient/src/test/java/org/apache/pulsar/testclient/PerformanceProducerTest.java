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
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PerformanceProducerTest extends MockedPulsarServiceBaseTest {
    private final String testTenant = "prop-xyz";
    private final String testNamespace = "ns1";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/test-";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        // Setup namespaces
        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(testTenant, tenantInfo);
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
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
        thread.start();
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topic).subscriptionName("sub-1")
                .subscriptionType(SubscriptionType.Key_Shared).subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topic).subscriptionName("sub-1")
                .subscriptionType(SubscriptionType.Key_Shared).subscribe();

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
        thread2.start();

        Consumer newConsumer1 = pulsarClient.newConsumer().topic(topic2).subscriptionName("sub-2")
                .subscriptionType(SubscriptionType.Key_Shared).subscribe();
        Consumer newConsumer2 = pulsarClient.newConsumer().topic(topic2).subscriptionName("sub-2")
                .subscriptionType(SubscriptionType.Key_Shared).subscribe();
        count1 = 0;
        count2 = 0;
        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = newConsumer1.receive(1, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            count1++;
            newConsumer1.acknowledge(message);
        }
        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = newConsumer2.receive(1, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            count2++;
            newConsumer2.acknowledge(message);
        }

        Assert.assertTrue(count1 > 0 && count2 > 0);
        thread2.interrupt();
        newConsumer1.close();
        newConsumer2.close();
    }
}

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

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.PulsarClientException.InvalidConfigurationException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ConsumerConfigurationTest extends MockedPulsarServiceBaseTest {
    private static String persistentTopic = "persistent://my-property/use/my-ns/persist";
    private static String nonPersistentTopic = "non-persistent://my-property/use/my-ns/nopersist";

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testReadCompactPersistentExclusive() throws Exception {
        pulsarClient.newConsumer().topic(persistentTopic).subscriptionName("sub1").readCompacted(true)
                .subscriptionType(SubscriptionType.Exclusive).subscribe().close();
    }

    @Test
    public void testReadCompactPersistentFailover() throws Exception {
        pulsarClient.newConsumer().topic(persistentTopic).subscriptionName("sub1").readCompacted(true)
                .subscriptionType(SubscriptionType.Failover).subscribe().close();
    }

    @Test(expectedExceptions = InvalidConfigurationException.class)
    public void testReadCompactPersistentShared() throws Exception {
        pulsarClient.newConsumer().topic(persistentTopic).subscriptionName("sub1").readCompacted(true)
                .subscriptionType(SubscriptionType.Shared).subscribe().close();
    }

    @Test(expectedExceptions = InvalidConfigurationException.class)
    public void testReadCompactNonPersistentExclusive() throws Exception {
        pulsarClient.newConsumer().topic(nonPersistentTopic).subscriptionName("sub1").readCompacted(true)
                .subscriptionType(SubscriptionType.Exclusive).subscribe().close();
    }
}

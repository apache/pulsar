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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.PulsarClientException.InvalidConfigurationException;
import org.apache.pulsar.client.api.SubscriptionType;

import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumerConfigurationTest extends MockedPulsarServiceBaseTest {
    private static final Logger log = LoggerFactory.getLogger(ConsumerConfigurationTest.class);
    private static String persistentTopic = "persistent://my-property/use/my-ns/persist";
    private static String nonPersistentTopic = "non-persistent://my-property/use/my-ns/nopersist";

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use",
                new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
        admin.properties().createProperty("my-property",
                new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testReadCompactPersistentExclusive() throws Exception {
        ConsumerConfiguration conf = new ConsumerConfiguration().setReadCompacted(true);
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        pulsarClient.subscribe(persistentTopic, "sub1", conf).close();
    }

    @Test
    public void testReadCompactPersistentFailover() throws Exception {
        ConsumerConfiguration conf = new ConsumerConfiguration().setReadCompacted(true);
        conf.setSubscriptionType(SubscriptionType.Failover);
        pulsarClient.subscribe(persistentTopic, "sub1", conf).close();
    }

    @Test(expectedExceptions=InvalidConfigurationException.class)
    public void testReadCompactPersistentShared() throws Exception {
        ConsumerConfiguration conf = new ConsumerConfiguration().setReadCompacted(true);
        conf.setSubscriptionType(SubscriptionType.Shared);
        pulsarClient.subscribe(persistentTopic, "sub1", conf).close();
    }

    @Test(expectedExceptions=InvalidConfigurationException.class)
    public void testReadCompactNonPersistentExclusive() throws Exception {
        ConsumerConfiguration conf = new ConsumerConfiguration().setReadCompacted(true);
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        pulsarClient.subscribe(nonPersistentTopic, "sub1", conf).close();
    }
}

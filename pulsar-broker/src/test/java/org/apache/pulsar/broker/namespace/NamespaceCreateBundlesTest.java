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
package org.apache.pulsar.broker.namespace;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.Policies;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceCreateBundlesTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setDefaultNumberOfNamespaceBundles(16);
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testCreateNamespaceWithDefaultBundles() throws Exception {
        String namespaceName = "prop/" + UUID.randomUUID().toString();

        admin.namespaces().createNamespace(namespaceName);

        Policies policies = admin.namespaces().getPolicies(namespaceName);
        assertEquals(policies.bundles.getNumBundles(), 16);
        assertEquals(policies.bundles.getBoundaries().size(), 17);
    }

    @Test
    public void testSplitBundleUpdatesLocalPoliciesWithoutOverwriting() throws Exception {
        String namespaceName = "prop/" + UUID.randomUUID().toString();
        String topicName = "persistent://" + namespaceName + "/my-topic5";

        admin.namespaces().createNamespace(namespaceName);

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName).sendTimeout(1,
                TimeUnit.SECONDS);

        Producer<byte[]> producer = producerBuilder.create();

        String bundle = admin.lookups().getBundleRange(topicName);
        BookieAffinityGroupData bookieAffinityGroup = BookieAffinityGroupData.builder()
                .bookkeeperAffinityGroupPrimary("test")
                .build();
        admin.namespaces().setBookieAffinityGroup(namespaceName, bookieAffinityGroup);
        admin.namespaces().splitNamespaceBundle(namespaceName, bundle, false, null);
        assertNotNull(admin.namespaces().getBookieAffinityGroup(namespaceName));
        producer.close();
    }
}

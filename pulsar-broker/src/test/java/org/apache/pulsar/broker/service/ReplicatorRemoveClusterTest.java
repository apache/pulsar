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
package org.apache.pulsar.broker.service;

import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Starts 3 brokers that are in 3 different clusters
 */
@Test(groups = "broker")
public class ReplicatorRemoveClusterTest extends ReplicatorTestBase {

    protected String methodName;

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
        admin1.namespaces().removeBacklogQuota("pulsar/ns");
        admin1.namespaces().removeBacklogQuota("pulsar/ns1");
        admin1.namespaces().removeBacklogQuota("pulsar/global/ns");
    }

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @DataProvider(name = "partitionedTopic")
    public Object[][] partitionedTopicProvider() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }


    @Test
    public void testRemoveClusterFromNamespace() throws Exception {
        admin1.tenants().createTenant("pulsar1",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"),
                        Sets.newHashSet("r1", "r2", "r3")));

        admin1.namespaces().createNamespace("pulsar1/ns1", Sets.newHashSet("r1", "r2", "r3"));

        PulsarClient repClient1 = pulsar1.getBrokerService().getReplicationClient("r3",
                pulsar1.getBrokerService().pulsar().getPulsarResources().getClusterResources()
                .getCluster("r3"));
        Assert.assertNotNull(repClient1);
        Assert.assertFalse(repClient1.isClosed());

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String topicName = "persistent://pulsar1/ns1/testRemoveClusterFromNamespace-" + UUID.randomUUID();

        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .create();

        producer.send("Pulsar".getBytes());

        producer.close();
        client.close();

        Replicator replicator = pulsar1.getBrokerService().getTopicReference(topicName)
                .get().getReplicators().get("r3");

        Awaitility.await().untilAsserted(() -> Assert.assertTrue(replicator.isConnected()));

        admin1.clusters().deleteCluster("r3");

        Awaitility.await().untilAsserted(() -> Assert.assertFalse(replicator.isConnected()));
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(repClient1.isClosed()));

        Awaitility.await().untilAsserted(() -> Assert.assertNull(
                pulsar1.getBrokerService().getReplicationClients().get("r3")));
    }
}

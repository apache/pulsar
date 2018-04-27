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

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.google.common.collect.Sets;

public class PeerReplicatorTest extends ReplicatorTestBase {

    @Override
    @BeforeClass
    void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass
    void shutdown() throws Exception {
        super.shutdown();
    }

    @DataProvider(name = "lookupType")
    public Object[][] codecProvider() {
        return new Object[][] { { "http" }, { "binary" } };
    }

    /**
     * It verifies that lookup/admin requests for global-namespace would be redirected to peer-cluster if local cluster
     * doesn't own it and peer-cluster owns it, else request will be failed.
     * <pre>
     * 1. Create global-namespace ns1 for replication cluster-r1
     * 2. Try to create producer using broker in cluster r3
     * 3. Reject lookup: "r3" receives request and doesn't find namespace in local/peer cluster
     * 4. Add "r1" as a peer-cluster into "r3"
     * 5. Try to create producer using broker in cluster r3
     * 6. Success : "r3" finds "r1" in peer cluster which owns n1 and redirects to "r1"
     * 7. call admin-api to "r3" which redirects request to "r1"
     *
     * </pre>
     *
     * @param protocol
     * @throws Exception
     */
    @Test(dataProvider = "lookupType")
    public void testPeerClusterTopicLookup(String protocol) throws Exception {

        final String serviceUrl = protocol.equalsIgnoreCase("http") ? pulsar3.getWebServiceAddress()
                : pulsar3.getBrokerServiceUrl();
        final String namespace1 = "pulsar/global/peer1-" + protocol;
        final String namespace2 = "pulsar/global/peer2-" + protocol;
        admin1.namespaces().createNamespace(namespace1);
        admin1.namespaces().createNamespace(namespace2);
        // add replication cluster
        admin1.namespaces().setNamespaceReplicationClusters(namespace1, Sets.newHashSet("r1"));
        admin1.namespaces().setNamespaceReplicationClusters(namespace2, Sets.newHashSet("r2"));
        admin1.clusters().updatePeerClusterNames("r3", null);
        // disable tls as redirection url is prepared according tls configuration
        pulsar1.getConfiguration().setTlsEnabled(false);
        pulsar2.getConfiguration().setTlsEnabled(false);
        pulsar3.getConfiguration().setTlsEnabled(false);

        final String topic1 = "persistent://" + namespace1 + "/topic1";
        final String topic2 = "persistent://" + namespace2 + "/topic2";

        PulsarClient client3 = PulsarClient.builder().serviceUrl(serviceUrl).statsInterval(0, TimeUnit.SECONDS).build();
        try {
            // try to create producer for topic1 (part of cluster: r1) by calling cluster: r3
            client3.newProducer().topic(topic1).create();
            fail("should have failed as cluster:r3 doesn't own namespace");
        } catch (PulsarClientException e) {
            // Ok
        }

        try {
            // try to create producer for topic2 (part of cluster: r2) by calling cluster: r3
            client3.newProducer().topic(topic2).create();
            fail("should have failed as cluster:r3 doesn't own namespace");
        } catch (PulsarClientException e) {
            // Ok
        }

        // set peer-clusters : r3->r1
        admin1.clusters().updatePeerClusterNames("r3", Sets.newLinkedHashSet(Lists.newArrayList("r1")));
        Producer<byte[]> producer = client3.newProducer().topic(topic1).create();
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topic1).get();
        assertNotNull(topic);
        pulsar1.getBrokerService().updateRates();
        // get stats for topic1 using cluster-r3's admin3
        TopicStats stats = admin1.topics().getStats(topic1);
        assertNotNull(stats);
        assertEquals(stats.publishers.size(), 1);
        stats = admin3.topics().getStats(topic1);
        assertNotNull(stats);
        assertEquals(stats.publishers.size(), 1);
        producer.close();

        // set peer-clusters : r3->r2
        admin2.clusters().updatePeerClusterNames("r3", Sets.newLinkedHashSet(Lists.newArrayList("r2")));
        producer = client3.newProducer().topic(topic2).create();
        topic = (PersistentTopic) pulsar2.getBrokerService().getOrCreateTopic(topic2).get();
        assertNotNull(topic);
        pulsar2.getBrokerService().updateRates();
        // get stats for topic1 using cluster-r3's admin3
        stats = admin3.topics().getStats(topic2);
        assertNotNull(stats);
        assertEquals(stats.publishers.size(), 1);
        stats = admin3.topics().getStats(topic2);
        assertNotNull(stats);
        assertEquals(stats.publishers.size(), 1);
        producer.close();

        client3.close();

    }

	@Test
	public void testGetPeerClusters() throws Exception {
		final String mainClusterName = "r1";
		assertEquals(admin1.clusters().getPeerClusterNames(mainClusterName), null);
		LinkedHashSet<String> peerClusters = Sets.newLinkedHashSet(Lists.newArrayList("r2", "r3"));
		admin1.clusters().updatePeerClusterNames(mainClusterName, peerClusters);
		retryStrategically((test) -> {
			try {
				return admin1.clusters().getPeerClusterNames(mainClusterName).size() == 1;
			} catch (PulsarAdminException e) {
				return false;
			}
		}, 5, 100);
		assertEquals(admin1.clusters().getPeerClusterNames(mainClusterName), peerClusters);
	}
}

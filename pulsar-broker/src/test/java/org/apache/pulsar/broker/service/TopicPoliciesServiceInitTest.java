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

package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Optional;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicPoliciesServiceInitTest {

    private final LocalBookkeeperEnsemble bk = new LocalBookkeeperEnsemble(1, 0, () -> 0);
    private ServiceConfiguration config;

    @BeforeClass
    public void setup() throws Exception {
        bk.start();
    }

    @AfterClass
    public void teardown() throws Exception {
        bk.stop();
    }

    @BeforeMethod
    public void initConfig() {
        config = new ServiceConfiguration();
        config.setClusterName("testCluster");
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bk.getZookeeperPort());
        config.setWebServicePort(Optional.of(0));
        config.setBrokerServicePort(Optional.of(0));
        config.setAdvertisedAddress("127.0.0.1");
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setManagedLedgerDefaultEnsembleSize(1);
    }

    @Test
    public void testSystemTopicPoliciesService() throws Exception {
        config.setTopicPoliciesServiceClassName(SystemTopicBasedTopicPoliciesService.class.getName());
        @Cleanup final var pulsar = new PulsarService(config);
        pulsar.start();
        assertTrue(pulsar.getTopicPoliciesService() instanceof SystemTopicBasedTopicPoliciesService);
    }

    @Test
    public void testLegacyAwareTopicPoliciesService() throws Exception {
        config.setTopicPoliciesServiceClassName(MetadataStoreTopicPoliciesService.class.getName());
        @Cleanup final var pulsar = new PulsarService(config);
        pulsar.start();
        assertTrue(pulsar.getTopicPoliciesService() instanceof LegacyAwareTopicPoliciesService);
    }

    @Test
    public void testMetadataStoreTopicPoliciesService() throws Exception {
        config.setTopicPoliciesServiceClassName(MetadataStoreTopicPoliciesService.class.getName());
        // the topic policies service won't be aware of the legacy `__change_events` system topics
        config.setSystemTopicEnabled(false);
        @Cleanup final var pulsar = new PulsarService(config);
        pulsar.start();
        assertTrue(pulsar.getTopicPoliciesService() instanceof MetadataStoreTopicPoliciesService);
    }

    @Test
    public void testWrongInitialization() throws Exception {
        config.setTopicPoliciesServiceClassName(LegacyAwareTopicPoliciesService.class.getName());
        @Cleanup final var pulsar = new PulsarService(config);
        try {
            pulsar.start();
            fail();
        } catch (PulsarServerException e) {
            assertTrue(e.getCause().getCause() instanceof NoSuchMethodException);
        }
    }
}

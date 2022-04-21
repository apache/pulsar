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
package org.apache.pulsar.broker.zookeeper;

import static org.testng.Assert.assertTrue;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class MultiBrokerMetadataConsistencyTest extends MultiBrokerBaseTest {
    @Override
    protected int numberOfAdditionalBrokers() {
        return 2;
    }

    TestZKServer testZKServer;

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        testZKServer = new TestZKServer();
    }

    @Override
    protected void onCleanup() {
        super.onCleanup();
        if (testZKServer != null) {
            try {
                testZKServer.close();
            } catch (Exception e) {
                log.error("Error in stopping ZK server", e);
            }
        }
    }

    @Override
    protected MetadataStoreExtended createLocalMetadataStore() throws MetadataStoreException {
        return MetadataStoreExtended.create(testZKServer.getConnectionString(), MetadataStoreConfig.builder().build());
    }

    @Override
    protected MetadataStoreExtended createConfigurationMetadataStore() throws MetadataStoreException {
        return MetadataStoreExtended.create(testZKServer.getConnectionString(), MetadataStoreConfig.builder().build());
    }

    @Test
    public void newTopicShouldBeInTopicsList() throws PulsarAdminException {
        List<PulsarAdmin> admins = getAllAdmins();
        PulsarAdmin first = admins.get(0);
        PulsarAdmin second = admins.get(1);
        List<String> cacheMiss = second.topics().getList("public/default");
        assertTrue(cacheMiss.isEmpty());
        first.topics().createNonPartitionedTopic("persistent://public/default/my-topic");
        List<String> topics = second.topics().getList("public/default");
        assertTrue(topics.contains("persistent://public/default/my-topic"));
    }
}

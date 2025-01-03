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
package org.apache.pulsar.tests.integration.messaging;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarGeoClusterTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Geo replication test.
 */
@Slf4j
public class GeoReplicationTest extends PulsarGeoClusterTestBase {

    GeoReplication test;

    @BeforeClass(alwaysRun = true)
    public final void setupBeforeClass() throws Exception {
        setup();
        var cluster1 = getGeoCluster().getClusters()[0];
        var cluster2 = getGeoCluster().getClusters()[1];
        this.test = new GeoReplication(
                getPulsarClient(cluster1),
                getPulsarAdmin(cluster1),
                getPulsarClient(cluster2),
                getPulsarAdmin(cluster2)
        );
    }

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder[] beforeSetupCluster (
            PulsarClusterSpec.PulsarClusterSpecBuilder... specBuilder) {
        if (specBuilder != null) {
            Map<String, String> brokerEnvs = new HashMap<>();
            brokerEnvs.put("systemTopicEnabled", "false");
            brokerEnvs.put("topicLevelPoliciesEnabled", "false");
            for(PulsarClusterSpec.PulsarClusterSpecBuilder builder : specBuilder) {
                builder.brokerEnvs(brokerEnvs);
            }
        }
        return specBuilder;
    }

    @AfterClass(alwaysRun = true)
    public final void tearDownAfterClass() throws Exception {
        cleanup();
        this.test.close();
    }

    @Test(timeOut = 1000 * 30, dataProvider = "TopicDomain")
    public void testTopicReplication(String domain) throws Exception {
        test.testTopicReplication(domain);
    }
}

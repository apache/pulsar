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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.TopicType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-replication")
public class ReplicationTopicGcUsingGlobalZKTest extends ReplicationTopicGcTest {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.usingGlobalZK = true;
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test(dataProvider = "topicTypes")
    public void testTopicGC(TopicType topicType) throws Exception {
        if (topicType.equals(TopicType.PARTITIONED)) {
            // Pulsar does not support the feature "brokerDeleteInactivePartitionedTopicMetadataEnabled" when enabling
            // Geo-Replication with Global ZK.
            return;
        }
        super.testTopicGC(topicType);
    }

    @Test(dataProvider = "topicTypes")
    public void testRemoteClusterStillConsumeAfterCurrentClusterGc(TopicType topicType) throws Exception {
        if (topicType.equals(TopicType.PARTITIONED)) {
            // Pulsar does not support the feature "brokerDeleteInactivePartitionedTopicMetadataEnabled" when enabling
            // Geo-Replication with Global ZK.
            return;
        }
        super.testRemoteClusterStillConsumeAfterCurrentClusterGc(topicType);
    }
}

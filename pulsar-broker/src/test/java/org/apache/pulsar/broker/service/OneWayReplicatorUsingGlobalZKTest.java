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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class OneWayReplicatorUsingGlobalZKTest extends OneWayReplicatorTest {

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

    @Test(enabled = false)
    public void testReplicatorProducerStatInTopic() throws Exception {
        super.testReplicatorProducerStatInTopic();
    }

    @Test(enabled = false)
    public void testCreateRemoteConsumerFirst() throws Exception {
        super.testReplicatorProducerStatInTopic();
    }

    @Test(enabled = false)
    public void testTopicCloseWhenInternalProducerCloseErrorOnce() throws Exception {
        super.testReplicatorProducerStatInTopic();
    }

    @Test(enabled = false)
    public void testConcurrencyOfUnloadBundleAndRecreateProducer() throws Exception {
        super.testConcurrencyOfUnloadBundleAndRecreateProducer();
    }

    @Test(enabled = false)
    public void testPartitionedTopicLevelReplication() throws Exception {
        super.testPartitionedTopicLevelReplication();
    }

    @Test(enabled = false)
    public void testPartitionedTopicLevelReplicationRemoteTopicExist() throws Exception {
        super.testPartitionedTopicLevelReplicationRemoteTopicExist();
    }

    @Test(enabled = false)
    public void testPartitionedTopicLevelReplicationRemoteConflictTopicExist() throws Exception {
        super.testPartitionedTopicLevelReplicationRemoteConflictTopicExist();
    }

    @Test(enabled = false)
    public void testConcurrencyOfUnloadBundleAndRecreateProducer2() throws Exception {
        super.testConcurrencyOfUnloadBundleAndRecreateProducer2();
    }

    @Test(enabled = false)
    public void testUnFenceTopicToReuse() throws Exception {
        super.testUnFenceTopicToReuse();
    }

    @Test
    public void testDeleteNonPartitionedTopic() throws Exception {
        super.testDeleteNonPartitionedTopic();
    }

    @Test
    public void testDeletePartitionedTopic() throws Exception {
        super.testDeletePartitionedTopic();
    }

    @Test(enabled = false)
    public void testNoExpandTopicPartitionsWhenDisableTopicLevelReplication() throws Exception {
        super.testNoExpandTopicPartitionsWhenDisableTopicLevelReplication();
    }

    @Test(enabled = false)
    public void testExpandTopicPartitionsOnNamespaceLevelReplication() throws Exception {
        super.testExpandTopicPartitionsOnNamespaceLevelReplication();
    }
}

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

package org.apache.pulsar.broker.admin;

import org.apache.pulsar.broker.service.MetadataStoreTopicPoliciesService;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class MetadataStoreTopicPoliciesTest extends TopicPoliciesTest {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setTopicPoliciesServiceClassName(MetadataStoreTopicPoliciesService.class.getName());
        super.setup();
    }

    @Override
    protected void clearTopicPoliciesCache() {
    }

    @Test(enabled = false)
    @Override
    public void testTopicPolicyInitialValueWithNamespaceAlreadyLoaded() throws Exception {
        // This test is specific to SystemTopicBasedTopicPoliciesService (uses getPoliciesCacheInit).
        // Not applicable to MetadataStoreTopicPoliciesService.
    }

    @Test(enabled = false)
    @Override
    public void testSystemTopicShouldBeCompacted() throws Exception {
        // Relies on __change_events system topic, which does not exist with MetadataStoreTopicPoliciesService.
    }

    @Test(enabled = false)
    @Override
    public void testPoliciesCanBeDeletedWithTopic() throws Exception {
        // Directly accesses __change_events PersistentTopic for compaction.
        // Not applicable to MetadataStoreTopicPoliciesService.
    }

    @Test(enabled = false)
    @Override
    public void testProduceChangesWithEncryptionRequired() throws Exception {
        // Checks __change_events LAC, which does not exist with MetadataStoreTopicPoliciesService.
    }

    @Test(enabled = false)
    @Override
    public void testTopicPoliciesAfterCompaction(String reloadPolicyType) throws Exception {
        // The "Recreate_Service" variant creates a new SystemTopicBasedTopicPoliciesService,
        // which is not applicable to MetadataStoreTopicPoliciesService.
    }
}

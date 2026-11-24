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

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.UUID;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The topic-level policies of a scalable topic are rejected with 405 when topic-level policies are disabled.
 */
@Test(groups = "broker-admin")
public class ScalableTopicPoliciesDisabledTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(false);
        internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void testTopicPoliciesDisabledReturns405() throws Exception {
        String topic = "topic://public/default/" + UUID.randomUUID();
        admin.scalableTopics().createScalableTopic(topic, 1);
        PulsarAdminException.NotAllowedException e = expectThrows(PulsarAdminException.NotAllowedException.class,
                () -> admin.topicPolicies().getRetention(topic));
        assertTrue(e.getMessage().contains("Topic level policies is disabled"), e.getMessage());
    }
}

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

package org.apache.pulsar.tests.integration.cli.topicpolicies;

import static org.testng.Assert.assertTrue;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class SchemaCompatibilityStrategyTest extends PulsarTestSuite {
    @Override
    public void setupCluster() throws Exception {
        super.setupCluster();
    }

    @Override
    public void tearDownCluster() throws Exception {
        super.tearDownCluster();
    }

    @Test
    public void testSchemaCompatibilityCmd() throws Exception {
        String topicName = generateTopicName("",true);
        pulsarAdmin.topics().createNonPartitionedTopic(topicName);

        Awaitility.await().untilAsserted(()->{
            ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("topics get-schema-compatibility-strategy " + topicName);
            assertTrue(result.getStdout().contentEquals("UNDEFINED"));
        });

        Awaitility.await().untilAsserted(()->{
            ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("topics get-schema-compatibility-strategy --applied " + topicName);
            assertTrue(result.getStdout().contentEquals("FULL"));
        });

        pulsarAdmin.topicPolicies().removeSchemaCompatibilityStrategy(topicName);
        Awaitility.await().untilAsserted(()->{
            ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("topics get-schema-compatibility-strategy " + topicName);
            assertTrue(result.getStdout().contentEquals("UNDEFINED"));
        });
    }

    @Test
    public void testSchemaCompatibilityCmdWithNamespaceLevel() throws Exception {
        String topicName = generateTopicName("",true);
        pulsarAdmin.namespaces()
                .setSchemaCompatibilityStrategy("public/default", SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);
        pulsarAdmin.topics().createNonPartitionedTopic(topicName);

        Awaitility.await().untilAsserted(()->{
            ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("topics get-schema-compatibility-strategy " + topicName);
            assertTrue(result.getStdout().contentEquals("UNDEFINED"));
        });
        Awaitility.await().untilAsserted(()->{
            ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("topics get-schema-compatibility-strategy --applied " + topicName);
            assertTrue(result.getStdout().contentEquals("ALWAYS_INCOMPATIBLE"));
        });

        pulsarAdmin.namespaces()
                .setSchemaCompatibilityStrategy("public/default", SchemaCompatibilityStrategy.UNDEFINED);
        Awaitility.await().untilAsserted(()->{
            ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("topics get-schema-compatibility-strategy " + topicName);
            assertTrue(result.getStdout().contentEquals("UNDEFINED"));
        });
    }
}

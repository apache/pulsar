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

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarCliTestSuite;
import org.awaitility.Awaitility;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SchemaCompatibilityStrategyTest extends PulsarCliTestSuite {
    @BeforeClass(alwaysRun = true)
    @Override
    public void before() throws Exception {
        enableTopicPolicies();
        super.before();
    }

    @BeforeClass(alwaysRun = true)
    @Override
    public void after() throws Exception {
        super.after();
    }

    @Test
    public void testSchemaCompatibilityStrategyCmd() throws Exception {
        String topicName = generateTopicName("test-schema-compatibility-strategy", true);
        pulsarAdmin.topics().createNonPartitionedTopic(topicName);

        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies",
                "get-schema-compatibility-strategy", topicName);
        assertEquals(result.getStdout().trim(), "null");

        result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies", "get-schema-compatibility-strategy",
                "--applied", topicName);
        assertEquals(result.getStdout().trim(), SchemaCompatibilityStrategy.FULL.name());

        pulsarAdmin.topicPolicies().removeSchemaCompatibilityStrategy(topicName);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies",
                    "get-schema-compatibility-strategy", topicName).getStdout().trim(), "null");
        });
    }

    @Test
    public void testSchemaCompatibilityStrategyCmdWithNamespaceLevel() throws Exception {
        String ns = generateNamespaceName();
        String fullNS = "public/" + ns;
        pulsarAdmin.namespaces().createNamespace("public/"+ns);

        String topicName = generateTopicName(ns, "test-schema-compatibility-strategy",
                true);
        pulsarAdmin.namespaces().setSchemaCompatibilityStrategy(fullNS, SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);
        pulsarAdmin.topics().createNonPartitionedTopic(topicName);

        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies",
                "get-schema-compatibility-strategy", topicName);
        assertEquals(result.getStdout().trim(), "null");

        result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies",
                "get-schema-compatibility-strategy", "--applied", topicName);
        assertEquals(result.getStdout().trim(), SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE.name());

        pulsarAdmin.namespaces()
                .setSchemaCompatibilityStrategy(fullNS, SchemaCompatibilityStrategy.UNDEFINED);
        result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies", "get-schema-compatibility-strategy",
                topicName);
        assertEquals(result.getStdout().trim(), "null");
    }
}

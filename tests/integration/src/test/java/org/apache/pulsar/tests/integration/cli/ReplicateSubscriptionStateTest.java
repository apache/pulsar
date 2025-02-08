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
package org.apache.pulsar.tests.integration.cli;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarCliTestSuite;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ReplicateSubscriptionStateTest extends PulsarCliTestSuite {
    @BeforeClass(alwaysRun = true)
    @Override
    public void before() throws Exception {
        enableTopicPolicies();
        super.before();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void after() throws Exception {
        super.after();
    }

    @Test
    public void testReplicateSubscriptionStateCmd() throws Exception {
        TopicName topicName = TopicName.get(generateTopicName("testReplicateSubscriptionState", true));
        pulsarAdmin.topics().createNonPartitionedTopic(topicName.toString());
        String subName = "my-sub";
        pulsarAdmin.topics().createSubscription(topicName.toString(), subName, MessageId.earliest);

        String topicNameString = topicName.toString();
        String namesapceNameString = topicName.getNamespace();

        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "get-replicate-subscription-state", namesapceNameString);
        assertEquals(result.getStdout().trim(), "null");
        assertEquals(result.getExitCode(), 0);
        result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies",
                "get-schema-compatibility-strategy", topicNameString);
        assertEquals(result.getStdout().trim(), "null");
        assertEquals(result.getExitCode(), 0);

        result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-replicate-subscription-state", "--enabled", "true", namesapceNameString);
        assertEquals(result.getExitCode(), 0);
        result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "get-replicate-subscription-state", namesapceNameString);
        assertEquals(result.getExitCode(), 0);
        assertEquals(result.getStdout().trim(), "true");
        result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-replicate-subscription-state", "--enabled", "false", namesapceNameString);
        assertEquals(result.getExitCode(), 0);
        result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "get-replicate-subscription-state", namesapceNameString);
        assertEquals(result.getExitCode(), 0);
        assertEquals(result.getStdout().trim(), "false");
        result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "remove-replicate-subscription-state", namesapceNameString);
        assertEquals(result.getExitCode(), 0);
        result = pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "get-replicate-subscription-state", namesapceNameString);
        assertEquals(result.getExitCode(), 0);
        assertEquals(result.getStdout().trim(), "null");

        result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies", "get-replicate-subscription-state",
                "--applied", topicNameString);
        assertEquals(result.getExitCode(), 0);
        assertEquals(result.getStdout().trim(), "null");
        result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies", "get-replicate-subscription-state",
                topicNameString);
        assertEquals(result.getExitCode(), 0);
        assertEquals(result.getStdout().trim(), "null");

        result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies",
                "set-replicate-subscription-state", "--enabled", "false", topicNameString);
        assertEquals(result.getExitCode(), 0);
        await().untilAsserted(() -> {
            ContainerExecResult r =
                    pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies", "get-replicate-subscription-state",
                            topicNameString);
            assertEquals(r.getExitCode(), 0);
            assertEquals(r.getStdout().trim(), "false");
        });
        await().untilAsserted(() -> {
            ContainerExecResult r =
                    pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies", "get-replicate-subscription-state",
                            "--applied",
                            topicNameString);
            assertEquals(r.getExitCode(), 0);
            assertEquals(r.getStdout().trim(), "false");
        });

        result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies",
                "set-replicate-subscription-state", "--enabled", "true", topicNameString);
        assertEquals(result.getExitCode(), 0);
        await().untilAsserted(() -> {
            ContainerExecResult r =
                    pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies", "get-replicate-subscription-state",
                            topicNameString);
            assertEquals(r.getExitCode(), 0);
            assertEquals(r.getStdout().trim(), "true");
        });
        await().untilAsserted(() -> {
            ContainerExecResult r =
                    pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies", "get-replicate-subscription-state",
                            "--applied",
                            topicNameString);
            assertEquals(r.getExitCode(), 0);
            assertEquals(r.getStdout().trim(), "true");
        });

        result = pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies",
                "remove-replicate-subscription-state", topicNameString);
        assertEquals(result.getExitCode(), 0);
        await().untilAsserted(() -> {
            ContainerExecResult r =
                    pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies", "get-replicate-subscription-state",
                            topicNameString);
            assertEquals(r.getExitCode(), 0);
            assertEquals(r.getStdout().trim(), "null");
        });
    }

    @Test
    public void testReplicateSubscriptionStateCmdWithInvalidParameters() throws Exception {
        assertThrows(ContainerExecException.class, () -> pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-replicate-subscription-state", "public/default"));
        assertThrows(ContainerExecException.class, () -> pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-replicate-subscription-state", "--enabled", "public/default"));

        assertThrows(ContainerExecException.class, () -> pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies",
                "set-replicate-subscription-state", "public/default/test"));
        assertThrows(ContainerExecException.class, () -> pulsarCluster.runAdminCommandOnAnyBroker("topicPolicies",
                "set-replicate-subscription-state", "--enabled", "public/default/test"));
    }
}
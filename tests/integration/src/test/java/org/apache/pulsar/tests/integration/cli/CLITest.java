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
package org.apache.pulsar.tests.integration.cli;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.annotations.Test;

/**
 * Test Pulsar CLI.
 */
public class CLITest extends PulsarTestSuite {

    @Test
    public void testDeprecatedCommands() throws Exception {
        String tenantName = "test-deprecated-commands";

        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("--help");
        assertFalse(result.getStdout().isEmpty());
        assertFalse(result.getStdout().contains("Usage: properties "));
        result = pulsarCluster.runAdminCommandOnAnyBroker(
            "properties", "create", tenantName,
            "--allowed-clusters", pulsarCluster.getClusterName(),
            "--admin-roles", "admin"
        );
        assertTrue(result.getStderr().contains("deprecated"));

        result = pulsarCluster.runAdminCommandOnAnyBroker(
            "properties", "list");
        assertTrue(result.getStdout().contains(tenantName));
        result = pulsarCluster.runAdminCommandOnAnyBroker(
            "tenants", "list");
        assertTrue(result.getStdout().contains(tenantName));
    }

    @Test
    public void testCreateSubscriptionCommand() throws Exception {
        String topic = "testCreateSubscriptionCommmand";

        String subscriptionPrefix = "subscription-";

        int i = 0;
        for (BrokerContainer container : pulsarCluster.getBrokers()) {
            ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "topics",
                "create-subscription",
                "persistent://public/default/" + topic,
                "--subscription",
                "" + subscriptionPrefix + i
            );
            assertTrue(result.getStdout().isEmpty());
            assertTrue(result.getStderr().isEmpty());
            i++;
        }
    }

    @Test
    public void testTopicTerminationOnTopicsWithoutConnectedConsumers() throws Exception {
        String topicName = "persistent://public/default/test-topic-termination";
        BrokerContainer container = pulsarCluster.getAnyBroker();
        ContainerExecResult result = container.execCmd(
            PulsarCluster.CLIENT_SCRIPT,
            "produce",
            "-m",
            "\"test topic termination\"",
            "-n",
            "1",
            topicName);

        assertTrue(result.getStdout().contains("1 messages successfully produced"));

        // terminate the topic
        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "topics",
            "terminate",
            topicName);
        assertTrue(result.getStdout().contains("Topic succesfully terminated at"));

        // try to produce should fail
        try {
            pulsarCluster.getAnyBroker().execCmd(PulsarCluster.CLIENT_SCRIPT,
                                                 "produce",
                                                 "-m",
                                                 "\"test topic termination\"",
                                                 "-n",
                                                 "1",
                                                 topicName);
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStdout().contains("Topic was already terminated"));
        }
    }

    @Test
    public void testSchemaCLI() throws Exception {
        BrokerContainer container = pulsarCluster.getAnyBroker();
        String topicName = "persistent://public/default/test-schema-cli";

        ContainerExecResult result = container.execCmd(
            PulsarCluster.CLIENT_SCRIPT,
            "produce",
            "-m",
            "\"test topic schema\"",
            "-n",
            "1",
            topicName);
        assertTrue(result.getStdout().contains("1 messages successfully produced"));

        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "schemas",
            "upload",
            topicName,
            "-f",
            "/pulsar/conf/schema_example.conf"
        );
        assertTrue(result.getStdout().isEmpty());
        assertTrue(result.getStderr().isEmpty());

        // get schema
        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "schemas",
            "get",
            topicName);
        assertTrue(result.getStdout().contains("\"type\" : \"STRING\""));

        // delete the schema
        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "schemas",
            "delete",
            topicName);
        assertTrue(result.getStdout().isEmpty());
        assertTrue(result.getStderr().isEmpty());

        // get schema again
        try {
            container.execCmd(PulsarCluster.ADMIN_SCRIPT,
                              "schemas",
                              "get",
                              "persistent://public/default/test-schema-cli"
                              );
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStderr().contains("Reason: HTTP 404 Not Found"));
        }
    }

    @Test
    public void testSetInfiniteRetention() throws Exception {
        ContainerExecResult result;

        String namespace = "get-and-set-retention" + randomName(8);
        pulsarCluster.createNamespace(namespace);

        String[] setCommand = {
            "namespaces", "set-retention", "public/" + namespace,
            "--size", "-1",
            "--time", "-1"
        };

        result = pulsarCluster.runAdminCommandOnAnyBroker(setCommand);
        assertTrue(
            result.getStdout().isEmpty(),
            result.getStdout()
        );
        assertTrue(
            result.getStderr().isEmpty(),
            result.getStdout()
        );

        String[] getCommand = {
            "namespaces", "get-retention", "public/" + namespace
        };

        result = pulsarCluster.runAdminCommandOnAnyBroker(getCommand);
        assertTrue(
            result.getStdout().contains("\"retentionTimeInMinutes\" : -1"),
            result.getStdout());
        assertTrue(
            result.getStdout().contains("\"retentionSizeInMB\" : -1"),
            result.getStdout());
    }

}

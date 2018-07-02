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
package org.apache.pulsar.tests.integration;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.tests.containers.BrokerContainer;
import org.apache.pulsar.tests.topologies.PulsarCluster;
import org.apache.pulsar.tests.topologies.PulsarClusterTestBase;
import org.testcontainers.containers.Container.ExecResult;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test Pulsar CLI.
 */
public class TestCLI extends PulsarClusterTestBase {

    @Test
    public void testDeprecatedCommands() throws Exception {
        String tenantName = "test-deprecated-commands";

        ExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("--help");
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
            ExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "persistent",
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
        ExecResult result = container.execCmd(
            PulsarCluster.CLIENT_SCRIPT,
            "produce",
            "-m",
            "\"test topic termination\"",
            "-n",
            "1",
            topicName);

        Assert.assertTrue(result.getStdout().contains("1 messages successfully produced"));

        // terminate the topic
        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "persistent",
            "terminate",
            topicName);
        Assert.assertTrue(result.getStdout().contains("Topic succesfully terminated at"));

        // try to produce should fail
        result = pulsarCluster.getAnyBroker().execCmd(
            PulsarCluster.CLIENT_SCRIPT,
            "produce",
            "-m",
            "\"test topic termination\"",
            "-n",
            "1",
            topicName);
        assertTrue(result.getStdout().contains("Topic was already terminated"));
    }

    @Test
    public void testSchemaCLI() throws Exception {
        BrokerContainer container = pulsarCluster.getAnyBroker();
        String topicName = "persistent://public/default/test-schema-cli";

        ExecResult result = container.execCmd(
            PulsarCluster.CLIENT_SCRIPT,
            "produce",
            "-m",
            "\"test topic schema\"",
            "-n",
            "1",
            topicName);
        Assert.assertTrue(result.getStdout().contains("1 messages successfully produced"));

        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "schemas",
            "upload",
            topicName,
            "-f",
            "/pulsar/conf/schema_example.conf"
        );
        Assert.assertTrue(result.getStdout().isEmpty());
        Assert.assertTrue(result.getStderr().isEmpty());

        // get schema
        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "schemas",
            "get",
            topicName);
        Assert.assertTrue(result.getStdout().contains("\"type\" : \"STRING\""));

        // delete the schema
        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "schemas",
            "delete",
            topicName);
        Assert.assertTrue(result.getStdout().isEmpty());
        Assert.assertTrue(result.getStderr().isEmpty());

        // get schema again
        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "schemas",
            "get",
            "persistent://public/default/test-schema-cli"
        );
        assertTrue(result.getStderr().contains("Reason: HTTP 404 Not Found"));
    }
}

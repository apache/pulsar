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

import org.apache.pulsar.tests.PulsarClusterUtils;
import org.apache.pulsar.tests.integration.cluster.Cluster3Bookie2Broker;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

public class TestCLI {
    private static final String clusterName = "test";
    private Cluster3Bookie2Broker cluster;

    @BeforeClass
    public void setup() throws Exception {
        cluster = new Cluster3Bookie2Broker(TestCompaction.class.getSimpleName());
        cluster.start();
        cluster.startAllBrokers();
        cluster.startAllProxies();
    }

    @Test
    public void testDeprecatedCommands() throws Exception {
        Assert.assertFalse(cluster.execInBroker(PulsarClusterUtils.PULSAR_ADMIN, "--help")
                           .contains("Usage: properties "));
        Assert.assertTrue(cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN, "properties",
            "create", "compaction-test-cli", "--allowed-clusters", clusterName,
            "--admin-roles", "admin").contains("deprecated"));
        Assert.assertTrue(cluster.execInBroker(PulsarClusterUtils.PULSAR_ADMIN, "properties", "list")
                          .contains("compaction-test-cli"));
        Assert.assertTrue(cluster.execInBroker(PulsarClusterUtils.PULSAR_ADMIN, "tenants", "list")
                          .contains("compaction-test-cli"));

    }

    @Test(dependsOnMethods = "testDeprecatedCommands")
    public void testCreateSubscriptionCommand() {
        String topic = "testCreateSubscriptionCommmand";

        String subscriptionPrefix = "subscription-";

        int i = 0;
//        for (String b : PulsarClusterUtils.brokerSet(docker, clusterName)) {
//            Assert.assertTrue(
//                DockerUtils.runCommand(docker, b,
//                    PulsarClusterUtils.PULSAR_ADMIN,
//                    "persistent",
//                    "create-subscription",
//                    "persistent://public/default/" + topic,
//                    "--subscription",
//                    subscriptionPrefix + i
//                ).isEmpty()
//            );
//            i++;
//        }
    }

    @Test(dependsOnMethods = "testCreateSubscriptionCommand")
    public void testTopicTerminationOnTopicsWithoutConnectedConsumers() throws Exception {

        Assert.assertTrue(cluster.execInBroker(
            PulsarClusterUtils.PULSAR_CLIENT,
            "produce",
            "-m",
            "\"test topic termination\"",
            "-n",
            "1",
            "persistent://public/default/test-topic-termination"
        ).contains("1 messages successfully produced"));

        // terminate the topic
        Assert.assertTrue(cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN,
            "persistent",
            "terminate",
            "persistent://public/default/test-topic-termination"
        ).contains("Topic succesfully terminated at"));

        // try to produce should fail

        try {
                cluster.execInBroker(
                    PulsarClusterUtils.PULSAR_CLIENT,
                "produce",
                "-m",
                "\"test topic termination\"",
                "-n",
                "1",
                "persistent://public/default/test-topic-termination"
            );
            fail("Should fail to produce messages to a terminated topic");
        } catch (RuntimeException re) {
            // expected
        }
    }

    @Test(dependsOnMethods = "testTopicTerminationOnTopicsWithoutConnectedConsumers")
    public void testSchemaCLI() throws Exception {

        Assert.assertTrue(cluster.execInBroker(
            PulsarClusterUtils.PULSAR_CLIENT,
            "produce",
            "-m",
            "\"test topic schema\"",
            "-n",
            "1",
            "persistent://public/default/test-schema-cli"
        ).contains("1 messages successfully produced"));

        Assert.assertTrue(cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN,
            "schemas",
            "upload",
            "persistent://public/default/test-schema-cli",
            "-f",
            "/pulsar/conf/schema_example.conf"
        ).isEmpty());

        // get schema
        Assert.assertTrue(cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN,
            "schemas",
            "get",
            "persistent://public/default/test-schema-cli"
        ).contains("\"type\" : \"STRING\""));

        // delete the schema
        Assert.assertTrue(cluster.execInBroker(
            PulsarClusterUtils.PULSAR_ADMIN,
            "schemas",
            "delete",
            "persistent://public/default/test-schema-cli"
        ).isEmpty());

        // get schema again
        try {
            cluster.execInBroker(
                PulsarClusterUtils.PULSAR_ADMIN,
                "schemas",
                "get",
                "persistent://public/default/test-schema-cli"
            );
            fail("Should fail to get schema if the schema is deleted");
        } catch (RuntimeException re) {
            // expected
        }

    }

    @AfterClass
    public void cleanup() throws Exception {
        cluster.stop();
    }
}

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

import static org.testng.Assert.fail;

import com.github.dockerjava.api.DockerClient;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.tests.DockerUtils;
import org.apache.pulsar.tests.PulsarClusterUtils;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.arquillian.testng.Arquillian;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCLI extends Arquillian {
    private static String clusterName = "test";

    @ArquillianResource
    DockerClient docker;

    @BeforeMethod
    public void waitServicesUp() throws Exception {
        Assert.assertTrue(PulsarClusterUtils.waitZooKeeperUp(docker, clusterName, 30, TimeUnit.SECONDS));
        Assert.assertTrue(PulsarClusterUtils.waitAllBrokersUp(docker, clusterName));
    }

    @Test
    public void testDeprecatedCommands() throws Exception {
        String broker = PulsarClusterUtils.brokerSet(docker, clusterName).stream().findAny().get();

        Assert.assertFalse(DockerUtils.runCommand(docker, broker, "/pulsar/bin/pulsar-admin", "--help")
                           .contains("Usage: properties "));
        Assert.assertTrue(DockerUtils.runCommand(docker, broker,
                                                 "/pulsar/bin/pulsar-admin", "properties",
                                                 "create", "compaction-test-cli", "--allowed-clusters", clusterName,
                                                 "--admin-roles", "admin").contains("deprecated"));
        Assert.assertTrue(DockerUtils.runCommand(docker, broker, "/pulsar/bin/pulsar-admin", "properties", "list")
                          .contains("compaction-test-cli"));
        Assert.assertTrue(DockerUtils.runCommand(docker, broker, "/pulsar/bin/pulsar-admin", "tenants", "list")
                          .contains("compaction-test-cli"));

    }

    @Test
    public void testCreateSubscriptionCommand() throws Exception {
        String topic = "testCreateSubscriptionCommmand";

        String subscriptionPrefix = "subscription-";

        int i = 0;
        for (String b : PulsarClusterUtils.brokerSet(docker, clusterName)) {
            Assert.assertTrue(
                DockerUtils.runCommand(docker, b,
                    "/pulsar/bin/pulsar-admin",
                    "persistent",
                    "create-subscription",
                    "persistent://public/default/" + topic,
                    "--subscription",
                    subscriptionPrefix + i
                ).isEmpty()
            );
            i++;
        }
    }

    @Test
    public void testTopicTerminationOnTopicsWithoutConnectedConsumers() throws Exception {
        String broker = PulsarClusterUtils.brokerSet(docker, clusterName).stream().findAny().get();

        Assert.assertTrue(DockerUtils.runCommand(
            docker, broker,
            "/pulsar/bin/pulsar-client",
            "produce",
            "-m",
            "\"test topic termination\"",
            "-n",
            "1",
            "persistent://public/default/test-topic-termination"
        ).contains("1 messages successfully produced"));

        // terminate the topic
        Assert.assertTrue(DockerUtils.runCommand(
            docker, broker,
            "/pulsar/bin/pulsar-admin",
            "persistent",
            "terminate",
            "persistent://public/default/test-topic-termination"
        ).contains("Topic succesfully terminated at"));

        // try to produce should fail

        try {
            DockerUtils.runCommand(
                docker, broker,
                "/pulsar/bin/pulsar-client",
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
}

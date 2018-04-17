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
}

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
package org.apache.pulsar.tests.integration.schema;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * Test schema operations
 */
@Slf4j
public class TestSchema extends PulsarClusterTestBase {

    @BeforeSuite
    @Override
    public void setupCluster() throws Exception {
        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .clusterName(clusterName)
                .build();

        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    @AfterSuite
    @Override
    public void tearDownCluster() {
        super.tearDownCluster();
    }

    @Test(dataProvider = "ServiceAndAdminUrls")
    public void testJarPojoSchemaUploadAvro(String serviceUrl, String adminUrl) throws Exception {

        ContainerExecResult containerExecResult = pulsarCluster.runAdminCommandOnAnyBroker(
                "schemas",
                "pojo", "--jar", "/pulsar/examples/api-examples.jar", "--type", "avro",
                "--class-name", "org.apache.pulsar.functions.api.examples.pojo.Tick",
                "persistent://public/default/pojo-avro");

        Assert.assertEquals(containerExecResult.getExitCode(), 0);
    }

    @Test(dataProvider = "ServiceAndAdminUrls")
    public void testJarPojoSchemaUploadJson(String serviceUrl, String adminUrl) throws Exception {

        ContainerExecResult containerExecResult = pulsarCluster.runAdminCommandOnAnyBroker(
                "schemas",
                "pojo", "--jar", "/pulsar/examples/api-examples.jar", "--type", "json",
                "--class-name", "org.apache.pulsar.functions.api.examples.pojo.Tick",
                "persistent://public/default/pojo-json");

        Assert.assertEquals(containerExecResult.getExitCode(), 0);
    }

}

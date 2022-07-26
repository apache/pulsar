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
package org.apache.pulsar.tests.integration.io;

import static org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.JAVAJAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.StandaloneContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testcontainers.containers.Network;
import org.testng.annotations.Test;

/**
 * Test behaviour of sinks with a pre-processing function
 */
@Slf4j
public class SinkWithPreprocessingFunctionTest extends PulsarStandaloneTestSuite {

    //Use PIP-117 new defaults so that the package management service is enabled.
    @Override
    public void setUpCluster() throws Exception {
        incrementSetupNumber();
        network = Network.newNetwork();
        String clusterName = PulsarClusterTestBase.randomName(8);
        container = new StandaloneContainer(clusterName, PulsarContainer.DEFAULT_IMAGE_NAME)
                .withNetwork(network)
                .withNetworkAliases(StandaloneContainer.NAME + "-" + clusterName)
                .withEnv("PF_stateStorageServiceUrl", "bk://localhost:4181");
        container.start();
        log.info("Pulsar cluster {} is up running:", clusterName);
        log.info("\tBinary Service Url : {}", container.getPlainTextServiceUrl());
        log.info("\tHttp Service Url : {}", container.getHttpServiceUrl());

        // add cluster to public tenant
        ContainerExecResult result = container.execCmd(
                "/pulsar/bin/pulsar-admin", "namespaces", "policies", "public/default");
        assertEquals(0, result.getExitCode());
        log.info("public/default namespace policies are {}", result.getStdout());
    }

    @Test(groups = {"sink"})
    public void testSinkWithPreprocessingFunction() throws Exception {

        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(container.getPlainTextServiceUrl())
                .build();

        final int numRecords = 10;

        String sinkName = "sink-with-preprocessing-function";
        String topicName = "test-sink-with-preprocessing-function";
        String packageName = "function://public/default/test-function@1.0";

        submitPackage(packageName, "package-function", JAVAJAR);

        submitSinkConnector(
                sinkName,
                topicName,
                "org.apache.pulsar.tests.integration.io.TestLoggingSink",
                JAVAJAR,
                "{\"log-topic\": \"log\"}",
                packageName,
                "org.apache.pulsar.functions.api.examples.RecordFunction");

        getSinkInfoSuccess(sinkName);
        getSinkStatus(sinkName);

        @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topicName)
                    .create();

        @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic("log")
                .subscriptionName("sub")
                .subscribe();

        for (int i = 0; i < numRecords; i++) {
            producer.send(i + "-test");
        }

        try {
            log.info("waiting for sink {}", sinkName);

            for (int i = 0; i < numRecords; i++) {
                Message<String> receive = consumer.receive(5, TimeUnit.SECONDS);
                assertNotNull(receive);
                assertEquals(receive.getValue(), i + "-test!");
            }
        } finally {
            dumpFunctionLogs(sinkName);
        }

        deleteSink(sinkName);
        getSinkInfoNotFound(sinkName);
    }

    private void submitPackage(String packageName, String description, String packagePath) throws Exception {
        String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "packages", "upload",
                packageName,
                "--description", description,
                "--path", packagePath

        };
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = container.execCmd(commands);
        assertTrue(
                result.getStdout().contains("successfully"),
                result.getStdout());
    }

    private void submitSinkConnector(String sinkName,
                                     String inputTopicName,
                                     String className,
                                     String archive,
                                     String configs,
                                     String preprocessFunction,
                                     String preprocessFunctionClassName) throws Exception {
        String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "sinks", "create",
                "--name", sinkName,
                "-i", inputTopicName,
                "--archive", archive,
                "--classname", className,
                "--sink-config", configs,
                "--preprocess-function", preprocessFunction,
                "--preprocess-function-classname", preprocessFunctionClassName
        };
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = container.execCmd(commands);
        assertTrue(
                result.getStdout().contains("Created successfully"),
                result.getStdout());
    }

    private void getSinkInfoSuccess(String sinkName) throws Exception {
        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "sinks",
                "get",
                "--tenant", "public",
                "--namespace", "default",
                "--name", sinkName
        );
        assertTrue(result.getStdout().contains("\"name\": \"" + sinkName + "\""));
    }

    private void getSinkStatus(String sinkName) throws Exception {
        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "sinks",
                "status",
                "--tenant", "public",
                "--namespace", "default",
                "--name", sinkName
        );
        log.info(result.getStdout());
        log.info(result.getStderr());
        assertTrue(result.getStdout().contains("\"running\" : true"));
    }

    private void deleteSink(String sinkName) throws Exception {
        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "sinks",
                "delete",
                "--tenant", "public",
                "--namespace", "default",
                "--name", sinkName
        );
        assertTrue(result.getStdout().contains("successfully"));
        result.assertNoStderr();
    }

    private void getSinkInfoNotFound(String sinkName) throws Exception {
        try {
            container.execCmd(
                    PulsarCluster.ADMIN_SCRIPT,
                    "sinks",
                    "get",
                    "--tenant", "public",
                    "--namespace", "default",
                    "--name", sinkName);
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStderr().contains(sinkName + " doesn't exist"));
        }
    }
}


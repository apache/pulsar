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
package org.apache.pulsar.tests.integration.io.sources;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

import static org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.JAVAJAR;
import static org.apache.pulsar.tests.integration.suites.PulsarTestSuite.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Source Property related test cases.
 */
@Slf4j
public class PulsarSourcePropertyTest extends PulsarStandaloneTestSuite {
    @Test(groups = {"source"})
    public void testSourceProperty() throws Exception {
        String outputTopicName = "test-source-property-input-" + randomName(8);
        String sourceName = "test-source-property-" + randomName(8);
        submitSourceConnector(sourceName, outputTopicName, "org.apache.pulsar.tests.integration.io.TestPropertySource",  JAVAJAR);

        // get source info
        getSourceInfoSuccess(sourceName);

        // get source status
        getSourceStatus(sourceName);

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {

            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                SourceStatus status = admin.sources().getSourceStatus("public", "default", sourceName);
                assertEquals(status.getInstances().size(), 1);
                assertTrue(status.getInstances().get(0).getStatus().numWritten > 0);
            });
        }

        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(container.getPlainTextServiceUrl())
                .build();
        @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(outputTopicName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("test-sub")
                .subscribe();

        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive();
            assertEquals(msg.getValue(), "property");
            assertEquals(msg.getProperty("hello"), "world");
            assertEquals(msg.getProperty("foo"), "bar");
        }

        // delete source
        deleteSource(sourceName);

        getSourceInfoNotFound(sourceName);
    }

    private void submitSourceConnector(String sourceName,
                                       String outputTopicName,
                                       String className,
                                       String archive) throws Exception {
        String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "sources", "create",
                "--name", sourceName,
                "--destinationTopicName", outputTopicName,
                "--archive", archive,
                "--classname", className
        };
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = container.execCmd(commands);
        assertTrue(
                result.getStdout().contains("\"Created successfully\""),
                result.getStdout());
    }

    private void getSourceInfoSuccess(String sourceName) throws Exception {
        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "sources",
                "get",
                "--tenant", "public",
                "--namespace", "default",
                "--name", sourceName
        );
        assertTrue(result.getStdout().contains("\"name\": \"" + sourceName + "\""));
    }

    private void getSourceStatus(String sourceName) throws Exception {
        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "sources",
                "status",
                "--tenant", "public",
                "--namespace", "default",
                "--name", sourceName
        );
        assertTrue(result.getStdout().contains("\"running\" : true"));
    }

    private void deleteSource(String sourceName) throws Exception {
        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "sources",
                "delete",
                "--tenant", "public",
                "--namespace", "default",
                "--name", sourceName
        );
        assertTrue(result.getStdout().contains("Delete source successfully"));
        result.assertNoStderr();
    }

    private void getSourceInfoNotFound(String sourceName) throws Exception {
        try {
            container.execCmd(
                    PulsarCluster.ADMIN_SCRIPT,
                    "sources",
                    "get",
                    "--tenant", "public",
                    "--namespace", "default",
                    "--name", sourceName);
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStderr().contains("Reason: Source " + sourceName + " doesn't exist"));
        }
    }
}


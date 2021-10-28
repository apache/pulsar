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
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.tests.integration.containers.StandaloneContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.JAVAJAR;
import static org.apache.pulsar.tests.integration.suites.PulsarTestSuite.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * This tests verifies that a batch source can be successfully submitted and run via the pulsar-admin CLI
 */
@Slf4j
public class BatchSourceTest extends PulsarStandaloneTestSuite {

    private static final String BATCH_CONFIG = "{\"discoveryTriggererConfig\": {\"__CRON__\": \"* * * * * *\"}, " +
            "\"discoveryTriggererClassName\": \"org.apache.pulsar.io.batchdiscovery.CronTriggerer\"}";

    @Test(groups = {"source"})
    public void testGenericRecordSource() throws Exception {
        String outputTopicName = "test-state-source-output-" + randomName(8);
        String sourceName = "test-state-source-" + randomName(8);
        int numMessages = 10;
        try {
            submitSourceConnector(
                    sourceName,
                    outputTopicName,
                    "builtin://batch-data-generator");

            // get source info
            getSourceInfoSuccess(container, sourceName);

            // get source status
            getSourceStatus(container, sourceName);

            try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {

                retryStrategically((test) -> {
                    try {
                        SourceStatus status = admin.sources().getSourceStatus("public", "default", sourceName);
                        return status.getInstances().size() > 0
                                && status.getInstances().get(0).getStatus().numWritten >= 10;
                    } catch (PulsarAdminException e) {
                        return false;
                    }
                }, 10, 200);

                SourceStatus status = admin.sources().getSourceStatus("public", "default", sourceName);
                assertEquals(status.getInstances().size(), 1);
                assertTrue(status.getInstances().get(0).getStatus().numWritten >= 10);
            }

            consumeMessages(container, outputTopicName, numMessages);

            // delete source
            deleteSource(container, sourceName);

            getSourceInfoNotFound(container, sourceName);
        } finally {
            dumpFunctionLogs(sourceName);
        }

    }

    private void submitSourceConnector(String sourceName,
                                       String outputTopicName,
                                       String archive) throws Exception {
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "sources", "create",
            "--name", sourceName,
            "--destinationTopicName", outputTopicName,
            "--archive", archive,
            "--batch-source-config", BATCH_CONFIG
        };
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = container.execCmd(commands);
        assertTrue(
            result.getStdout().contains("Created successfully"),
            result.getStdout());
    }

    private static void getSourceInfoSuccess(StandaloneContainer container, String sourceName) throws Exception {
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

    private static void getSourceStatus(StandaloneContainer container,String sourceName) throws Exception {
        retryStrategically((test) -> {
                    try {
                        ContainerExecResult result = container.execCmd(
                                PulsarCluster.ADMIN_SCRIPT,
                                "sources",
                                "status",
                                "--tenant", "public",
                                "--namespace", "default",
                                "--name", sourceName);

                        if (result.getStdout().contains("\"running\" : true")) {
                            return true;
                        }
                        return false;
                    } catch (Exception e) {
                        log.error("Encountered error when getting source status", e);
                        return false;
                    }
                }, 10, 200);

        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "sources",
                "status",
                "--tenant", "public",
                "--namespace", "default",
                "--name", sourceName);

        Assert.assertTrue(result.getStdout().contains("\"running\" : true"));
    }

    private static void consumeMessages(StandaloneContainer container, String outputTopic,
                                        int numMessages) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(container.getPlainTextServiceUrl())
            .build();

        // read using Pulsar GenericRecord abstraction
        @Cleanup
        Consumer<GenericRecord> consumer = client.newConsumer(Schema.AUTO_CONSUME())
            .topic(outputTopic)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionName("test-sub")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .startMessageIdInclusive()
            .subscribe();

        for (int i = 0; i < numMessages; i++) {
            Message<GenericRecord> msg = consumer.receive(10, TimeUnit.SECONDS);
            if (msg == null) {
                fail("message "+i+" not received in time");
                return;
            }
            log.info("received {}", msg.getValue());
            msg.getValue().getFields().forEach( f -> {
                log.info("field {} {}", f, msg.getValue().getField(f));
            });
        }
    }

    private static void deleteSource(StandaloneContainer container, String sourceName) throws Exception {
        ContainerExecResult result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "sources",
            "delete",
            "--tenant", "public",
            "--namespace", "default",
            "--name", sourceName
        );
        assertTrue(result.getStdout().contains("Delete source successfully"));
        assertTrue(result.getStderr().isEmpty());
    }

    private static void getSourceInfoNotFound(StandaloneContainer container, String sourceName) throws Exception {
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

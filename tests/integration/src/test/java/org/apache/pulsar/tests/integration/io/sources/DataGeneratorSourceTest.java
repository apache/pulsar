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
package org.apache.pulsar.tests.integration.io.sources;

import static org.apache.pulsar.tests.integration.suites.PulsarTestSuite.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.BatchingConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tests.integration.containers.StandaloneContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * This tests verifies that a batch source can be successfully submitted and run via the pulsar-admin CLI.
 */
@Slf4j
public class DataGeneratorSourceTest extends PulsarStandaloneTestSuite {

    @Test(groups = {"source"})
    public void testSource() throws Exception {
        testGenericRecordSource(null);
    }

    @Test(groups = {"source"})
    public void testSourceCustomBatching() throws Exception {
        BatchingConfig config = BatchingConfig.builder()
                .enabled(true)
                .batchingMaxPublishDelayMs(5)
                .roundRobinRouterBatchingPartitionSwitchFrequency(10)
                .batchingMaxMessages(10)
                .batchingMaxBytes(32 * 1024)
                .batchBuilder("KEY_BASED")
                .build();
        testGenericRecordSource(config);
    }

    @Test(groups = {"source"})
    public void testSourceDisableBatching() throws Exception {
        BatchingConfig config = BatchingConfig.builder()
                .enabled(false)
                .build();
        testGenericRecordSource(config);
    }

    public void testGenericRecordSource(BatchingConfig config) throws Exception {
        String outputTopicName = "test-state-source-output-" + randomName(8);
        String sourceName = "test-state-source-" + randomName(8);
        int numMessages = 10;
        try {
            ProducerConfig producerConfig = null;
            if (config != null) {
                producerConfig = ProducerConfig.builder()
                        .batchingConfig(config)
                        .build();
            }
            submitSourceConnector(
                    sourceName,
                    outputTopicName,
                    "builtin://data-generator",
                    producerConfig);

            // get source info
            String info = getSourceInfoSuccess(container, sourceName);
            SourceConfig sourceConfig =
                    ObjectMapperFactory.getMapper().getObjectMapper().readValue(info, SourceConfig.class);
            // checking batching config is applied
            checkBatchingConfig(sourceName, config, sourceConfig);

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

            // delete source
            deleteSource(container, sourceName);

            getSourceInfoNotFound(container, sourceName);
        } finally {
            dumpFunctionLogs(sourceName);
        }
    }

    private void submitSourceConnector(String sourceName,
                                       String outputTopicName,
                                       String archive,
                                       ProducerConfig producerConfig) throws Exception {
        List<String> commands = new ArrayList<>(List.of(
                PulsarCluster.ADMIN_SCRIPT,
                "sources", "create",
                "--name", sourceName,
                "--destinationTopicName", outputTopicName,
                "--archive", archive
        ));
        if (producerConfig != null) {
            commands.add("--producer-config");
            commands.add(new Gson().toJson(producerConfig));
        }
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = container.execCmd(commands.toArray(new String[0]));
        assertTrue(
                result.getStdout().contains("Created successfully"),
                result.getStdout());
    }

    private static String getSourceInfoSuccess(StandaloneContainer container, String sourceName) throws Exception {
        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "sources",
                "get",
                "--tenant", "public",
                "--namespace", "default",
                "--name", sourceName
        );
        assertTrue(result.getStdout().contains("\"name\": \"" + sourceName + "\""));
        return result.getStdout();
    }

    private static void getSourceStatus(StandaloneContainer container, String sourceName) throws Exception {
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

    // checking batching config, we can only check this by checking the logs for now
    private void checkBatchingConfig(String functionName, BatchingConfig config, SourceConfig sourceConfig) {
        if (config != null) {
            assertNotNull(sourceConfig.getProducerConfig());
            assertNotNull(sourceConfig.getProducerConfig().getBatchingConfig());
            assertEquals(config.toString(), sourceConfig.getProducerConfig().getBatchingConfig().toString());
        }

        String functionLogs = getFunctionLogs(functionName);
        if (config == null || config.isEnabled()) {
            BatchingConfig finalConfig = config;
            if (finalConfig == null) {
                finalConfig = BatchingConfig.builder().build();
            }
            assertTrue(functionLogs.contains(finalConfig.toString()));
        } else {
            assertTrue(functionLogs.contains("BatchingConfig(enabled=false"));
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

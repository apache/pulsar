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

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

import static org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.JAVAJAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Test behaviour of simple sinks
 */
@Slf4j
public class PulsarKeyValueSinkTest extends PulsarStandaloneTestSuite {

    @Test(groups = {"sink"})
    public void testSourceProperty() throws Exception {
        String outputTopicName = "test-kv-sink-input-" + randomName(8);
        String sinkName = "test-kv-sink-" + randomName(8);
        submitSinkConnector(sinkName, outputTopicName, "org.apache.pulsar.tests.integration.io.TestKeyValueSink",  JAVAJAR);

        // get sink info
        getSinkInfoSuccess(sinkName);

        // get source status
        getSinkStatus(sinkName);

        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(container.getPlainTextServiceUrl())
                .build();
        @Cleanup Producer<KeyValue<String, Integer>> producer = client.newProducer(Schema.KeyValue(String.class, Integer.class))
                .topic(outputTopicName)
                .create();

        producer.send(new KeyValue<>("foo", 1234));
        producer.send(new KeyValue<>("foo", 1234));
        producer.send(new KeyValue<>("foo", 1234));
        producer.send(new KeyValue<>("foo", 1234));

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {

            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                SinkStatus status = admin.sinks().getSinkStatus("public", "default", sinkName);
                assertEquals(status.getInstances().size(), 1);
                assertTrue(status.getInstances().get(0).getStatus().numReadFromPulsar > 0);
                assertTrue(status.getInstances().get(0).getStatus().numSinkExceptions == 0);
                assertTrue(status.getInstances().get(0).getStatus().numSystemExceptions == 0);
            });
        }


        // delete source
        deleteSink(sinkName);

        getSinkInfoNotFound(sinkName);
    }

    private void submitSinkConnector(String sinkName,
                                     String inputTopicName,
                                     String className,
                                     String archive) throws Exception {
        String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "sinks", "create",
                "--name", sinkName,
                "-i", inputTopicName,
                "--archive", archive,
                "--classname", className
        };
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = container.execCmd(commands);
        assertTrue(
                result.getStdout().contains("\"Created successfully\""),
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


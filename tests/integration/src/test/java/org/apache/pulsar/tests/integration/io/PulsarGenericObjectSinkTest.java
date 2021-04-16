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

import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
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

import java.util.Arrays;
import java.util.List;

import static org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.JAVAJAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Test behaviour of simple sinks
 */
@Slf4j
public class PulsarGenericObjectSinkTest extends PulsarStandaloneTestSuite {

    private static final class SinkSpec<T> {
        final String outputTopicName;
        final String sinkName;
        final Schema<T> schema;
        final T testValue;

        public SinkSpec(String outputTopicName, String sinkName, Schema<T> schema, T testValue) {
            this.outputTopicName = outputTopicName;
            this.sinkName = sinkName;
            this.schema = schema;
            this.testValue = testValue;
        }
    }

    @Data
    @Builder
    public static final class Pojo {
        private String field1;
        private int field2;
    }

    @Test(groups = {"sink"})
    public void testGenericObjectSink() throws Exception {
        // we are not using a parametrized test in order to save resources
        // we create N sinks, send the records and verify each sink
        // sinks execution happens in parallel
        List<SinkSpec> specs = Arrays.asList(
                new SinkSpec("test-kv-sink-input-string-" + randomName(8), "test-kv-sink-string-" + randomName(8), Schema.STRING, "foo"),
                new SinkSpec("test-kv-sink-input-int-" + randomName(8), "test-kv-sink-int-" + randomName(8), Schema.INT32, 123),
                new SinkSpec("test-kv-sink-input-avro-" + randomName(8), "test-kv-sink-avro-" + randomName(8), Schema.AVRO(Pojo.class), Pojo.builder().field1("a").field2(2).build()),
                new SinkSpec("test-kv-sink-input-json-" + randomName(8), "test-kv-sink-json-" + randomName(8), Schema.JSON(Pojo.class), Pojo.builder().field1("a").field2(2).build())
        );
        // submit all sinks
        for (SinkSpec spec : specs) {
            submitSinkConnector(spec.sinkName, spec.outputTopicName, "org.apache.pulsar.tests.integration.io.TestGenericObjectSink", JAVAJAR);
        }
        // check all sinks
        for (SinkSpec spec : specs) {
            // get sink info
            getSinkInfoSuccess(spec.sinkName);
            getSinkStatus(spec.sinkName);
        }

        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(container.getPlainTextServiceUrl())
                .build();

        final int numRecords = 10;

        for (SinkSpec spec : specs) {
            @Cleanup Producer<Object> producer = client.newProducer(spec.schema)
                    .topic(spec.outputTopicName)
                    .create();
            for (int i = 0; i < numRecords; i++) {
                producer.send(spec.testValue);
            }
        }

        // wait that all sinks processed all records without errors
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {

            for (SinkSpec spec : specs) {
                Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                    SinkStatus status = admin.sinks().getSinkStatus("public", "default", spec.sinkName);
                    assertEquals(status.getInstances().size(), 1);
                    assertTrue(status.getInstances().get(0).getStatus().numReadFromPulsar >= numRecords);
                    assertTrue(status.getInstances().get(0).getStatus().numSinkExceptions == 0);
                    assertTrue(status.getInstances().get(0).getStatus().numSystemExceptions == 0);
                });
            }
        }


        for (SinkSpec spec : specs) {
            deleteSink(spec.sinkName);
            getSinkInfoNotFound(spec.sinkName);
        }
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


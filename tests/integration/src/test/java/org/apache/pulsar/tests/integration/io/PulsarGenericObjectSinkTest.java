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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    @Data
    @Builder
    public static final class PojoKey {
        private String field1;
    }

    @Test(groups = {"sink"})
    public void testGenericObjectSink() throws Exception {

        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(container.getPlainTextServiceUrl())
                .build();

        // we are not using a parametrized test in order to save resources
        // we create N sinks, send the records and verify each sink
        // sinks execution happens in parallel
        List<SinkSpec> specs = Arrays.asList(
                new SinkSpec("test-kv-sink-input-string-" + randomName(8), "test-kv-sink-string-" + randomName(8), Schema.STRING, "foo"),
                new SinkSpec("test-kv-sink-input-avro-" + randomName(8), "test-kv-sink-avro-" + randomName(8), Schema.AVRO(Pojo.class), Pojo.builder().field1("a").field2(2).build()),
                new SinkSpec("test-kv-sink-input-kv-string-int-" + randomName(8), "test-kv-sink-input-kv-string-int-" + randomName(8),
                        Schema.KeyValue(Schema.STRING, Schema.INT32), new KeyValue<>("foo", 123)),
                new SinkSpec("test-kv-sink-input-kv-avro-json-" + randomName(8), "test-kv-sink-input-kv-string-int-" + randomName(8),
                        Schema.KeyValue(Schema.AVRO(PojoKey.class), Schema.JSON(Pojo.class)), new KeyValue<>(PojoKey.builder().field1("a").build(), Pojo.builder().field1("a").field2(2).build()))
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

        final int numRecords = 10;

        for (SinkSpec spec : specs) {
            @Cleanup Producer<Object> producer = client.newProducer(spec.schema)
                    .topic(spec.outputTopicName)
                    .create();
            for (int i = 0; i < numRecords; i++) {
                MessageId messageId = producer.newMessage()
                        .value(spec.testValue)
                        .property("expectedType", spec.schema.getSchemaInfo().getType().toString())
                        .send();
                log.info("sent message {} {}  with ID {}", spec.testValue, spec.schema.getSchemaInfo().getType().toString(), messageId);
            }
        }

        // wait that all sinks processed all records without errors
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {
            for (SinkSpec spec : specs) {
                try {
                    log.info("waiting for sink {}", spec.sinkName);
                    for (int i = 0; i < 120; i++) {
                        SinkStatus status = admin.sinks().getSinkStatus("public", "default", spec.sinkName);
                        log.info("sink {} status {}", spec.sinkName, status);
                        assertEquals(status.getInstances().size(), 1);
                        SinkStatus.SinkInstanceStatus instance = status.getInstances().get(0);
                        if (instance.getStatus().numWrittenToSink >= numRecords) {
                            break;
                        }
                        assertTrue(instance.getStatus().numRestarts > 1, "Sink was restarted, probably an error occurred");
                        Thread.sleep(1000);
                    }

                    SinkStatus status = admin.sinks().getSinkStatus("public", "default", spec.sinkName);
                    log.info("sink {} status {}", spec.sinkName, status);
                    assertEquals(status.getInstances().size(), 1);
                    assertTrue(status.getInstances().get(0).getStatus().numWrittenToSink >= numRecords);
                    assertTrue(status.getInstances().get(0).getStatus().numSinkExceptions == 0);
                    assertTrue(status.getInstances().get(0).getStatus().numSystemExceptions == 0);
                    log.info("sink {} is okay", spec.sinkName);
                } finally {
                    dumpSinkLogs(spec);
                }
            }
        }


        for (SinkSpec spec : specs) {
            deleteSink(spec.sinkName);
            getSinkInfoNotFound(spec.sinkName);
        }
    }

    private void dumpSinkLogs(SinkSpec spec) {
        try {
            String logFile = "/pulsar/logs/functions/public/default/" + spec.sinkName + "/" + spec.sinkName + "-0.log";
            String logs = container.<String>copyFileFromContainer(logFile, (inputStream) -> {
                return IOUtils.toString(inputStream, "utf-8");
            });
            log.info("Sink {} logs {}", spec.sinkName, logs);
        } catch (Throwable err) {
            log.info("Cannot download sink {} logs", spec.sinkName, err);
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


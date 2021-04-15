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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;


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
        private String keyfield1;
    }

    @Test(groups = {"sink"})
    public void testGenericObjectSinkSubmitSinkAndThenProduce() throws Exception {
        testGenericObjectSink(false);
        testGenericObjectSink(true);
    }

    private void testGenericObjectSink(boolean precreateTopicWithSchema) throws Exception {
        // two variants of the test:
        // create the topic and set a schema -> then create the sink: the sink starts and detects the schema
        // create the sink and then produce messages -> the sink creates the topic and sets the schema
        String prefix = precreateTopicWithSchema ? "manually-created-" : "sink-created-";

        // we are not using a parametrized test in order to save resources
        // we create N sinks, send the records and verify each sink
        // sinks execution happens in parallel

        List<SinkSpec> specs = Arrays.asList(
                new SinkSpec(prefix + "test-kv-sink-input-string-" + randomName(8), prefix + "test-kv-sink-string-" + randomName(8), Schema.STRING, "foo")
//                new SinkSpec(prefix + "test-kv-sink-input-kvprimitive-" + randomName(8), prefix + "test-kv-sink-kvprimitive-inline-" + randomName(8),
//                        Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.INLINE), new KeyValue<String, Integer>("foo", 123)),
//                new SinkSpec(prefix + "test-kv-sink-input-kvavro-" + randomName(8), prefix + "test-kv-sink-kvavro-sep-" + randomName(8),
//                        Schema.KeyValue(Schema.AVRO(PojoKey.class), Schema.AVRO(Pojo.class), KeyValueEncodingType.SEPARATED),
//                        new KeyValue<PojoKey, Pojo>(PojoKey.builder().keyfield1("a").build(), Pojo.builder().field1("a").field2(2).build()))
        );

        for (SinkSpec spec : specs) {
            try {
                @Cleanup PulsarClient client = PulsarClient.builder()
                        .serviceUrl(container.getPlainTextServiceUrl())
                        .build();

                if (precreateTopicWithSchema) {
                        log.info("Pre creating producer for {} on {} in order to set the schema", spec.schema, spec.outputTopicName);
                        @Cleanup Producer<Object> producer = client.newProducer(spec.schema)
                                .topic(spec.outputTopicName)
                                .create();

                }

                submitSinkConnector(spec.sinkName, spec.outputTopicName, "org.apache.pulsar.tests.integration.io.TestGenericObjectSink", JAVAJAR);
                // get sink info
                getSinkInfoSuccess(spec.sinkName);
                getSinkStatus(spec.sinkName);

                final int numRecords = 1;

                log.info("Creating producer for {} on {}", spec.schema, spec.outputTopicName);
                @Cleanup Producer<Object> producer = client.newProducer(spec.schema)
                        .topic(spec.outputTopicName)
                        .create();
                for (int i = 0; i < numRecords; i++) {
                    producer.newMessage()
                            .value(spec.testValue)
                            .property("expectedType", spec.schema.getSchemaInfo().getType().toString())
                            .send();
                }


                // wait that the sinks processed all records without errors
                try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {

                    log.info("waiting for sink {}", spec.sinkName);

                    Awaitility.await()
                            .atMost(10, TimeUnit.MINUTES) // slow CI ?
                            .ignoreExceptions()
                            .alias("waiting for sink "+spec.sinkName)
                            .untilAsserted(() -> {
                        SinkStatus status = admin.sinks().getSinkStatus("public", "default", spec.sinkName);
                        log.info("sink {} status {}", spec.sinkName, status);
                        assertEquals(status.getInstances().size(), 1, "problem (instances) with sink "+spec.sinkName);
                        assertTrue(status.getInstances().get(0).getStatus().numReadFromPulsar >= numRecords, "problem (too few records) with sink "+spec.sinkName);
                        assertTrue(status.getInstances().get(0).getStatus().numSinkExceptions == 0, "problem (too many exceptions) with sink "+spec.sinkName);
                        assertTrue(status.getInstances().get(0).getStatus().numSystemExceptions == 0, "problem (too many system exceptions) with sink "+spec.sinkName);
                    });
                }


                    deleteSink(spec.sinkName);
                    getSinkInfoNotFound(spec.sinkName);

            } finally {
                    try {
                        String logFile = "/pulsar/logs/functions/public/default/" + spec.sinkName + "/" + spec.sinkName + "-0.log";
                        String logs = container.<String>copyFileFromContainer(logFile, (inputStream) -> {
                            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                        });
                        log.info("Sink {} logs", spec.sinkName);
                        log.info("{}", logs);
                    } catch (Throwable err) {
                        log.error("Cannot download logs for sink {}", spec.sinkName, err);
                    }
            }
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


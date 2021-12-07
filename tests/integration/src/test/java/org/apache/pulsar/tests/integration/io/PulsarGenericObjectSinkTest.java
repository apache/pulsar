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
import lombok.Getter;
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
import org.apache.pulsar.common.schema.KeyValueEncodingType;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.JAVAJAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Test behaviour of simple sinks
 */
@Slf4j
public class PulsarGenericObjectSinkTest extends PulsarStandaloneTestSuite {

    @Getter
    private static final class SinkSpec<T> {
        final String outputTopicName;
        final Schema<T> schema;
        final T testValue;

        public SinkSpec(String outputTopicName, Schema<T> schema, T testValue) {
            this.outputTopicName = outputTopicName;
            this.schema = schema;
            this.testValue = testValue;
        }
    }

    @Data
    @Builder
    public static class Pojo {
        private String field1;
        private int field2;
    }

    @Data
    @Builder
    public static class PojoV2 {
        private String field1;
        private int field2;
        private Double field3;
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

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build();

        // we are not using a parametrized test in order to save resources
        // we create one sink that listens on multiple topics, send the records and verify the sink
        List<SinkSpec> specs = Arrays.asList(
                new SinkSpec("test-kv-sink-input-string-" + randomName(8), Schema.STRING, "foo"),
                new SinkSpec("test-kv-sink-input-avro-" + randomName(8), Schema.AVRO(Pojo.class), Pojo.builder().field1("a").field2(2).build()),
                new SinkSpec("test-kv-sink-input-json-" + randomName(8), Schema.JSON(Pojo.class), Pojo.builder().field1("a").field2(2).build()),
                new SinkSpec("test-kv-sink-input-kv-string-int-" + randomName(8),
                        Schema.KeyValue(Schema.STRING, Schema.INT32), new KeyValue<>("foo", 123)),
                new SinkSpec("test-kv-sink-input-kv-avro-json-inl-" + randomName(8),
                        Schema.KeyValue(Schema.AVRO(PojoKey.class), Schema.JSON(Pojo.class), KeyValueEncodingType.INLINE), new KeyValue<>(PojoKey.builder().field1("a").build(), Pojo.builder().field1("a").field2(2).build())),
                new SinkSpec("test-kv-sink-input-kv-avro-json-sep-" + randomName(8),
                        Schema.KeyValue(Schema.AVRO(PojoKey.class), Schema.JSON(Pojo.class), KeyValueEncodingType.SEPARATED), new KeyValue<>(PojoKey.builder().field1("a").build(), Pojo.builder().field1("a").field2(2).build()))
        );

        final int numRecordsPerTopic = 2;

        String sinkName = "genericobject-sink";
        String topicNames = specs
                .stream()
                .map(SinkSpec::getOutputTopicName)
                .collect(Collectors.joining(","));
        submitSinkConnector(sinkName, topicNames, "org.apache.pulsar.tests.integration.io.TestGenericObjectSink", JAVAJAR);
        // get sink info
        getSinkInfoSuccess(sinkName);
        getSinkStatus(sinkName);


        for (SinkSpec spec : specs) {

            @Cleanup Producer<Object> producer = client.newProducer(spec.schema)
                    .topic(spec.outputTopicName)
                    .create();
            for (int i = 0; i < numRecordsPerTopic; i++) {
                MessageId messageId = producer.newMessage()
                        .value(spec.testValue)
                        .property("expectedType", spec.schema.getSchemaInfo().getType().toString())
                        .property("recordNumber", i + "")
                        .send();
                log.info("sent message {} {}  with ID {}", spec.testValue, spec.schema.getSchemaInfo().getType().toString(), messageId);
            }
        }

        // wait that sink processed all records without errors

        try {
            log.info("waiting for sink {}", sinkName);

            for (int i = 0; i < 120; i++) {
                SinkStatus status = admin.sinks().getSinkStatus("public", "default", sinkName);
                log.info("sink {} status {}", sinkName, status);
                assertEquals(status.getInstances().size(), 1);
                SinkStatus.SinkInstanceStatus instance = status.getInstances().get(0);
                if (instance.getStatus().numWrittenToSink >= numRecordsPerTopic * specs.size()
                    || instance.getStatus().numSinkExceptions > 0
                    || instance.getStatus().numSystemExceptions > 0
                    || instance.getStatus().numRestarts > 0) {
                    break;
                }
                Thread.sleep(1000);
            }

            SinkStatus status = admin.sinks().getSinkStatus("public", "default", sinkName);
            log.info("sink {} status {}", sinkName, status);
            assertEquals(status.getInstances().size(), 1);
            assertTrue(status.getInstances().get(0).getStatus().numWrittenToSink >= numRecordsPerTopic * specs.size());
            assertTrue(status.getInstances().get(0).getStatus().numSinkExceptions == 0);
            assertTrue(status.getInstances().get(0).getStatus().numSystemExceptions == 0);
            log.info("sink {} is okay", sinkName);
        } finally {
            dumpFunctionLogs(sinkName);
        }

        deleteSink(sinkName);
        getSinkInfoNotFound(sinkName);
    }

    @Test(groups = {"sink"})
    public void testGenericObjectSinkWithSchemaChange() throws Exception {

        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(container.getPlainTextServiceUrl())
                .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build();


        final int numRecords = 2;

        String sinkName = "genericobject-sink";
        String topicName = "test-genericobject-sink-schema-change";

        submitSinkConnector(sinkName, topicName, "org.apache.pulsar.tests.integration.io.TestGenericObjectSink", JAVAJAR);
        // get sink info
        getSinkInfoSuccess(sinkName);
        getSinkStatus(sinkName);

        @Cleanup Producer<byte[]> producer = client.newProducer()
                    .topic(topicName)
                    .create();
        Schema<Pojo> schemav1 = Schema.AVRO(Pojo.class);
        Pojo record1 = Pojo.builder().field1("foo").field2(23).build();
        producer.newMessage(schemav1)
                .value(record1)
                .property("expectedType", schemav1.getSchemaInfo().getType().toString())
                .property("expectedSchemaDefinition", schemav1.getSchemaInfo().getSchemaDefinition())
                .property("recordNumber", "1")
                .send();

        Schema<PojoV2> schemav2 = Schema.AVRO(PojoV2.class);
        PojoV2 record2 = PojoV2.builder().field1("foo").field2(23).field3(42.5).build();
        producer.newMessage(schemav2)
                .value(record2)
                .property("expectedType", schemav2.getSchemaInfo().getType().toString())
                .property("expectedSchemaDefinition", schemav2.getSchemaInfo().getSchemaDefinition())
                .property("recordNumber", "2")
                .send();

        // wait that sink processed all records without errors

        try {
            log.info("waiting for sink {}", sinkName);

            for (int i = 0; i < 120; i++) {
                SinkStatus status = admin.sinks().getSinkStatus("public", "default", sinkName);
                log.info("sink {} status {}", sinkName, status);
                assertEquals(status.getInstances().size(), 1);
                SinkStatus.SinkInstanceStatus instance = status.getInstances().get(0);
                if (instance.getStatus().numWrittenToSink >= numRecords
                        || instance.getStatus().numSinkExceptions > 0
                        || instance.getStatus().numSystemExceptions > 0
                        || instance.getStatus().numRestarts > 0) {
                    break;
                }
                Thread.sleep(1000);
            }

            SinkStatus status = admin.sinks().getSinkStatus("public", "default", sinkName);
            log.info("sink {} status {}", sinkName, status);
            assertEquals(status.getInstances().size(), 1);
            assertTrue(status.getInstances().get(0).getStatus().numWrittenToSink >= numRecords);
            assertTrue(status.getInstances().get(0).getStatus().numSinkExceptions == 0);
            assertTrue(status.getInstances().get(0).getStatus().numSystemExceptions == 0);
            log.info("sink {} is okay", sinkName);
        } finally {
            dumpFunctionLogs(sinkName);
        }

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


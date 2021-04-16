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
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
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
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;


/**
 * Test behaviour of Sink&lt;GenericObject&gt;
 */
@Slf4j
public class PulsarGenericObjectSinkTest extends PulsarStandaloneTestSuite {


    @Test(groups = {"sink"})
    public void testGenericObjectSink() throws Exception {
        String outputTopicName = "test-kv-genericobject-sink";
        Schema<KeyValue<String, Integer>> schema = Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.INLINE);
        String sinkName = "test-kv-genericobject-sink";
        KeyValue<String, Integer> testValue = new KeyValue<String, Integer>("foo", 123);

        try {
            @Cleanup PulsarClient client = PulsarClient.builder()
                    .serviceUrl(container.getPlainTextServiceUrl())
                    .build();

            // create the topic and set the schema
            @Cleanup Producer<KeyValue<String, Integer>> producer = client.newProducer(schema)
                    .topic(outputTopicName)
                    .create();

            submitSinkConnector(sinkName, outputTopicName, "org.apache.pulsar.tests.integration.io.TestGenericObjectSink", JAVAJAR);
            // get sink info
            getSinkInfoSuccess(sinkName);
            getSinkStatus(sinkName);

            final int numRecords = 1;


            for (int i = 0; i < numRecords; i++) {
                MessageId messageId = producer.newMessage()
                        .value(testValue)
                        .property("expectedType", schema.getSchemaInfo().getType().toString())
                        .send();
                log.info("sent message with ID {}", messageId);
            }


            // wait that the sinks processed all records without errors
            log.info("waiting for sink {}", sinkName);
            try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {
                for (int i = 0; i < 60; i++) {
                    SinkStatus status = admin.sinks().getSinkStatus("public", "default", sinkName);
                    log.info("sink {} status {}", sinkName, status);
                    if (status.getInstances().size() > 0 &&
                            status.getInstances().get(0).getStatus().numReadFromPulsar >= numRecords) {
                        break;
                    }
                    Thread.sleep(5000);
                }

                // final assertions
                SinkStatus status = admin.sinks().getSinkStatus("public", "default", sinkName);
                log.info("sink {} status {}", sinkName, status);
                assertEquals(status.getInstances().size(), 1, "problem (instances) with sink " + sinkName);
                assertTrue(status.getInstances().get(0).getStatus().numReadFromPulsar >= numRecords, "problem (too few records) with sink " + sinkName);
                assertTrue(status.getInstances().get(0).getStatus().numSinkExceptions == 0, "problem (too many exceptions) with sink " + sinkName);
                assertTrue(status.getInstances().get(0).getStatus().numSystemExceptions == 0, "problem (too many system exceptions) with sink " + sinkName);
            }


            deleteSink(sinkName);
            getSinkInfoNotFound(sinkName);

        } finally {
            try {
                String logFile = "/pulsar/logs/functions/public/default/" + sinkName + "/" + sinkName + "-0.log";
                String logs = container.<String>copyFileFromContainer(logFile, (inputStream) -> {
                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    // copy first 1MB of logs
                    for (int j = 0; j < 1024 * 1024 * 1024; j++) {
                        int r = inputStream.read();
                        if (r == -1) {
                            break;
                        }
                        oo.write(r);
                    }
                    return oo.toString(StandardCharsets.UTF_8.name());
                });
                log.info("Sink {} logs", sinkName);
                log.info("{}", logs);
            } catch (Throwable err) {
                log.error("Cannot download logs for sink {}", sinkName, err);
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
        log.info(result.getStdout());
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
        log.info(result.getStdout());
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


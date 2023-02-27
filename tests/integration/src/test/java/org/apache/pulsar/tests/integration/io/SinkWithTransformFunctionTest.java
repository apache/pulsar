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
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.examples.pojo.Users;
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
 * Test behaviour of sinks with a transform function
 */
@Slf4j
public class SinkWithTransformFunctionTest extends PulsarStandaloneTestSuite {

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
    public void testSinkWithTransformFunction() throws Exception {

        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(container.getPlainTextServiceUrl())
                .build();

        final int numRecords = 10;

        String sinkName = "sink-with-function";
        String topicName = "sink-with-function";
        String logTopicName = "log-sink-with-function";
        String packageName = "function://public/default/sink-with-function-function@1.0";

        submitPackage(packageName, "package-function", JAVAJAR);

        submitSinkConnector(
                sinkName,
                topicName,
                "org.apache.pulsar.tests.integration.io.TestLoggingSink",
                JAVAJAR,
                "{\"log-topic\": \"" + logTopicName + "\"}",
                packageName,
                "org.apache.pulsar.functions.api.examples.RecordFunction");

        getSinkInfoSuccess(sinkName);
        getSinkStatus(sinkName);

        @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topicName)
                    .create();

        @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(logTopicName)
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
                assertEquals(receive.getValue(), "STRING - " + i + "-test!");
            }
        } finally {
            dumpFunctionLogs(sinkName);
        }

        deleteSink(sinkName);
        getSinkInfoNotFound(sinkName);
    }

    @Test(groups = {"sink"})
    public void testGenericObjectSinkWithTransformFunction() throws Exception {

        @Cleanup PulsarClient client = PulsarClient.builder()
            .serviceUrl(container.getPlainTextServiceUrl())
            .build();

        final int numRecords = 10;

        String sinkName = "sink-with-genericobject-function";
        String topicName = "sink-with-genericobject-function";
        String logTopicName = "log-sink-with-genericobject-function";
        String packageName = "function://public/default/sink-with-genericobject-function-function@1.0";

        submitPackage(packageName, "package-function", JAVAJAR);

        submitSinkConnector(
            sinkName,
            topicName,
            "org.apache.pulsar.tests.integration.io.TestLoggingSink",
            JAVAJAR,
            "{\"log-topic\": \"" + logTopicName + "\"}",
            packageName,
            "org.apache.pulsar.tests.integration.functions.RemoveAvroFieldRecordFunction");

        getSinkInfoSuccess(sinkName);
        getSinkStatus(sinkName);

        try {
            @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(logTopicName)
                .subscriptionName("sub")
                .subscribe();

            @Cleanup Producer<Users.UserV1> producer1 = client.newProducer(Schema.AVRO(Users.UserV1.class))
                .topic(topicName)
                .create();

            for (int i = 0; i < numRecords; i++) {
                producer1.send(new Users.UserV1("foo" + i, i));
            }

            for (int i = 0; i < numRecords; i++) {
                Message<String> receive = consumer.receive(5, TimeUnit.SECONDS);
                assertNotNull(receive);
                assertEquals(receive.getValue(), "AVRO - {\"name\": \"foo" + i + "\"}");
            }

            // Test with schema evolution
            @Cleanup Producer<Users.UserV2> producer2 = client.newProducer(Schema.AVRO(Users.UserV2.class))
                .topic(topicName)
                .create();

            for (int i = 0; i < numRecords; i++) {
                producer2.send(new Users.UserV2("foo" + i, i, "bar" + i));
            }

            for (int i = 0; i < numRecords; i++) {
                Message<String> receive = consumer.receive(5, TimeUnit.SECONDS);
                assertNotNull(receive);
                assertEquals(receive.getValue(), "AVRO - {\"name\": \"foo" + i + "\", \"phone\": \"bar"+ i + "\"}");
            }

            for (int i = 0; i < numRecords; i++) {
                producer1.send(new Users.UserV1("foo" + i, i));
            }

            for (int i = 0; i < numRecords; i++) {
                Message<String> receive = consumer.receive(5, TimeUnit.SECONDS);
                assertNotNull(receive);
                assertEquals(receive.getValue(), "AVRO - {\"name\": \"foo" + i + "\"}");
            }
        } finally {
            dumpFunctionLogs(sinkName);
        }

        deleteSink(sinkName);
        getSinkInfoNotFound(sinkName);
    }

    @Test(groups = {"sink"})
    public void testKeyValueSinkWithTransformFunction() throws Exception {

        @Cleanup PulsarClient client = PulsarClient.builder()
            .serviceUrl(container.getPlainTextServiceUrl())
            .build();

        final int numRecords = 10;

        String sinkName = "sink-with-kv-function";
        String topicName = "sink-with-kv-function";
        String logTopicName = "log-sink-with-kv-function";
        String packageName = "function://public/default/sink-with-kv-function-function@1.0";

        submitPackage(packageName, "package-function", JAVAJAR);

        submitSinkConnector(
            sinkName,
            topicName,
            "org.apache.pulsar.tests.integration.io.TestLoggingSink",
            JAVAJAR,
            "{\"log-topic\": \"" + logTopicName + "\"}",
            packageName,
            "org.apache.pulsar.tests.integration.functions.RemoveAvroFieldRecordFunction");

        getSinkInfoSuccess(sinkName);
        getSinkStatus(sinkName);

        try {
            @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(logTopicName)
                .subscriptionName("sub")
                .subscribe();

            @Cleanup Producer<KeyValue<Users.UserV1, Users.UserV1>> producer = client
                .newProducer(Schema.KeyValue(
                    Schema.AVRO(Users.UserV1.class),
                    Schema.AVRO(Users.UserV1.class), KeyValueEncodingType.SEPARATED))
                .topic(topicName)
                .create();

            for (int i = 0; i < numRecords; i++) {
                producer.send(new KeyValue<>(new Users.UserV1("foo" + i, i),
                    new Users.UserV1("bar" + i, i + 100)));
            }

            for (int i = 0; i < numRecords; i++) {
                Message<String> receive = consumer.receive(5, TimeUnit.SECONDS);
                assertNotNull(receive);
                assertEquals(receive.getValue(), "KEY_VALUE - (key = {\"age\": " + i
                    + ", \"name\": \"foo" + i + "\"}, value = "
                    + "{\"name\": \"bar" + i + "\"})");
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
                                     String transformFunction,
                                     String transformFunctionClassName) throws Exception {
        String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "sinks", "create",
                "--name", sinkName,
                "-i", inputTopicName,
                "--archive", archive,
                "--classname", className,
                "--sink-config", configs,
                "--transform-function", transformFunction,
                "--transform-function-classname", transformFunctionClassName
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


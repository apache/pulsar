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
package org.apache.pulsar.tests.integration.functions;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.JAVAJAR;
import static org.apache.pulsar.tests.integration.suites.PulsarTestSuite.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * State related test cases.
 */
@Slf4j
public class PulsarStateTest extends PulsarStandaloneTestSuite {

    public static final String WORDCOUNT_PYTHON_CLASS =
        "wordcount_function.WordCountFunction";

    public static final String WORDCOUNT_PYTHON_FILE = "wordcount_function.py";

    @Test(groups = {"python_state", "state", "function", "python_function"})
    public void testPythonWordCountFunction() throws Exception {
        String inputTopicName = "test-wordcount-py-input-" + randomName(8);
        String outputTopicName = "test-wordcount-py-output-" + randomName(8);
        String functionName = "test-wordcount-py-fn-" + randomName(8);

        final int numMessages = 10;

        // submit the exclamation function
        submitExclamationFunction(
            Runtime.PYTHON, inputTopicName, outputTopicName, functionName);

        // get function info
        getFunctionInfoSuccess(functionName);

        // publish and consume result
        publishAndConsumeMessages(inputTopicName, outputTopicName, numMessages);

        // get function status
        getFunctionStatus(functionName, numMessages);

        // get state
        queryState(functionName, "hello", numMessages);
        queryState(functionName, "test", numMessages);
        for (int i = 0; i < numMessages; i++) {
            queryState(functionName, "message-" + i, 1);
        }

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);
    }

    @Test(groups = {"java_state", "state", "function", "java_function"})
    public void testSourceState() throws Exception {
        String outputTopicName = "test-state-source-output-" + randomName(8);
        String sourceName = "test-state-source-" + randomName(8);

        submitSourceConnector(sourceName, outputTopicName, "org.apache.pulsar.tests.integration.io.TestStateSource",  JAVAJAR);

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

            {
                FunctionState functionState =
                        admin.functions().getFunctionState("public", "default", sourceName, "initial");
                assertEquals(functionState.getStringValue(), "val1");
            }

            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                FunctionState functionState = admin.functions().getFunctionState("public", "default", sourceName, "now");
                assertTrue(functionState.getStringValue().matches("val1-.*"));
            });
        }

        // delete source
        deleteSource(sourceName);

        getSourceInfoNotFound(sourceName);
    }

    @Test(groups = {"java_state", "state", "function", "java_function"})
    public void testSinkState() throws Exception {
        String inputTopicName = "test-state-sink-input-" + randomName(8);
        String sinkName = "test-state-sink-" + randomName(8);
        int numMessages = 10;

        submitSinkConnector(sinkName, inputTopicName, "org.apache.pulsar.tests.integration.io.TestStateSink",  JAVAJAR);

        // get sink info
        getSinkInfoSuccess(sinkName);

        // get sink status
        getSinkStatus(sinkName);

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {

            // java supports schema
            @Cleanup PulsarClient client = PulsarClient.builder()
                    .serviceUrl(container.getPlainTextServiceUrl())
                    .build();
            @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(inputTopicName)
                    .create();

            {
                FunctionState functionState =
                        admin.functions().getFunctionState("public", "default", sinkName, "initial");
                assertEquals(functionState.getStringValue(), "val1");
            }

            for (int i = 0; i < numMessages; i++) {
                producer.send("foo");
            }

            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                SinkStatus status = admin.sinks().getSinkStatus("public", "default", sinkName);
                assertEquals(status.getInstances().size(), 1);
                assertTrue(status.getInstances().get(0).getStatus().numWrittenToSink > 0);
            });

            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                FunctionState functionState = admin.functions().getFunctionState("public", "default", sinkName, "now");
                assertEquals(functionState.getStringValue(), String.format("val1-%d", numMessages - 1));
            });
        }

        // delete source
        deleteSink(sinkName);

        getSinkInfoNotFound(sinkName);
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

    private void submitSinkConnector(String sinkName,
                                         String inputTopicName,
                                         String className,
                                         String archive) throws Exception {
        String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "sinks", "create",
                "--name", sinkName,
                "--inputs", inputTopicName,
                "--archive", archive,
                "--classname", className
        };
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = container.execCmd(commands);
        assertTrue(
                result.getStdout().contains("\"Created successfully\""),
                result.getStdout());
    }

    private void submitExclamationFunction(Runtime runtime,
                                                  String inputTopicName,
                                                  String outputTopicName,
                                                  String functionName) throws Exception {
        submitFunction(
            runtime,
            inputTopicName,
            outputTopicName,
            functionName,
            getExclamationClass(runtime),
            Schema.BYTES);
    }

    protected static String getExclamationClass(Runtime runtime) {
        if (Runtime.PYTHON == runtime) {
            return WORDCOUNT_PYTHON_CLASS;
        } else {
            throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }
    }

    private <T> void submitFunction(Runtime runtime,
                                           String inputTopicName,
                                           String outputTopicName,
                                           String functionName,
                                           String functionClass,
                                           Schema<T> inputTopicSchema) throws Exception {
        CommandGenerator generator;
        generator = CommandGenerator.createDefaultGenerator(inputTopicName, functionClass);
        generator.setSinkTopic(outputTopicName);
        generator.setFunctionName(functionName);
        String command;
        if (Runtime.JAVA == runtime) {
            command = generator.generateCreateFunctionCommand();
        } else if (Runtime.PYTHON == runtime) {
            generator.setRuntime(runtime);
            command = generator.generateCreateFunctionCommand(WORDCOUNT_PYTHON_FILE);
        } else {
            throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }
        String[] commands = {
            "sh", "-c", command
        };
        ContainerExecResult result = container.execCmd(
            commands);
        assertTrue(result.getStdout().contains("\"Created successfully\""));

        ensureSubscriptionCreated(inputTopicName, String.format("public/default/%s", functionName), inputTopicSchema);
    }

    private <T> void ensureSubscriptionCreated(String inputTopicName,
                                                      String subscriptionName,
                                                      Schema<T> inputTopicSchema)
            throws Exception {
        // ensure the function subscription exists before we start producing messages
        try (PulsarClient client = PulsarClient.builder()
            .serviceUrl(container.getPlainTextServiceUrl())
            .build()) {
            try (Consumer<T> ignored = client.newConsumer(inputTopicSchema)
                .topic(inputTopicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subscriptionName)
                .subscribe()) {
            }
        }
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

    private void getFunctionInfoSuccess(String functionName) throws Exception {
        ContainerExecResult result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "get",
            "--tenant", "public",
            "--namespace", "default",
            "--name", functionName
        );
        assertTrue(result.getStdout().contains("\"name\": \"" + functionName + "\""));
    }

    private void getFunctionInfoNotFound(String functionName) throws Exception {
        try {
            container.execCmd(
                    PulsarCluster.ADMIN_SCRIPT,
                    "functions",
                    "get",
                    "--tenant", "public",
                    "--namespace", "default",
                    "--name", functionName);
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStderr().contains("Reason: Function " + functionName + " doesn't exist"));
        }
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

    private void getFunctionStatus(String functionName, int numMessages) throws Exception {
        ContainerExecResult result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "getstatus",
            "--tenant", "public",
            "--namespace", "default",
            "--name", functionName
        );
        assertTrue(result.getStdout().contains("\"running\" : true"));
        assertTrue(result.getStdout().contains("\"numSuccessfullyProcessed\" : " + numMessages));
    }

    private void queryState(String functionName, String key, int amount)
        throws Exception {
        ContainerExecResult result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "querystate",
            "--tenant", "public",
            "--namespace", "default",
            "--name", functionName,
            "--key", key
        );
        assertTrue(result.getStdout().contains("\"numberValue\": " + amount));
    }

    private void publishAndConsumeMessages(String inputTopic,
                                                  String outputTopic,
                                                  int numMessages) throws Exception {
        @Cleanup PulsarClient client = PulsarClient.builder()
            .serviceUrl(container.getPlainTextServiceUrl())
            .build();
        @Cleanup Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
            .topic(outputTopic)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionName("test-sub")
            .subscribe();
        @Cleanup Producer<byte[]> producer = client.newProducer(Schema.BYTES)
            .topic(inputTopic)
            .create();

        for (int i = 0; i < numMessages; i++) {
            producer.send(("hello test message-" + i).getBytes(UTF_8));
        }

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive();
            assertEquals("hello test message-" + i + "!", new String(msg.getValue(), UTF_8));
        }
    }

    private void deleteFunction(String functionName) throws Exception {
        ContainerExecResult result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "delete",
            "--tenant", "public",
            "--namespace", "default",
            "--name", functionName
        );
        assertTrue(result.getStdout().contains("Deleted successfully"));
        result.assertNoStderr();
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

    private void deleteSink(String sinkName) throws Exception {
        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "sinks",
                "delete",
                "--tenant", "public",
                "--namespace", "default",
                "--name", sinkName
        );
        assertTrue(result.getStdout().contains("Deleted successfully"));
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
            assertTrue(e.getResult().getStderr().contains("Reason: Sink " + sinkName + " doesn't exist"));
        }
    }

}

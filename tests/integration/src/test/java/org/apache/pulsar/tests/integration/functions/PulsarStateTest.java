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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.annotations.Test;

/**
 * State related test cases.
 */
public class PulsarStateTest extends PulsarStandaloneTestSuite {

    public static final String WORDCOUNT_PYTHON_CLASS =
        "wordcount_function.WordCountFunction";

    public static final String WORDCOUNT_PYTHON_FILE = "wordcount_function.py";


    @Test
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

    private static void submitExclamationFunction(Runtime runtime,
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

    private static <T> void submitFunction(Runtime runtime,
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

    private static <T> void ensureSubscriptionCreated(String inputTopicName,
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

    private static void getFunctionInfoSuccess(String functionName) throws Exception {
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

    private static void getFunctionInfoNotFound(String functionName) throws Exception {
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

    private static void getFunctionStatus(String functionName, int numMessages) throws Exception {
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

    private static void queryState(String functionName, String key, int amount)
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

    private static void publishAndConsumeMessages(String inputTopic,
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

    private static void deleteFunction(String functionName) throws Exception {
        ContainerExecResult result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "delete",
            "--tenant", "public",
            "--namespace", "default",
            "--name", functionName
        );
        assertTrue(result.getStdout().contains("Deleted successfully"));
        assertTrue(result.getStderr().isEmpty());
    }

}

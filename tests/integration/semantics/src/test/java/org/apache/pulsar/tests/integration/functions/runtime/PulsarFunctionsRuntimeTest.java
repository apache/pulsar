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
package org.apache.pulsar.tests.integration.functions.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTestBase;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.topologies.PulsarCluster;
import org.testcontainers.containers.Container.ExecResult;
import org.testng.annotations.Test;

/**
 * The tests that run over different container mode.
 */
public abstract class PulsarFunctionsRuntimeTest extends PulsarFunctionsTestBase {

    public enum RuntimeFactory {
        PROCESS,
        THREAD
    }

    private final RuntimeFactory runtimeFactory;

    public PulsarFunctionsRuntimeTest(RuntimeFactory runtimeFactory) {
        this.runtimeFactory = runtimeFactory;
    }

    //
    // Test CRUD functions on different runtimes.
    //

    @Test(dataProvider = "FunctionRuntimes")
    public void testExclamationFunction(Runtime runtime) throws Exception {
        if (runtimeFactory == RuntimeFactory.THREAD && runtime == Runtime.PYTHON) {
            // python can only run on process mode
            return;
        }

        String inputTopicName = "test-exclamation-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-exclamation-" + runtime + "-output-" + randomName(8);
        String functionName = "test-exclamation-fn-" + randomName(8);
        final int numMessages = 10;

        // submit the exclamation function
        submitExclamationFunction(
            runtime, inputTopicName, outputTopicName, functionName);

        // get function info
        getFunctionInfoSuccess(functionName);

        // publish and consume result
        publishAndConsumeMessages(inputTopicName, outputTopicName, numMessages);

        // get function status
        getFunctionStatus(functionName, numMessages);

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);
    }

    private static void submitExclamationFunction(Runtime runtime,
                                                  String inputTopicName,
                                                  String outputTopicName,
                                                  String functionName) throws Exception {
        CommandGenerator generator;
        generator = CommandGenerator.createDefaultGenerator(inputTopicName, getExclamationClass(runtime));
        generator.setSinkTopic(outputTopicName);
        generator.setFunctionName(functionName);
        String command;
        if (Runtime.JAVA == runtime) {
            command = generator.generateCreateFunctionCommand();
        } else if (Runtime.PYTHON == runtime) {
            generator.setRuntime(runtime);
            command = generator.generateCreateFunctionCommand(EXCLAMATION_PYTHON_FILE);
        } else {
            throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }
        String[] commands = {
            "sh", "-c", command
        };
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(
            commands);
        assertTrue(result.getStdout().contains("\"Created successfully\""));
    }

    private static void getFunctionInfoSuccess(String functionName) throws Exception {
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(
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
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "get",
            "--tenant", "public",
            "--namespace", "default",
            "--name", functionName
        );
        assertTrue(result.getStderr().contains("Reason: Function " + functionName + " doesn't exist"));
    }

    private static void getFunctionStatus(String functionName, int numMessages) throws Exception {
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "getstatus",
            "--tenant", "public",
            "--namespace", "default",
            "--name", functionName
        );
        assertTrue(result.getStdout().contains("\"running\": true"));
        assertTrue(result.getStdout().contains("\"numProcessed\": \"" + numMessages + "\""));
        assertTrue(result.getStdout().contains("\"numSuccessfullyProcessed\": \"" + numMessages + "\""));
    }

    private static void publishAndConsumeMessages(String inputTopic,
                                                  String outputTopic,
                                                  int numMessages) throws Exception {
        @Cleanup PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build();
        @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic(outputTopic)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionName("test-sub")
            .subscribe();
        @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
            .topic(inputTopic)
            .create();

        for (int i = 0; i < numMessages; i++) {
            producer.send("message-" + i);
        }

        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive();
            assertEquals("message-" + i + "!", msg.getValue());
        }
    }

    private static void deleteFunction(String functionName) throws Exception {
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(
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

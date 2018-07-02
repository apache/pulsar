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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.containers.WorkerContainer;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.functions.utils.UploadDownloadCommandGenerator;
import org.apache.pulsar.tests.topologies.PulsarCluster;
import org.testcontainers.containers.Container.ExecResult;
import org.testng.annotations.Test;

@Slf4j
public class PulsarFunctionsTest extends PulsarFunctionsTestBase {

    //
    // Tests on uploading/downloading function packages.
    //

    @Test
    public String checkUpload() throws Exception {
        String bkPkgPath = String.format("%s/%s/%s",
            "tenant-" + randomName(8),
            "ns-" + randomName(8),
            "fn-" + randomName(8));

        UploadDownloadCommandGenerator generator = UploadDownloadCommandGenerator.createUploader(
            PulsarCluster.ADMIN_SCRIPT,
            bkPkgPath);
        String actualCommand = generator.generateCommand();

        log.info(actualCommand);

        String[] commands = {
            "sh", "-c", actualCommand
        };
        ExecResult output = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(output.getStdout().contains("\"Uploaded successfully\""));
        return bkPkgPath;
    }

    @Test
    public void checkDownload() throws Exception {
        String bkPkgPath = checkUpload();
        String localPkgFile = "/tmp/checkdownload-" + randomName(16);

        UploadDownloadCommandGenerator generator = UploadDownloadCommandGenerator.createDownloader(
                localPkgFile,
                bkPkgPath);
        String actualCommand = generator.generateCommand();

        log.info(actualCommand);

        String[] commands = {
            "sh", "-c", actualCommand
        };
        WorkerContainer container = pulsarCluster.getAnyWorker();
        ExecResult output = container.execCmd(commands);
        assertTrue(output.getStdout().contains("\"Downloaded successfully\""));
        String[] diffCommand = {
            "diff",
            PulsarCluster.ADMIN_SCRIPT,
            localPkgFile
        };
        output = container.execCmd(diffCommand);
        assertTrue(output.getStdout().isEmpty());
        assertTrue(output.getStderr().isEmpty());
    }

    //
    // Test CRUD functions on different runtimes.
    //

    @Test(dataProvider = "FunctionRuntimes")
    public void testExclamationFunction(Runtime runtime) throws Exception {
        String inputTopicName = "test-exclamation-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-exclamation-" + runtime + "-output-" + randomName(8);
        String functionName = "test-exclamation-fn-" + randomName(8);
        final int numMessages = 10;

        // submit the exclamation function
        submitExclamationFunction(
            inputTopicName, outputTopicName, functionName);

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

    private static void submitExclamationFunction(String inputTopicName,
                                                  String outputTopicName,
                                                  String functionName) throws Exception {
        CommandGenerator generator = CommandGenerator.createDefaultGenerator(inputTopicName, EXCLAMATION_FUNC_CLASS);
        generator.setSinkTopic(outputTopicName);
        generator.setFunctionName(functionName);
        String command = generator.generateCreateFunctionCommand();
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

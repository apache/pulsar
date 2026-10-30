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
package org.apache.pulsar.tests.integration.functions;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.JAVAJAR;
import static org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.PYTHONBASE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import com.google.common.base.Utf8;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.functions.api.examples.WordCountFunction;
import org.apache.pulsar.functions.instance.state.PulsarMetadataStateStoreProviderImpl;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.testng.annotations.Test;

/**
 * State related test cases.
 */
public abstract class PulsarStateTest extends PulsarStandaloneTestSuite {

    protected String stateStoreProvider;

    protected PulsarStateTest(String stateStoreProvider) {
        super();
        this.stateStoreProvider = stateStoreProvider;
    }

    public static final String WORDCOUNT_PYTHON_CLASS =
            "wordcount_function.WordCountFunction";
    public static final String WORDCOUNT_JAVA_CLASS = WordCountFunction.class.getName();

    public static final String WORDCOUNT_PYTHON_FILE = "wordcount_function.py";

    // The memory that CommandGenerator gives a Java function with --ram
    private static final long JAVA_FUNCTION_RAM_BYTES = 128 * 1024 * 1024;

    public static final String VALUE_BASE64 = "0a8001127e0a172e6576656e74732e437573746f6d65724372656174656412630a243"
            + "2336366666263652d623038342d346631352d616565342d326330643135356131666"
            + "36312026e311a3700000000000000000000000000000000000000000000000000000"
            + "000000000000000000000000000000000000000000000000000000000";

    private void doTestPythonWordCountFunction(String functionName, boolean checkStateWithCli) throws Exception {
        String inputTopicName = "test-wordcount-py-input-" + randomName(8);
        String outputTopicName = "test-wordcount-py-output-" + randomName(8);

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {
            // submit the word count function
            submitWordCountFunction(admin, Runtime.PYTHON, inputTopicName, outputTopicName, functionName);
            // run tests
            doTestFunction(admin, functionName, inputTopicName, outputTopicName, checkStateWithCli);
        }
    }

    private void doTestJavaWordCountFunction(String functionName, boolean checkStateWithCli) throws Exception {
        String inputTopicName = "test-wordcount-java-input-" + randomName(8);

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {
            // submit the word count function
            submitWordCountFunction(admin, Runtime.JAVA, inputTopicName, null, functionName);
            // run tests
            doTestFunction(admin, functionName, inputTopicName, null, checkStateWithCli);
        }
    }

    // Checks through the REST API rather than pulsar-admin, which starts a JVM in the container for every command
    // (seconds each). With checkStateWithCli, the byte state is put and queried through pulsar-admin.
    private void doTestFunction(PulsarAdmin admin, String functionName, String inputTopicName, String outputTopicName,
                                boolean checkStateWithCli) throws Exception {
        final int numMessages = 10;
        // get function info
        getFunctionInfoSuccess(admin, functionName);

        // publish and consume result
        publishAndConsumeMessages(inputTopicName, outputTopicName, numMessages);

        // get function status
        getFunctionStatus(admin, functionName, numMessages);

        // get state
        queryState(admin, functionName, "hello", numMessages, numMessages - 1);
        queryState(admin, functionName, "test", numMessages, numMessages - 1);
        for (int i = 0; i < numMessages; i++) {
            queryState(admin, functionName, "message-" + i, 1, 0);
        }

        // test put state
        FunctionState stringState = putAndQueryState(admin, functionName,
                FunctionState.builder().key("test-string").stringValue("test value").build());
        assertEquals(stringState.getStringValue(), "test value");

        FunctionState numberState = putAndQueryState(admin, functionName,
                FunctionState.builder().key("test-number").numberValue(20L).build());
        assertEquals(numberState.getNumberValue(), Long.valueOf(20));

        byte[] valueBytes = Base64.getDecoder().decode(VALUE_BASE64);
        if (checkStateWithCli) {
            // pulsar-admin decodes the base64 byteValue of the JSON state and prints the bytes back
            String bytesString = Base64.getEncoder().encodeToString(valueBytes);
            String byteState = "{\"key\":\"test-bytes\",\"byteValue\":\"" + bytesString + "\"}";
            putAndQueryStateByte(functionName, "test-bytes", byteState, valueBytes);
        } else {
            FunctionState byteState = putAndQueryState(admin, functionName,
                    FunctionState.builder().key("test-bytes").byteValue(valueBytes).build());
            assertNull(byteState.getStringValue());
            assertEquals(byteState.getByteValue(), valueBytes);
        }

        String valueStr = "hello pulsar";
        byte[] valueStrBytes = valueStr.getBytes(UTF_8);
        FunctionState strBytesState = putAndQueryState(admin, functionName,
                FunctionState.builder().key("test-str-bytes").byteValue(valueStrBytes).build());
        assertEquals(strBytesState.getStringValue(), valueStr);

        FunctionState strBytesStateWithEmptyValues = putAndQueryState(admin, functionName,
                FunctionState.builder().key("test-str-bytes").byteValue(valueStrBytes)
                        .stringValue("").numberValue(0L).build());
        assertEquals(strBytesStateWithEmptyValues.getStringValue(), valueStr);

        // delete function
        deleteFunction(admin, functionName);

        // get function info
        getFunctionInfoNotFound(admin, functionName);

        // The worker terminates the instance after the delete returns, and deletes its subscription last. A function
        // created again with the same name before that is taken for an update of this one and stopped.
        String subscription = String.format("public/default/%s", functionName);
        Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() ->
                assertFalse(admin.topics().getSubscriptions(inputTopicName).contains(subscription)));
    }

    @Test(groups = {"python_state", "state", "function", "python_function"})
    public void testPythonWordCountFunction() throws Exception {
        if (PulsarMetadataStateStoreProviderImpl.class.getName().equals(stateStoreProvider)) {
            // python function doesn't support metadata state store yet
            return;
        }
        String functionName = "test-wordcount-py-fn-" + randomName(8);
        doTestPythonWordCountFunction(functionName, true);

        // after a function is deleted, its state should be clean
        // we just recreate and test the word count function again, and it should have same result
        doTestPythonWordCountFunction(functionName, false);
    }

    @Test(groups = {"java_state", "state", "function", "java_function"})
    public void testJavaWordCountFunction() throws Exception {
        String functionName = "test-wordcount-java-fn-" + randomName(8);
        doTestJavaWordCountFunction(functionName, true);

        // after a function is deleted, its state should be clean
        // we just recreate and test the word count function again, and it should have same result
        doTestJavaWordCountFunction(functionName, false);
    }


    @Test(groups = {"java_state", "state", "function", "java_function"})
    public void testSourceState() throws Exception {
        String outputTopicName = "test-state-source-output-" + randomName(8);
        String sourceName = "test-state-source-" + randomName(8);

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {
            submitSourceConnector(admin, sourceName, outputTopicName,
                    "org.apache.pulsar.tests.integration.io.TestStateSource", JAVAJAR);

            // get source info
            getSourceInfoSuccess(admin, sourceName);

            // get source status
            getSourceStatus(admin, sourceName);

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

            // query a non-exist key should get a 404 error
            {
                PulsarAdminException e = expectThrows(PulsarAdminException.class, () -> {
                    admin.functions().getFunctionState("public", "default", sourceName, "non-exist");
                });
                assertEquals(e.getStatusCode(), 404);
            }

            // query a non-exist instance should get a 404 error
            {
                PulsarAdminException e = expectThrows(PulsarAdminException.class, () -> {
                    admin.functions().getFunctionState("public", "default", "non-exist", "non-exist");
                });
                assertEquals(e.getStatusCode(), 404);
            }

            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                FunctionState functionState =
                        admin.functions().getFunctionState("public", "default", sourceName, "now");
                assertTrue(functionState.getStringValue().matches("val1-.*"));
            });

            // delete source
            deleteSource(admin, sourceName);

            getSourceInfoNotFound(admin, sourceName);
        }
    }

    @Test(groups = {"java_state", "state", "function", "java_function"})
    public void testSinkState() throws Exception {
        String inputTopicName = "test-state-sink-input-" + randomName(8);
        String sinkName = "test-state-sink-" + randomName(8);
        int numMessages = 10;

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {
            submitSinkConnector(admin, sinkName, inputTopicName, "org.apache.pulsar.tests.integration.io.TestStateSink",
                    JAVAJAR);

            // get sink info
            getSinkInfoSuccess(admin, sinkName);

            // get sink status
            getSinkStatus(admin, sinkName);

            // The sink puts "initial" when it opens, before it subscribes to its input at the latest message:
            // wait for its consumer before producing.
            waitForConsumer(admin, inputTopicName, String.format("public/default/%s", sinkName));

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

            // query a non-exist key should get a 404 error
            {
                PulsarAdminException e = expectThrows(PulsarAdminException.class, () -> {
                    admin.functions().getFunctionState("public", "default", sinkName, "non-exist");
                });
                assertEquals(e.getStatusCode(), 404);
            }

            // query a non-exist instance should get a 404 error
            {
                PulsarAdminException e = expectThrows(PulsarAdminException.class, () -> {
                    admin.functions().getFunctionState("public", "default", "non-exist", "non-exist");
                });
                assertEquals(e.getStatusCode(), 404);
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

            // delete sink
            deleteSink(admin, sinkName);

            getSinkInfoNotFound(admin, sinkName);
        }
    }

    @Test(groups = {"python_state", "state", "function", "python_function"})
    public void testNonExistFunction() throws Exception {
        String functionName = "non-exist-function-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {
            // query a non-exist instance should get a 404 error
            {
                PulsarAdminException e = expectThrows(PulsarAdminException.class, () -> {
                    admin.functions().getFunctionState("public", "default", functionName, "non-exist");
                });
                assertEquals(e.getStatusCode(), 404);
            }
        }
    }

    @Test(groups = {"java_state", "state", "function", "java_function"})
    public void testBytes2StringNotUTF8() {
        byte[] valueBytes = Base64.getDecoder().decode(VALUE_BASE64);
        assertFalse(Utf8.isWellFormed(valueBytes));
        assertNotEquals(valueBytes, new String(valueBytes, UTF_8).getBytes(UTF_8));
    }

    @Test(groups = {"java_state", "state", "function", "java_function"})
    public void testSourceByteState() throws Exception {
        String outputTopicName = "test-state-source-output-" + randomName(8);
        String sourceName = "test-state-source-" + randomName(8);

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {
            submitSourceConnector(admin, sourceName, outputTopicName,
                    "org.apache.pulsar.tests.integration.io.TestByteStateSource", JAVAJAR);

            // get source info
            getSourceInfoSuccess(admin, sourceName);

            // get source status
            getSourceStatus(admin, sourceName);

            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                SourceStatus status = admin.sources().getSourceStatus("public", "default", sourceName);
                assertEquals(status.getInstances().size(), 1);
                assertTrue(status.getInstances().get(0).getStatus().numWritten > 0);
            });

            {
                FunctionState functionState =
                        admin.functions().getFunctionState("public", "default", sourceName, "initial");
                assertNull(functionState.getStringValue());
                assertEquals(functionState.getByteValue(), Base64.getDecoder().decode(VALUE_BASE64));
            }

            Awaitility.await().ignoreExceptions().untilAsserted(() -> {
                FunctionState functionState =
                        admin.functions().getFunctionState("public", "default", sourceName, "now");
                assertNull(functionState.getStringValue());
                assertEquals(functionState.getByteValue(), Base64.getDecoder().decode(VALUE_BASE64));
            });

            // delete source
            deleteSource(admin, sourceName);

            getSourceInfoNotFound(admin, sourceName);
        }
    }

    // The sources, sinks and functions are created through the REST API with the configuration that pulsar-admin
    // sends for the same arguments, and the package it would upload from the container's file system.
    private void submitSourceConnector(PulsarAdmin admin,
                                       String sourceName,
                                       String outputTopicName,
                                       String className,
                                       String archive) throws Exception {
        SourceConfig sourceConfig = SourceConfig.builder()
                .tenant("public")
                .namespace("default")
                .name(sourceName)
                .topicName(outputTopicName)
                .archive(archive)
                .className(className)
                .parallelism(1)
                .build();
        String archiveFile = copyFromContainer(archive);
        createWhenLeaderReady(() -> admin.sources().createSource(sourceConfig, archiveFile));
    }

    private void submitSinkConnector(PulsarAdmin admin,
                                     String sinkName,
                                     String inputTopicName,
                                     String className,
                                     String archive) throws Exception {
        SinkConfig sinkConfig = SinkConfig.builder()
                .tenant("public")
                .namespace("default")
                .name(sinkName)
                .inputs(List.of(inputTopicName))
                .archive(archive)
                .className(className)
                .parallelism(1)
                .build();
        String archiveFile = copyFromContainer(archive);
        createWhenLeaderReady(() -> admin.sinks().createSink(sinkConfig, archiveFile));
    }

    private void submitWordCountFunction(PulsarAdmin admin,
                                         Runtime runtime,
                                         String inputTopicName,
                                         String outputTopicName,
                                         String functionName) throws Exception {
        submitFunction(
                admin,
                runtime,
                inputTopicName,
                outputTopicName,
                functionName,
                getWordCountClass(runtime),
                Schema.BYTES);
    }

    protected static String getWordCountClass(Runtime runtime) {
        if (Runtime.PYTHON == runtime) {
            return WORDCOUNT_PYTHON_CLASS;
        } else if (Runtime.JAVA == runtime) {
            return WORDCOUNT_JAVA_CLASS;
        } else {
            throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }
    }

    private <T> void submitFunction(PulsarAdmin admin,
                                    Runtime runtime,
                                    String inputTopicName,
                                    String outputTopicName,
                                    String functionName,
                                    String functionClass,
                                    Schema<T> inputTopicSchema) throws Exception {
        FunctionConfig.FunctionConfigBuilder functionConfig = FunctionConfig.builder()
                .tenant("public")
                .namespace("default")
                .name(functionName)
                .className(functionClass)
                .inputs(List.of(inputTopicName))
                .output(outputTopicName);
        String packagePath;
        if (Runtime.JAVA == runtime) {
            packagePath = JAVAJAR;
            functionConfig.jar(packagePath).resources(Resources.builder().ram(JAVA_FUNCTION_RAM_BYTES).build());
        } else if (Runtime.PYTHON == runtime) {
            packagePath = PYTHONBASE + WORDCOUNT_PYTHON_FILE;
            functionConfig.py(packagePath);
        } else {
            throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }
        FunctionConfig config = functionConfig.build();
        String packageFile = copyFromContainer(packagePath);
        createWhenLeaderReady(() -> admin.functions().createFunction(config, packageFile));

        ensureSubscriptionCreated(inputTopicName, String.format("public/default/%s", functionName), inputTopicSchema);
    }

    // Until the functions worker is the leader, a moment after the cluster starts, it rejects creates with 503
    private static void createWhenLeaderReady(ThrowingRunnable create) {
        Awaitility.await().atMost(Duration.ofSeconds(30))
                .ignoreExceptionsMatching(e -> e instanceof PulsarAdminException
                        && ((PulsarAdminException) e).getStatusCode() == 503)
                .untilAsserted(create);
    }

    private String copyFromContainer(String path) throws IOException {
        Path file = Files.createTempFile("pulsar-state-test-", "-" + Path.of(path).getFileName());
        file.toFile().deleteOnExit();
        container.copyFileFromContainer(path, in -> Files.copy(in, file, StandardCopyOption.REPLACE_EXISTING));
        return file.toString();
    }

    @SuppressWarnings("try")
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

    private void getSinkInfoSuccess(PulsarAdmin admin, String sinkName) throws Exception {
        assertEquals(admin.sinks().getSink("public", "default", sinkName).getName(), sinkName);
    }

    private void getSourceInfoSuccess(PulsarAdmin admin, String sourceName) throws Exception {
        assertEquals(admin.sources().getSource("public", "default", sourceName).getName(), sourceName);
    }

    private void getFunctionInfoSuccess(PulsarAdmin admin, String functionName) throws Exception {
        assertEquals(admin.functions().getFunction("public", "default", functionName).getName(), functionName);
    }

    private void getFunctionInfoNotFound(PulsarAdmin admin, String functionName) {
        PulsarAdminException e = expectThrows(PulsarAdminException.class,
                () -> admin.functions().getFunction("public", "default", functionName));
        assertEquals(e.getStatusCode(), 404);
        assertTrue(e.getMessage().contains("Function " + functionName + " doesn't exist"), e.getMessage());
    }

    // The instance starts after the create command returns, so the status checks wait for it.
    private void getSinkStatus(PulsarAdmin admin, String sinkName) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() -> {
            SinkStatus status = admin.sinks().getSinkStatus("public", "default", sinkName);
            assertEquals(status.getInstances().size(), 1);
            assertTrue(status.getInstances().get(0).getStatus().isRunning());
        });
    }

    private void getSourceStatus(PulsarAdmin admin, String sourceName) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() -> {
            SourceStatus status = admin.sources().getSourceStatus("public", "default", sourceName);
            assertEquals(status.getInstances().size(), 1);
            assertTrue(status.getInstances().get(0).getStatus().isRunning());
        });
    }

    private void getFunctionStatus(PulsarAdmin admin, String functionName, int numMessages) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() -> {
            FunctionStatus status = admin.functions().getFunctionStatus("public", "default", functionName);
            assertEquals(status.getInstances().size(), 1);
            assertTrue(status.getInstances().get(0).getStatus().isRunning());
            assertEquals(status.getInstances().get(0).getStatus().getNumSuccessfullyProcessed(), numMessages);
        });
    }

    private void waitForConsumer(PulsarAdmin admin, String topic, String subscription) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() -> assertEquals(
                admin.topics().getStats(topic).getSubscriptions().get(subscription).getConsumers().size(), 1));
    }

    private void queryState(PulsarAdmin admin, String functionName, String key, int amount, long version)
            throws Exception {
        FunctionState state = admin.functions().getFunctionState("public", "default", functionName, key);
        assertEquals(state.getNumberValue(), Long.valueOf(amount));
        assertEquals(state.getVersion(), Long.valueOf(version));
        assertNull(state.getStringValue());
        assertNull(state.getByteValue());
    }

    private FunctionState putAndQueryState(PulsarAdmin admin, String functionName, FunctionState state)
            throws Exception {
        admin.functions().putFunctionState("public", "default", functionName, state);
        return admin.functions().getFunctionState("public", "default", functionName, state.getKey());
    }

    private void putAndQueryStateByte(String functionName, String key, String state, byte[] expect)
            throws Exception {
        container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "putstate",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName,
                "--state", state
        );

        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "querystate",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName,
                "--key", key
        );

        FunctionState byteState = new Gson().fromJson(result.getStdout(), FunctionState.class);
        assertNull(byteState.getStringValue());
        assertEquals(byteState.getByteValue(), expect);
    }

    private void publishAndConsumeMessages(String inputTopic,
                                           String outputTopic,
                                           int numMessages) throws Exception {
        @Cleanup PulsarClient client = PulsarClient.builder()
                .serviceUrl(container.getPlainTextServiceUrl())
                .build();
        @Cleanup Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(inputTopic)
                .create();

        for (int i = 0; i < numMessages; i++) {
            producer.send(("hello test message-" + i).getBytes(UTF_8));
        }

        if (outputTopic == null) {
            return;
        }
        @Cleanup Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                .topic(outputTopic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("test-sub")
                .subscribe();
        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive();
            assertEquals("hello test message-" + i + "!", new String(msg.getValue(), UTF_8));
        }
    }

    private void deleteFunction(PulsarAdmin admin, String functionName) throws Exception {
        admin.functions().deleteFunction("public", "default", functionName);
    }

    private void deleteSource(PulsarAdmin admin, String sourceName) throws Exception {
        admin.sources().deleteSource("public", "default", sourceName);
    }

    private void deleteSink(PulsarAdmin admin, String sinkName) throws Exception {
        admin.sinks().deleteSink("public", "default", sinkName);
    }

    private void getSourceInfoNotFound(PulsarAdmin admin, String sourceName) {
        PulsarAdminException e = expectThrows(PulsarAdminException.class,
                () -> admin.sources().getSource("public", "default", sourceName));
        assertEquals(e.getStatusCode(), 404);
        assertTrue(e.getMessage().contains("Source " + sourceName + " doesn't exist"), e.getMessage());
    }

    private void getSinkInfoNotFound(PulsarAdmin admin, String sinkName) {
        PulsarAdminException e = expectThrows(PulsarAdminException.class,
                () -> admin.sinks().getSink("public", "default", sinkName));
        assertEquals(e.getStatusCode(), 404);
        assertTrue(e.getMessage().contains("Sink " + sinkName + " doesn't exist"), e.getMessage());
    }

}

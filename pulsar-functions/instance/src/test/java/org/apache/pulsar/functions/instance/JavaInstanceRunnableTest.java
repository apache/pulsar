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
package org.apache.pulsar.functions.instance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.instance.stats.FunctionStatsManager;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.secretsprovider.EnvironmentBasedSecretsProvider;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class JavaInstanceRunnableTest {
    private final List<AutoCloseable> closeables = new ArrayList<>();

    static class IntegerSerDe implements SerDe<Integer> {
        @Override
        public Integer deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(Integer input) {
            return new byte[0];
        }
    }

    private static InstanceConfig createInstanceConfig(FunctionDetails functionDetails) {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetails);
        instanceConfig.setMaxBufferedTuples(1024);
        return instanceConfig;
    }

    private JavaInstanceRunnable createRunnable(String outputSerde) throws Exception {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        if (outputSerde != null) {
            functionDetailsBuilder.setSink(SinkSpec.newBuilder().setSerDeClassName(outputSerde).build());
        }
        return createRunnable(functionDetailsBuilder.build());
    }

    private JavaInstanceRunnable createRunnable(FunctionDetails functionDetails) throws Exception {
        ClientBuilder clientBuilder = mock(ClientBuilder.class);
        when(clientBuilder.build()).thenReturn(null);
        InstanceConfig config = createInstanceConfig(functionDetails);
        JavaInstanceRunnable javaInstanceRunnable = new JavaInstanceRunnable(
                config, clientBuilder, null, null, null, null, null, null, null, null);
        return javaInstanceRunnable;
    }

    private JavaInstanceRunnable createRunnable(SourceSpec sourceSpec,
                                                String functionClassName, SinkSpec sinkSpec)
            throws PulsarClientException {
        ClientBuilder clientBuilder = mock(ClientBuilder.class);
        when(clientBuilder.build()).thenReturn(null);
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setSource(sourceSpec)
                .setClassName(functionClassName)
                .setSink(sinkSpec)
                .build();
        InstanceConfig config = createInstanceConfig(functionDetails);
        config.setClusterName("test-cluster");
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://test-cluster:6650").build();
        registerCloseable(pulsarClient);
        return new JavaInstanceRunnable(config, clientBuilder,
                pulsarClient, null, null, null, null, null,
                Thread.currentThread().getContextClassLoader(), null);
    }

    private Method makeAccessible(JavaInstanceRunnable javaInstanceRunnable) throws Exception {
        Method method =
                javaInstanceRunnable.getClass().getDeclaredMethod("setupSerDe", Class[].class, ClassLoader.class);
        method.setAccessible(true);
        return method;
    }

    @Getter
    @Setter
    private class ComplexUserDefinedType {
        private String name;
        private Integer age;
    }

    private class ComplexTypeHandler implements Function<String, ComplexUserDefinedType> {
        @Override
        public ComplexUserDefinedType process(String input, Context context) throws Exception {
            return new ComplexUserDefinedType();
        }
    }

    private class ComplexSerDe implements SerDe<ComplexUserDefinedType> {
        @Override
        public ComplexUserDefinedType deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(ComplexUserDefinedType input) {
            return new byte[0];
        }
    }

    private class VoidInputHandler implements Function<Void, String> {
        @Override
        public String process(Void input, Context context) throws Exception {
            return new String("Interesting");
        }
    }

    private class VoidOutputHandler implements Function<String, Void> {
        @Override
        public Void process(String input, Context context) throws Exception {
            return null;
        }
    }

    @Test
    public void testFunctionAsyncTime() throws Exception {
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setAutoAck(true)
                .setProcessingGuarantees(org.apache.pulsar.functions.proto.Function.ProcessingGuarantees.MANUAL)
                .build();
        JavaInstanceRunnable javaInstanceRunnable = createRunnable(functionDetails);
        FunctionStatsManager manager = mock(FunctionStatsManager.class);
        javaInstanceRunnable.setStats(manager);
        JavaExecutionResult javaExecutionResult = new JavaExecutionResult();
        Thread.sleep(500);
        Record record = mock(Record.class);
        javaInstanceRunnable.handleResult(record, javaExecutionResult);
        ArgumentCaptor<Long> timeCaptor = ArgumentCaptor.forClass(Long.class);
        verify(manager).processTimeEnd(timeCaptor.capture());
        Assert.assertEquals(timeCaptor.getValue(), javaExecutionResult.getStartTime());
    }

    @Test
    public void testFunctionResultNull() throws Exception {
        JavaExecutionResult javaExecutionResult = new JavaExecutionResult();

        // ProcessingGuarantees == MANUAL, not need ack.
        Record record = mock(Record.class);
        getJavaInstanceRunnable(true, org.apache.pulsar.functions.proto.Function.ProcessingGuarantees.MANUAL)
                .handleResult(record, javaExecutionResult);
        verify(record, times(0)).ack();

        // ProcessingGuarantees == ATMOST_ONCE and autoAck == true, not need ack
        clearInvocations(record);
        getJavaInstanceRunnable(true, org.apache.pulsar.functions.proto.Function.ProcessingGuarantees.ATMOST_ONCE)
                .handleResult(record, javaExecutionResult);
        verify(record, times(0)).ack();

        // other case, need ack
        clearInvocations(record);
        getJavaInstanceRunnable(true, org.apache.pulsar.functions.proto.Function.ProcessingGuarantees.ATLEAST_ONCE)
                .handleResult(record, javaExecutionResult);
        verify(record, times(1)).ack();
        clearInvocations(record);
        getJavaInstanceRunnable(true, org.apache.pulsar.functions.proto.Function.ProcessingGuarantees.EFFECTIVELY_ONCE)
                .handleResult(record, javaExecutionResult);
        verify(record, times(1)).ack();
    }

    @NotNull
    private JavaInstanceRunnable getJavaInstanceRunnable(boolean autoAck,
                                                         org.apache.pulsar.functions.proto.Function.ProcessingGuarantees processingGuarantees) throws Exception {
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setAutoAck(autoAck)
                .setProcessingGuarantees(processingGuarantees).build();
        JavaInstanceRunnable javaInstanceRunnable = createRunnable(functionDetails);

        Field stats = JavaInstanceRunnable.class.getDeclaredField("stats");
        stats.setAccessible(true);
        stats.set(javaInstanceRunnable, mock(ComponentStatsManager.class));
        stats.setAccessible(false);
        return javaInstanceRunnable;
    }

    @Test
    public void testStatsManagerNull() throws Exception {
        JavaInstanceRunnable javaInstanceRunnable = createRunnable((String) null);

        Assert.assertEquals(javaInstanceRunnable.getFunctionStatus().build(),
                InstanceCommunication.FunctionStatus.newBuilder().build());

        Assert.assertEquals(javaInstanceRunnable.getMetrics(), InstanceCommunication.MetricsData.newBuilder().build());
    }

    @Test
    public void testSinkConfigParsingPreservesOriginalType() throws Exception {
        final Map<String, Object> parsedConfig = JavaInstanceRunnable.augmentAndFilterConnectorConfig(
                "{\"ttl\": 9223372036854775807}",
                new InstanceConfig(),
                new EnvironmentBasedSecretsProvider(),
                null,
                FunctionDetails.ComponentType.SINK
        );
        Assert.assertEquals(parsedConfig.get("ttl").getClass(), Long.class);
        Assert.assertEquals(parsedConfig.get("ttl"), Long.MAX_VALUE);
    }

    @Test
    public void testSourceConfigParsingPreservesOriginalType() throws Exception {
        final Map<String, Object> parsedConfig = JavaInstanceRunnable.augmentAndFilterConnectorConfig(
                "{\"ttl\": 9223372036854775807}",
                new InstanceConfig(),
                new EnvironmentBasedSecretsProvider(),
                null,
                FunctionDetails.ComponentType.SOURCE
        );
        Assert.assertEquals(parsedConfig.get("ttl").getClass(), Long.class);
        Assert.assertEquals(parsedConfig.get("ttl"), Long.MAX_VALUE);
    }

    @DataProvider(name = "component")
    public Object[][] component() {
        return new Object[][]{
                // Schema: component type, whether to map in secrets
                { FunctionDetails.ComponentType.SINK },
                { FunctionDetails.ComponentType.SOURCE },
                { FunctionDetails.ComponentType.FUNCTION },
                { FunctionDetails.ComponentType.UNKNOWN },
        };
    }

    @Test(dataProvider = "component")
    public void testEmptyStringInput(FunctionDetails.ComponentType componentType) throws Exception {
        final Map<String, Object> parsedConfig = JavaInstanceRunnable.augmentAndFilterConnectorConfig(
                "",
                new InstanceConfig(),
                new EnvironmentBasedSecretsProvider(),
                null,
                componentType
        );
        Assert.assertEquals(parsedConfig.size(), 0);
    }

    // Environment variables are set in the pom.xml file
    @Test(dataProvider = "component")
    public void testInterpolatingEnvironmentVariables(FunctionDetails.ComponentType componentType) throws Exception {
        final Map<String, Object> parsedConfig = JavaInstanceRunnable.augmentAndFilterConnectorConfig(
                """
                        {
                            "key": {
                                "key1": "${TEST_JAVA_INSTANCE_PARSE_ENV_VAR}",
                                "key2": "${unset-env-var}"
                            },
                            "key3": "${TEST_JAVA_INSTANCE_PARSE_ENV_VAR}"
                        }
                        """,
                new InstanceConfig(),
                new EnvironmentBasedSecretsProvider(),
                null,
                componentType
        );
        if ((componentType == FunctionDetails.ComponentType.SOURCE
                || componentType == FunctionDetails.ComponentType.SINK)) {
            Assert.assertEquals(((Map) parsedConfig.get("key")).get("key1"), "some-configuration");
            Assert.assertEquals(((Map) parsedConfig.get("key")).get("key2"), "${unset-env-var}");
            Assert.assertEquals(parsedConfig.get("key3"), "some-configuration");
        } else {
            Assert.assertEquals(((Map) parsedConfig.get("key")).get("key1"), "${TEST_JAVA_INSTANCE_PARSE_ENV_VAR}");
            Assert.assertEquals(((Map) parsedConfig.get("key")).get("key2"), "${unset-env-var}");
            Assert.assertEquals(parsedConfig.get("key3"), "${TEST_JAVA_INSTANCE_PARSE_ENV_VAR}");
        }
    }

    public static class ConnectorTestConfig1 {
        public String field1;
    }

    @DataProvider(name = "configIgnoreUnknownFields")
    public static Object[][] configIgnoreUnknownFields() {
        return new Object[][]{
                {false, FunctionDetails.ComponentType.SINK},
                {true, FunctionDetails.ComponentType.SINK},
                {false, FunctionDetails.ComponentType.SOURCE},
                {true, FunctionDetails.ComponentType.SOURCE}
        };
    }

    @Test(dataProvider = "configIgnoreUnknownFields")
    public void testSinkConfigIgnoreUnknownFields(boolean ignoreUnknownConfigFields,
                                                  FunctionDetails.ComponentType type) throws Exception {
        NarClassLoader narClassLoader = mock(NarClassLoader.class);
        final ConnectorDefinition connectorDefinition = new ConnectorDefinition();
        if (type == FunctionDetails.ComponentType.SINK) {
            connectorDefinition.setSinkConfigClass(ConnectorTestConfig1.class.getName());
        } else {
            connectorDefinition.setSourceConfigClass(ConnectorTestConfig1.class.getName());
        }
        when(narClassLoader.getServiceDefinition(any())).thenReturn(ObjectMapperFactory
                .getMapper().writer().writeValueAsString(connectorDefinition));
        final InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setIgnoreUnknownConfigFields(ignoreUnknownConfigFields);

        final Map<String, Object> parsedConfig = JavaInstanceRunnable.augmentAndFilterConnectorConfig(
                "{\"field1\": \"value\", \"field2\": \"value2\"}",
                instanceConfig,
                new EnvironmentBasedSecretsProvider(),
                narClassLoader,
                type
        );
        if (ignoreUnknownConfigFields) {
            Assert.assertEquals(parsedConfig.size(), 1);
            Assert.assertEquals(parsedConfig.get("field1"), "value");
        } else {
            Assert.assertEquals(parsedConfig.size(), 2);
            Assert.assertEquals(parsedConfig.get("field1"), "value");
            Assert.assertEquals(parsedConfig.get("field2"), "value2");
        }
    }

    public static class ConnectorTestConfig2 {
        public static int constantField = 1;
        public String field1;
        private long withGetter;
        @JsonIgnore
        private ConnectorTestConfig1 ignore;

        public long getWithGetter() {
            return withGetter;
        }
    }

    @Test
    public void testBeanPropertiesReader() throws Exception {
        final List<String> beanProperties = JavaInstanceRunnable.BeanPropertiesReader
                .getBeanProperties(ConnectorTestConfig2.class);
        Assert.assertEquals(new TreeSet<>(beanProperties), new TreeSet<>(Arrays.asList("field1", "withGetter")));
    }

    public static class TestSourceConnector implements Source<String> {

        private LinkedBlockingQueue<Record<String>> queue;
        private SourceContext context;

        public void pushRecord(Record<String> record) throws Exception {
            queue.put(record);
        }

        @Override
        public void open(Map config, SourceContext sourceContext) throws Exception {
            context = sourceContext;
            queue = new LinkedBlockingQueue<>();
        }

        @Override
        public Record<String> read() throws Exception {
            return queue.take();
        }

        @Override
        public void close() throws Exception {

        }

        public void fatalConnector() {
            context.fatal(new Exception(FailComponentType.FAIL_SOURCE.toString()));
        }
    }

    public static class TestFunction implements Function<String, CompletableFuture<String>> {
        @Override
        public CompletableFuture<String> process(String input, Context context) throws Exception {
            CompletableFuture<String> future = new CompletableFuture<>();
            new Thread(() -> {
                if (FailComponentType.FAIL_FUNC.toString().equals(input)) {
                    context.fatal(new Exception(FailComponentType.FAIL_FUNC.toString()));
                } else {
                    future.complete(input);
                }
            }).start();
            return future;
        }
    }

    public static class TestSinkConnector implements Sink<String> {
        SinkContext context;

        @Override
        public void open(Map config, SinkContext sinkContext) throws Exception {
            this.context = sinkContext;
        }

        @Override
        public void write(Record<String> record) throws Exception {
            new Thread(() -> {
                if (FailComponentType.FAIL_SINK.toString().equals(record.getValue())) {
                    context.fatal(new Exception(FailComponentType.FAIL_SINK.toString()));
                }
            }).start();
        }

        @Override
        public void close() throws Exception {

        }
    }

    private Object getPrivateField(JavaInstanceRunnable javaInstanceRunnable, String fieldName)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = JavaInstanceRunnable.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(javaInstanceRunnable);
    }

    public enum FailComponentType {
        FAIL_SOURCE,
        FAIL_FUNC,
        FAIL_SINK
    }

    @DataProvider(name = "failComponentType")
    public Object[][] failType() {
        return new Object[][]{{FailComponentType.FAIL_SOURCE}, {FailComponentType.FAIL_FUNC},
                {FailComponentType.FAIL_SINK}};
    }

    @Test(dataProvider = "failComponentType")
    public void testFatalTheInstance(FailComponentType failComponentType) throws Exception {
        JavaInstanceRunnable javaInstanceRunnable = createRunnable(
                SourceSpec.newBuilder()
                        .setClassName(TestSourceConnector.class.getName()).build(),
                TestFunction.class.getName(),
                SinkSpec.newBuilder().setClassName(TestSinkConnector.class.getName()).build()
        );

        Thread fnThread = new Thread(javaInstanceRunnable);
        fnThread.start();

        // Wait for the setup to complete
        AtomicReference<TestSourceConnector> source = new AtomicReference<>();
        Awaitility.await()
                .pollInterval(Duration.ofMillis(200))
                .atMost(Duration.ofSeconds(10))
                .ignoreExceptions().untilAsserted(() -> {
                    TestSourceConnector sourceConnector = (TestSourceConnector) getPrivateField(javaInstanceRunnable,
                            "source");
                    Assert.assertNotNull(sourceConnector);
                    source.set(sourceConnector);
                });

        // Fail the connector or function
        if (failComponentType == FailComponentType.FAIL_SOURCE) {
            source.get().fatalConnector();
        } else {
            source.get().pushRecord(failComponentType::toString);
        }

        // Assert that the instance is terminated with the fatal exception
        Awaitility.await()
                .pollInterval(Duration.ofMillis(200))
                .atMost(Duration.ofSeconds(10))
                .ignoreExceptions().untilAsserted(() -> {
                    Assert.assertNotNull(javaInstanceRunnable.getDeathException());
                    Assert.assertEquals(javaInstanceRunnable.getDeathException().getMessage(),
                            failComponentType.toString());

                    // Assert the java instance is closed
                    Assert.assertFalse(fnThread.isAlive());
                    Assert.assertFalse((boolean) getPrivateField(javaInstanceRunnable, "isInitialized"));
                });
    }

    @AfterClass
    public void cleanupInstanceCache() {
        InstanceCache.shutdown();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanupCloseables() {
        callCloseables(closeables);
    }

    protected <T extends AutoCloseable> T registerCloseable(T closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private static void callCloseables(List<AutoCloseable> closeables) {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception e) {
                log.error("Failure in calling close method", e);
            }
        }
    }
}

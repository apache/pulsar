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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.jetbrains.annotations.NotNull;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JavaInstanceRunnableTest {

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
        final Map<String, Object> parsedConfig = JavaInstanceRunnable.parseComponentConfig(
                "{\"ttl\": 9223372036854775807}",
                new InstanceConfig(),
                null,
                FunctionDetails.ComponentType.SINK
        );
        Assert.assertEquals(parsedConfig.get("ttl").getClass(), Long.class);
        Assert.assertEquals(parsedConfig.get("ttl"), Long.MAX_VALUE);
    }

    @Test
    public void testSourceConfigParsingPreservesOriginalType() throws Exception {
        final Map<String, Object> parsedConfig = JavaInstanceRunnable.parseComponentConfig(
                "{\"ttl\": 9223372036854775807}",
                new InstanceConfig(),
                null,
                FunctionDetails.ComponentType.SOURCE
        );
        Assert.assertEquals(parsedConfig.get("ttl").getClass(), Long.class);
        Assert.assertEquals(parsedConfig.get("ttl"), Long.MAX_VALUE);
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

        final Map<String, Object> parsedConfig = JavaInstanceRunnable.parseComponentConfig(
                "{\"field1\": \"value\", \"field2\": \"value2\"}",
                instanceConfig,
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
}

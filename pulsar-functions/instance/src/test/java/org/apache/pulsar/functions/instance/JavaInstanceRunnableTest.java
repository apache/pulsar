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

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SinkSpecOrBuilder;
import org.apache.pulsar.functions.proto.Function.SourceSpecOrBuilder;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.jetbrains.annotations.NotNull;
import org.testng.Assert;
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
        SinkSpecOrBuilder sinkSpec = mock(SinkSpecOrBuilder.class);
        when(sinkSpec.getConfigs()).thenReturn("{\"ttl\": 9223372036854775807}");
        Map<String, Object> parsedConfig =
                new ObjectMapper().readValue(sinkSpec.getConfigs(), new TypeReference<Map<String, Object>>() {
                });
        Assert.assertEquals(parsedConfig.get("ttl").getClass(), Long.class);
        Assert.assertEquals(parsedConfig.get("ttl"), Long.MAX_VALUE);
    }

    @Test
    public void testSourceConfigParsingPreservesOriginalType() throws Exception {
        SourceSpecOrBuilder sourceSpec = mock(SourceSpecOrBuilder.class);
        when(sourceSpec.getConfigs()).thenReturn("{\"ttl\": 9223372036854775807}");
        Map<String, Object> parsedConfig =
                new ObjectMapper().readValue(sourceSpec.getConfigs(), new TypeReference<Map<String, Object>>() {
                });
        Assert.assertEquals(parsedConfig.get("ttl").getClass(), Long.class);
        Assert.assertEquals(parsedConfig.get("ttl"), Long.MAX_VALUE);
    }
}

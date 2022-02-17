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
package org.apache.pulsar.functions.instance;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SinkSpecOrBuilder;
import org.apache.pulsar.functions.proto.Function.SourceSpecOrBuilder;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    private static InstanceConfig createInstanceConfig(String outputSerde) {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        if (outputSerde != null) {
            functionDetailsBuilder.setSink(SinkSpec.newBuilder().setSerDeClassName(outputSerde).build());
        }
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetailsBuilder.build());
        instanceConfig.setMaxBufferedTuples(1024);
        return instanceConfig;
    }

    private JavaInstanceRunnable createRunnable(String outputSerde) throws Exception {
        InstanceConfig config = createInstanceConfig(outputSerde);
        ClientBuilder clientBuilder = mock(ClientBuilder.class);
        when(clientBuilder.build()).thenReturn(null);
        JavaInstanceRunnable javaInstanceRunnable = new JavaInstanceRunnable(
                config, clientBuilder, null, null, null,null, null, null, null);
        return javaInstanceRunnable;
    }

    private Method makeAccessible(JavaInstanceRunnable javaInstanceRunnable) throws Exception {
        Method method = javaInstanceRunnable.getClass().getDeclaredMethod("setupSerDe", Class[].class, ClassLoader.class);
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
    public void testStatsManagerNull() throws Exception {
        JavaInstanceRunnable javaInstanceRunnable = createRunnable(null);

        Assert.assertEquals(javaInstanceRunnable.getFunctionStatus().build(), InstanceCommunication.FunctionStatus.newBuilder().build());

        Assert.assertEquals(javaInstanceRunnable.getMetrics(), InstanceCommunication.MetricsData.newBuilder().build());
    }

    @Test
    public void testSinkConfigParsingPreservesOriginalType() throws Exception {
        SinkSpecOrBuilder sinkSpec = mock(SinkSpecOrBuilder.class);
        when(sinkSpec.getConfigs()).thenReturn("{\"ttl\": 9223372036854775807}");
        Map<String, Object> parsedConfig =
                new ObjectMapper().readValue(sinkSpec.getConfigs(), new TypeReference<Map<String, Object>>() {});
        Assert.assertEquals(parsedConfig.get("ttl").getClass(), Long.class);
        Assert.assertEquals(parsedConfig.get("ttl"), Long.MAX_VALUE);
    }

    @Test
    public void testSourceConfigParsingPreservesOriginalType() throws Exception {
        SourceSpecOrBuilder sourceSpec = mock(SourceSpecOrBuilder.class);
        when(sourceSpec.getConfigs()).thenReturn("{\"ttl\": 9223372036854775807}");
        Map<String, Object> parsedConfig =
                new ObjectMapper().readValue(sourceSpec.getConfigs(), new TypeReference<Map<String, Object>>() {});
        Assert.assertEquals(parsedConfig.get("ttl").getClass(), Long.class);
        Assert.assertEquals(parsedConfig.get("ttl"), Long.MAX_VALUE);
    }
}

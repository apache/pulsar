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
package org.apache.pulsar.functions.source;


import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.fail;

@PrepareForTest({ConnectorUtils.class, FunctionCommon.class, InstanceUtils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.*" })
@Slf4j
public class PulsarSourceTest {

    private static Map<String, ConsumerConfig> consumerConfigs = new HashMap<>();
    static {
        consumerConfigs.put("persistent://sample/standalone/ns1/test_result", ConsumerConfig.builder()
                .serdeClassName(TopicSchema.DEFAULT_SERDE).isRegexPattern(false).build());
    }

    public static class TestSerDe implements SerDe<String> {

        @Override
        public String deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(String input) {
            return new byte[0];
        }
    }

    /**
     * Verify that JavaInstance does not support functions that take Void type as input.
     */
    private static PulsarClientImpl getPulsarClient() throws PulsarClientException {
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        ConsumerBuilder consumerBuilder = mock(ConsumerBuilder.class);
        doReturn(consumerBuilder).when(consumerBuilder).topics(anyList());
        doReturn(consumerBuilder).when(consumerBuilder).cryptoFailureAction(any());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionName(anyString());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionType(any());
        doReturn(consumerBuilder).when(consumerBuilder).ackTimeout(anyLong(), any());
        doReturn(consumerBuilder).when(consumerBuilder).messageListener(any());
        Consumer consumer = mock(Consumer.class);
        doReturn(consumer).when(consumerBuilder).subscribe();
        doReturn(consumerBuilder).when(pulsarClient).newConsumer(any());
        doReturn(CompletableFuture.completedFuture(consumer)).when(consumerBuilder).subscribeAsync();
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(pulsarClient).getSchema(anyString());
        return pulsarClient;
    }

    private static PulsarSourceConfig getPulsarConfigs() {
        PulsarSourceConfig pulsarConfig = new PulsarSourceConfig();
        pulsarConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        pulsarConfig.setTopicSchema(consumerConfigs);
        return pulsarConfig;
    }
    private static Function.FunctionDetails.Builder getFunctionDetails() {
        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder()
                .setProcessingGuarantees(Function.ProcessingGuarantees.ATLEAST_ONCE);
        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        sourceSpecBuilder.putInputSpecs("persistent://sample/standalone/ns1/test_result",
                Function.ConsumerSpec.newBuilder()
                        .setIsRegexPattern(false)
                        .build());
        functionDetailsBuilder.setSource(sourceSpecBuilder);
        return functionDetailsBuilder;
    }

    @Getter
    @Setter
    public static class ComplexUserDefinedType {
        private String name;
        private Integer age;
    }

    public static class ComplexSerDe implements SerDe<ComplexUserDefinedType> {
        @Override
        public ComplexUserDefinedType deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(ComplexUserDefinedType input) {
            return new byte[0];
        }
    }

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @Test
    public void testVoidInputClasses() throws IOException {
        mockStatic(FunctionCommon.class);
        Class<?>[] types = {
                Void.class,
                Integer.class
        };
        doReturn(types).when(FunctionCommon.class);
        FunctionCommon.getFunctionTypes(any(Function.FunctionDetails.class), any(ClassLoader.class));
        PulsarSourceConfig pulsarConfig = getPulsarConfigs();
        Function.FunctionDetails functionDetails = getFunctionDetails().build();
        PulsarSource pulsarSource = new PulsarSource(getPulsarClient(), pulsarConfig, new HashMap<>(), functionDetails, null);

        try {
            pulsarSource.open(new HashMap<>(), null);
            assertFalse(true);
        } catch (RuntimeException ex) {
            log.error("RuntimeException: {}", ex, ex);
            assertEquals(ex.getMessage(), "Input type of Pulsar Function cannot be Void");
        } catch (Exception ex) {
            log.error("Exception: {}", ex, ex);
            assertFalse(true);
        }
    }

    /**
     * Verify that function input type should be consistent with input serde type.
     */
    @Test
    public void testInconsistentInputType() throws IOException {
        PulsarSourceConfig pulsarConfig = getPulsarConfigs();
        Function.FunctionDetails.Builder functionDetailsBuilder = getFunctionDetails();
        // set type to be inconsistent to that of SerDe
        mockStatic(FunctionCommon.class);
        Class<?>[] types = {
                Integer.class,
                Integer.class
        };
        Thread.currentThread().setContextClassLoader(SerDe.class.getClassLoader());
        doReturn(types).when(FunctionCommon.class);
        FunctionCommon.getFunctionTypes(any(Function.FunctionDetails.class), any(ClassLoader.class));
        Map<String, ConsumerConfig> topicSerdeClassNameMap = new HashMap<>();
        topicSerdeClassNameMap.put("persistent://sample/standalone/ns1/test_result",
                ConsumerConfig.builder().serdeClassName(TestSerDe.class.getName()).build());
        pulsarConfig.setTopicSchema(topicSerdeClassNameMap);
        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        sourceSpecBuilder.putInputSpecs("persistent://sample/standalone/ns1/test_result",
                Function.ConsumerSpec.newBuilder()
                        .setSerdeClassName(TestSerDe.class.getName())
                        .build());
        functionDetailsBuilder.setSource(sourceSpecBuilder);
        PulsarSource pulsarSource = new PulsarSource(getPulsarClient(), pulsarConfig, new HashMap<>(), functionDetailsBuilder.build(), null);
        try {
            pulsarSource.open(new HashMap<>(), null);
            fail("Should fail constructing java instance if function type is inconsistent with serde type");
        } catch (RuntimeException ex) {
            log.error("RuntimeException: {}", ex, ex);
            assertTrue(ex.getMessage().startsWith("Inconsistent types found between function input type and serde type:"));
        } catch (Exception ex) {
            log.error("Exception: {}", ex, ex);
            assertTrue(false);
        }
    }

    /**
     * Verify that Default Serializer works fine.
     */
    @Test
    public void testDefaultSerDe() throws Exception {

        PulsarSourceConfig pulsarConfig = getPulsarConfigs();
        Function.FunctionDetails functionDetails = getFunctionDetails().build();
        // set type to void
        mockStatic(FunctionCommon.class);
        Class<?>[] types = {
                String.class,
                Integer.class
        };
        doReturn(types).when(FunctionCommon.class);
        FunctionCommon.getFunctionTypes(any(Function.FunctionDetails.class), any(ClassLoader.class));
        consumerConfigs.put("persistent://sample/standalone/ns1/test_result",
                ConsumerConfig.builder().serdeClassName(TopicSchema.DEFAULT_SERDE).build());
        pulsarConfig.setTopicSchema(consumerConfigs);
        PulsarSource pulsarSource = new PulsarSource(getPulsarClient(), pulsarConfig, new HashMap<>(), functionDetails, null);

        pulsarSource.open(new HashMap<>(), null);
    }

    @Test
    public void testComplexOuputType() throws Exception {
        PulsarSourceConfig pulsarConfig = getPulsarConfigs();
        Function.FunctionDetails functionDetails = getFunctionDetails().build();
        // set type to void
        mockStatic(FunctionCommon.class);
        Class<?>[] types = {
                ComplexUserDefinedType.class,
                Integer.class
        };
        Thread.currentThread().setContextClassLoader(SerDe.class.getClassLoader());
        doReturn(types).when(FunctionCommon.class);
        FunctionCommon.getFunctionTypes(any(Function.FunctionDetails.class), any(ClassLoader.class));
        consumerConfigs.put("persistent://sample/standalone/ns1/test_result",
                ConsumerConfig.builder().serdeClassName(ComplexSerDe.class.getName()).build());
        pulsarConfig.setTopicSchema(consumerConfigs);
        PulsarSource pulsarSource = new PulsarSource(getPulsarClient(), pulsarConfig, new HashMap<>(), functionDetails, null);

        pulsarSource.setupConsumerConfigs();
    }

}

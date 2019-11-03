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


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.io.core.SourceContext;
import org.testng.annotations.Test;

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
        ConsumerBuilder<?> consumerBuilder = mock(ConsumerBuilder.class);
        doReturn(consumerBuilder).when(consumerBuilder).topics(anyList());
        doReturn(consumerBuilder).when(consumerBuilder).cryptoFailureAction(any());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionName(any());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionInitialPosition(any());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionType(any());
        doReturn(consumerBuilder).when(consumerBuilder).ackTimeout(anyLong(), any());
        doReturn(consumerBuilder).when(consumerBuilder).messageListener(any());
        Consumer<?> consumer = mock(Consumer.class);
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
        pulsarConfig.setTypeClassName(String.class.getName());
        pulsarConfig.setSubscriptionPosition(SubscriptionInitialPosition.Latest);
        pulsarConfig.setSubscriptionType(SubscriptionType.Shared);
        return pulsarConfig;
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


    @Test
    public void testVoidInputClasses() throws Exception {
        PulsarSourceConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(Void.class.getName());

        @Cleanup
        PulsarSource<?> pulsarSource = new PulsarSource<>(getPulsarClient(), pulsarConfig, new HashMap<>(), Thread.currentThread().getContextClassLoader());

        try {
            pulsarSource.open(new HashMap<>(), mock(SourceContext.class));
            fail();
        } catch (RuntimeException ex) {
            log.error("RuntimeException: {}", ex, ex);
            assertEquals(ex.getMessage(), "Input type of Pulsar Function cannot be Void");
        } catch (Exception ex) {
            log.error("Exception: {}", ex, ex);
            fail();
        }
    }

    /**
     * Verify that function input type should be consistent with input serde type.
     */
    @Test
    public void testInconsistentInputType() throws Exception {
        PulsarSourceConfig pulsarConfig = getPulsarConfigs();
        // set type to be inconsistent to that of SerDe
        pulsarConfig.setTypeClassName(Integer.class.getName());
        Map<String, ConsumerConfig> topicSerdeClassNameMap = new HashMap<>();
        topicSerdeClassNameMap.put("persistent://sample/standalone/ns1/test_result",
                ConsumerConfig.builder().serdeClassName(TestSerDe.class.getName()).build());
        pulsarConfig.setTopicSchema(topicSerdeClassNameMap);

        @Cleanup
        PulsarSource<?> pulsarSource = new PulsarSource<>(getPulsarClient(), pulsarConfig, new HashMap<>(), Thread.currentThread().getContextClassLoader());
        try {
            pulsarSource.open(new HashMap<>(), mock(SourceContext.class));
            fail("Should fail constructing java instance if function type is inconsistent with serde type");
        } catch (RuntimeException ex) {
            log.error("RuntimeException: {}", ex, ex);
            assertTrue(ex.getMessage().startsWith("Inconsistent types found between function input type and serde type:"));
        } catch (Exception ex) {
            log.error("Exception: {}", ex, ex);
            fail();
        }
    }

    /**
     * Verify that Default Serializer works fine.
     */
    @Test
    public void testDefaultSerDe() throws Exception {

        PulsarSourceConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(String.class.getName());
        consumerConfigs.put("persistent://sample/standalone/ns1/test_result",
                ConsumerConfig.builder().serdeClassName(TopicSchema.DEFAULT_SERDE).build());
        pulsarConfig.setTopicSchema(consumerConfigs);

        @Cleanup
        PulsarSource<?> pulsarSource = new PulsarSource<>(getPulsarClient(), pulsarConfig, new HashMap<>(), Thread.currentThread().getContextClassLoader());

        pulsarSource.open(new HashMap<>(), mock(SourceContext.class));
    }

    @Test
    public void testComplexOuputType() throws Exception {
        PulsarSourceConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(ComplexUserDefinedType.class.getName());
        consumerConfigs.put("persistent://sample/standalone/ns1/test_result",
                ConsumerConfig.builder().serdeClassName(ComplexSerDe.class.getName()).build());
        pulsarConfig.setTopicSchema(consumerConfigs);

        @Cleanup
        PulsarSource<?> pulsarSource = new PulsarSource<>(getPulsarClient(), pulsarConfig, new HashMap<>(), Thread.currentThread().getContextClassLoader());

        pulsarSource.setupConsumerConfigs();
    }
}

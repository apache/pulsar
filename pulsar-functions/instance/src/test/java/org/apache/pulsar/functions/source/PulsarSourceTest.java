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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.io.core.SourceContext;
import org.testng.Assert;
import org.mockito.ArgumentMatcher;

import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class PulsarSourceTest {


    private static final String TOPIC = "persistent://sample/ns1/test_result";
    private static final ConsumerConfig CONSUMER_CONFIG =  ConsumerConfig.builder()
            .serdeClassName(TopicSchema.DEFAULT_SERDE).isRegexPattern(false).build();

    private static Map<String, ConsumerConfig> consumerConfigs = new HashMap<>();
    static {
        consumerConfigs.put(TOPIC,CONSUMER_CONFIG);
    }

    private static Map<String, ConsumerConfig> multipleConsumerConfigs = new HashMap<>();
    static {
        multipleConsumerConfigs.put("persistent://sample/ns1/test_result1", ConsumerConfig.builder()
                .serdeClassName(TopicSchema.DEFAULT_SERDE).isRegexPattern(false).build());
        multipleConsumerConfigs.put("persistent://sample/ns1/test_result2", ConsumerConfig.builder()
                .serdeClassName(TopicSchema.DEFAULT_SERDE).isRegexPattern(false).build());
        multipleConsumerConfigs.put("persistent://sample/ns1/test_result3", ConsumerConfig.builder()
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

    @DataProvider (name = "sourceImpls")
    public static Object[] getPulsarSourceImpls() {
        MultiConsumerPulsarSourceConfig multiConsumerPulsarSourceConfig = getMultiConsumerPulsarConfigs(false);
        SingleConsumerPulsarSourceConfig singleConsumerPulsarSourceConfig = getSingleConsumerPulsarConfigs();
        return new Object[]{singleConsumerPulsarSourceConfig, multiConsumerPulsarSourceConfig};
    }

    /**
     * Verify that JavaInstance does not support functions that take Void type as input.
     */
    private static PulsarClientImpl getPulsarClient() throws PulsarClientException {
        PulsarClientImpl pulsarClient = Mockito.mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(pulsarClient.getCnxPool()).thenReturn(connectionPool);
        ConsumerBuilder<?> goodConsumerBuilder = Mockito.mock(ConsumerBuilder.class);
        ConsumerBuilder<?> badConsumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Mockito.doReturn(goodConsumerBuilder).when(goodConsumerBuilder).topics(Mockito.argThat(new TopicMatcher("persistent://sample/ns1/test_result")));
        Mockito.doReturn(goodConsumerBuilder).when(goodConsumerBuilder).topics(Mockito.argThat(new TopicMatcher("persistent://sample/ns1/test_result1")));
        Mockito.doReturn(badConsumerBuilder).when(goodConsumerBuilder).topics(Mockito.argThat(new TopicMatcher("persistent://sample/ns1/test_result2")));
        Mockito.doReturn(goodConsumerBuilder).when(goodConsumerBuilder).topics(Mockito.argThat(new TopicMatcher("persistent://sample/ns1/test_result3")));
        Mockito.doReturn(goodConsumerBuilder).when(goodConsumerBuilder).cryptoFailureAction(any());
        Mockito.doReturn(goodConsumerBuilder).when(goodConsumerBuilder).subscriptionName(any());
        Mockito.doReturn(goodConsumerBuilder).when(goodConsumerBuilder).subscriptionInitialPosition(any());
        Mockito.doReturn(goodConsumerBuilder).when(goodConsumerBuilder).subscriptionType(any());
        Mockito.doReturn(goodConsumerBuilder).when(goodConsumerBuilder).ackTimeout(anyLong(), any());
        Mockito.doReturn(goodConsumerBuilder).when(goodConsumerBuilder).messageListener(any());
        Mockito.doReturn(goodConsumerBuilder).when(goodConsumerBuilder).properties(any());
        Mockito.doReturn(badConsumerBuilder).when(badConsumerBuilder).cryptoFailureAction(any());
        Mockito.doReturn(badConsumerBuilder).when(badConsumerBuilder).subscriptionName(any());
        Mockito.doReturn(badConsumerBuilder).when(badConsumerBuilder).subscriptionInitialPosition(any());
        Mockito.doReturn(badConsumerBuilder).when(badConsumerBuilder).subscriptionType(any());
        Mockito.doReturn(badConsumerBuilder).when(badConsumerBuilder).ackTimeout(anyLong(), any());
        Mockito.doReturn(badConsumerBuilder).when(badConsumerBuilder).messageListener(any());
        Mockito.doReturn(badConsumerBuilder).when(badConsumerBuilder).properties(any());

        Consumer<?> consumer = Mockito.mock(Consumer.class);
        Mockito.doReturn(consumer).when(goodConsumerBuilder).subscribe();
        Mockito.doReturn(goodConsumerBuilder).when(pulsarClient).newConsumer(any());
        Mockito.doReturn(CompletableFuture.completedFuture(consumer)).when(goodConsumerBuilder).subscribeAsync();
        CompletableFuture<Consumer<?>> badFuture = new CompletableFuture<>();
        badFuture.completeExceptionally(new PulsarClientException("Some Error"));
        Mockito.doReturn(badFuture).when(badConsumerBuilder).subscribeAsync();
        Mockito.doThrow(PulsarClientException.class).when(badConsumerBuilder).subscribe();
        Mockito.doReturn(CompletableFuture.completedFuture(Optional.empty())).when(pulsarClient).getSchema(anyString());
        return pulsarClient;
    }

    private static class TopicMatcher implements ArgumentMatcher<List<String>> {
        private final String topic;

        public TopicMatcher(String topic) {
            this.topic = topic;
        }

        @Override
        public boolean matches(List<String> arg) {
            return arg.contains(topic);
        }
    }


    private static MultiConsumerPulsarSourceConfig getMultiConsumerPulsarConfigs(boolean multiple) {
        MultiConsumerPulsarSourceConfig pulsarConfig = new MultiConsumerPulsarSourceConfig();
        pulsarConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        if (multiple) {
            pulsarConfig.setTopicSchema(multipleConsumerConfigs);
        } else {
            pulsarConfig.setTopicSchema(consumerConfigs);
        }
        pulsarConfig.setTypeClassName(String.class.getName());
        pulsarConfig.setSubscriptionPosition(SubscriptionInitialPosition.Latest);
        pulsarConfig.setSubscriptionType(SubscriptionType.Shared);
        return pulsarConfig;
    }

    private static SingleConsumerPulsarSourceConfig getSingleConsumerPulsarConfigs() {
        SingleConsumerPulsarSourceConfig pulsarConfig = new SingleConsumerPulsarSourceConfig();
        pulsarConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        pulsarConfig.setTopic(TOPIC);
        pulsarConfig.setConsumerConfig(CONSUMER_CONFIG);
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

    private PulsarSource getPulsarSource(PulsarSourceConfig pulsarSourceConfig) throws PulsarClientException {
        PulsarSource<?> pulsarSource;
        if (pulsarSourceConfig instanceof SingleConsumerPulsarSourceConfig) {
            pulsarSource = new SingleConsumerPulsarSource<>(getPulsarClient(), (SingleConsumerPulsarSourceConfig) pulsarSourceConfig, new HashMap<>(), Thread.currentThread().getContextClassLoader());
        } else {
            pulsarSource = new MultiConsumerPulsarSource<>(getPulsarClient(), (MultiConsumerPulsarSourceConfig) pulsarSourceConfig, new HashMap<>(), Thread.currentThread().getContextClassLoader());
        }

        return pulsarSource;
    }

    private void setTopicAndConsumerConfig(
            PulsarSourceConfig pulsarSourceConfig, String topic, ConsumerConfig consumerConfig) {
        if (pulsarSourceConfig instanceof MultiConsumerPulsarSourceConfig) {
            ((MultiConsumerPulsarSourceConfig) pulsarSourceConfig).setTopicSchema(Collections.singletonMap(topic, consumerConfig));
        } else {
            ((SingleConsumerPulsarSourceConfig) pulsarSourceConfig).setTopic(topic);
            ((SingleConsumerPulsarSourceConfig) pulsarSourceConfig).setConsumerConfig(consumerConfig);
        }
    }

    @Test(dataProvider = "sourceImpls")
    public void testVoidInputClasses(PulsarSourceConfig pulsarSourceConfig) throws Exception {
        // set type to void
        pulsarSourceConfig.setTypeClassName(Void.class.getName());

        @Cleanup
        PulsarSource<?> pulsarSource = getPulsarSource(pulsarSourceConfig);
        try {
            pulsarSource.open(new HashMap<>(), Mockito.mock(SourceContext.class));
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
    @Test(dataProvider = "sourceImpls")
    public void testInconsistentInputType(PulsarSourceConfig pulsarSourceConfig) throws Exception {
        // set type to be inconsistent to that of SerDe
        pulsarSourceConfig.setTypeClassName(Integer.class.getName());
        String topic = "persistent://sample/ns1/test_result";
        ConsumerConfig consumerConfig =  ConsumerConfig.builder().serdeClassName(TestSerDe.class.getName()).build();
        setTopicAndConsumerConfig(pulsarSourceConfig, topic, consumerConfig);

        @Cleanup
        PulsarSource<?> pulsarSource = getPulsarSource(pulsarSourceConfig);
        try {
            pulsarSource.open(new HashMap<>(), Mockito.mock(SourceContext.class));
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
    @Test(dataProvider = "sourceImpls")
    public void testDefaultSerDe(PulsarSourceConfig pulsarSourceConfig) throws Exception {
        // set type to void
        pulsarSourceConfig.setTypeClassName(String.class.getName());
        consumerConfigs.put("persistent://sample/ns1/test_result",
                ConsumerConfig.builder().serdeClassName(TopicSchema.DEFAULT_SERDE).build());
        setTopicAndConsumerConfig(pulsarSourceConfig, TOPIC, CONSUMER_CONFIG);

        @Cleanup
        PulsarSource<?> pulsarSource = getPulsarSource(pulsarSourceConfig);

        pulsarSource.open(new HashMap<>(), Mockito.mock(SourceContext.class));
    }

    @Test(dataProvider = "sourceImpls")
    public void testComplexOutputType(PulsarSourceConfig pulsarSourceConfig) throws Exception {
        // set type to void
        pulsarSourceConfig.setTypeClassName(ComplexUserDefinedType.class.getName());
        consumerConfigs.put("persistent://sample/ns1/test_result",
                ConsumerConfig.builder().serdeClassName(ComplexSerDe.class.getName()).build());
        setTopicAndConsumerConfig(pulsarSourceConfig, TOPIC, CONSUMER_CONFIG);

        @Cleanup
        PulsarSource<?> pulsarSource = getPulsarSource(pulsarSourceConfig);
        pulsarSource.open(new HashMap<>(), Mockito.mock(SourceContext.class));
    }

    @Test
    public void testDanglingSubscriptions() throws Exception {
        MultiConsumerPulsarSourceConfig pulsarConfig = getMultiConsumerPulsarConfigs(true);

        MultiConsumerPulsarSource<?> pulsarSource = new MultiConsumerPulsarSource<>(getPulsarClient(), pulsarConfig, new HashMap<>(), Thread.currentThread().getContextClassLoader());
        try {
            pulsarSource.open(new HashMap<>(), Mockito.mock(SourceContext.class));
            fail();
        } catch (CompletionException e) {
            pulsarSource.close();
            assertEquals(pulsarSource.getInputConsumers().size(), 1);
        } catch (Exception e) {
            fail();
        }

    }

    @Test(dataProvider = "sourceImpls")
    public void testPreserveOriginalSchema(PulsarSourceConfig pulsarSourceConfig) throws Exception {
        pulsarSourceConfig.setTypeClassName(GenericRecord.class.getName());

        PulsarSource<GenericRecord> pulsarSource = getPulsarSource(pulsarSourceConfig);

        pulsarSource.open(new HashMap<>(), Mockito.mock(SourceContext.class));
        Consumer consumer = Mockito.mock(Consumer.class);
        MessageImpl messageImpl = Mockito.mock(MessageImpl.class);
        Schema schema = Mockito.mock(Schema.class);
        Mockito.when(messageImpl.getSchemaInternal()).thenReturn(schema);
        if (pulsarSource instanceof MultiConsumerPulsarSource) {
            ((MultiConsumerPulsarSource) pulsarSource).received(consumer, messageImpl);
        } else {
            Mockito.doReturn(messageImpl).when(((SingleConsumerPulsarSource) pulsarSource).getInputConsumer()).receive();
        }
        Mockito.verify(messageImpl.getSchemaInternal(), Mockito.times(1));
        Record<GenericRecord> pushed = pulsarSource.read();
        assertSame(pushed.getSchema(), schema);
    }

    @Test(dataProvider = "sourceImpls")
    public void testInputConsumersGetter(PulsarSourceConfig pulsarSourceConfig) throws Exception {
        PulsarSource<GenericRecord> pulsarSource = getPulsarSource(pulsarSourceConfig);
        pulsarSource.open(new HashMap<>(), null);

        if (pulsarSourceConfig instanceof SingleConsumerPulsarSourceConfig) {
            SingleConsumerPulsarSourceConfig cfg = (SingleConsumerPulsarSourceConfig) pulsarSourceConfig;
            Assert.assertEquals(pulsarSource.getInputConsumers().size(), 1);
            return;
        }

        if (pulsarSourceConfig instanceof MultiConsumerPulsarSourceConfig) {
            MultiConsumerPulsarSourceConfig cfg = (MultiConsumerPulsarSourceConfig) pulsarSourceConfig;
            Assert.assertEquals(pulsarSource.getInputConsumers().size(), cfg.getTopicSchema().size());
            return;
        }

        fail("Unknown config type");
    }
}

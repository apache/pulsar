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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests of {@link ConsumerBuilderImpl}.
 */
public class ConsumerBuilderImplTest {

    private static final String TOPIC_NAME = "testTopicName";
    private ConsumerBuilderImpl consumerBuilderImpl;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        ConsumerConfigurationData consumerConfigurationData = mock(ConsumerConfigurationData.class);
        when(consumerConfigurationData.getTopicsPattern()).thenReturn(Pattern.compile("\\w+"));
        when(consumerConfigurationData.getSubscriptionName()).thenReturn("testSubscriptionName");
        consumerBuilderImpl = new ConsumerBuilderImpl(client, consumerConfigurationData, Schema.BYTES);
    }

    @Test
    public void testConsumerBuilderImpl() throws PulsarClientException {
        Consumer consumer = mock(Consumer.class);
        when(consumerBuilderImpl.subscribeAsync())
                .thenReturn(CompletableFuture.completedFuture(consumer));
        assertNotNull(consumerBuilderImpl.topic(TOPIC_NAME).subscribe());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenTopicNamesVarargsIsNull() {
        consumerBuilderImpl.topic(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenTopicNamesVarargsHasNullTopic() {
        consumerBuilderImpl.topic("my-topic", null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenTopicNamesVarargsHasBlankTopic() {
        consumerBuilderImpl.topic("my-topic", "  ");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenTopicNamesIsNull() {
        consumerBuilderImpl.topics(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenTopicNamesIsEmpty() {
        consumerBuilderImpl.topics(Arrays.asList());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenTopicNamesHasBlankTopic() {
        List<String> topicNames = Arrays.asList("my-topic", " ");
        consumerBuilderImpl.topics(topicNames);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenTopicNamesHasNullTopic() {
        List<String> topicNames = Arrays.asList("my-topic", null);
        consumerBuilderImpl.topics(topicNames);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenSubscriptionNameIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenSubscriptionNameIsBlank() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName(" ");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConsumerBuilderImplWhenConsumerEventListenerIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME)
                .subscriptionName("subscriptionName")
                .consumerEventListener(null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConsumerBuilderImplWhenCryptoKeyReaderIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME)
                .subscriptionName("subscriptionName")
                .cryptoKeyReader(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenDefaultCryptoKeyReaderIsNullString() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName("subscriptionName")
                .defaultCryptoKeyReader((String) null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenDefaultCryptoKeyReaderIsEmptyString() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName("subscriptionName").defaultCryptoKeyReader("");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConsumerBuilderImplWhenDefaultCryptoKeyReaderIsNullMap() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName("subscriptionName")
                .defaultCryptoKeyReader((Map<String, String>) null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenDefaultCryptoKeyReaderIsEmptyMap() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName("subscriptionName")
                .defaultCryptoKeyReader(new HashMap<String, String>());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConsumerBuilderImplWhenCryptoFailureActionIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME)
                .subscriptionName("subscriptionName")
                .cryptoFailureAction(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenConsumerNameIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).consumerName(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenConsumerNameIsBlank() {
        consumerBuilderImpl.topic(TOPIC_NAME).consumerName(" ");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPropertyKeyIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).property(null, "Test-Value");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPropertyKeyIsBlank() {
        consumerBuilderImpl.topic(TOPIC_NAME).property("   ", "Test-Value");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPropertyValueIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).property("Test-Key", null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPropertyValueIsBlank() {
        consumerBuilderImpl.topic(TOPIC_NAME).property("Test-Key", "   ");
    }

    @Test
    public void testConsumerBuilderImplWhenPropertiesAreCorrect() {
        Map<String, String> properties = new HashMap<>();
        properties.put("Test-Key", "Test-Value");
        properties.put("Test-Key2", "Test-Value2");

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPropertiesKeyIsNull() {
        Map<String, String> properties = new HashMap<>();
        properties.put(null, "Test-Value");

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPropertiesKeyIsBlank() {
        Map<String, String> properties = new HashMap<>();
        properties.put("  ", "Test-Value");

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPropertiesValueIsNull() {
        Map<String, String> properties = new HashMap<>();
        properties.put("Test-Key", null);

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPropertiesValueIsBlank() {
        Map<String, String> properties = new HashMap<>();
        properties.put("Test-Key", "   ");

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    @Test
    public void testConsumerBuilderImplWhenPropertiesIsEmpty() {
        Map<String, String> properties = new HashMap<>();

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConsumerBuilderImplWhenPropertiesIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).properties(null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConsumerBuilderImplWhenSubscriptionInitialPositionIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionInitialPosition(null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConsumerBuilderImplWhenSubscriptionTopicsModeIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionTopicsMode(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenNegativeAckRedeliveryDelayPropertyIsNegative() {
        consumerBuilderImpl.negativeAckRedeliveryDelay(-1, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPriorityLevelPropertyIsNegative() {
        consumerBuilderImpl.priorityLevel(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenMaxTotalReceiverQueueSizeAcrossPartitionsPropertyIsNegative() {
        consumerBuilderImpl.maxTotalReceiverQueueSizeAcrossPartitions(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPatternAutoDiscoveryPeriodPeriodInMinutesIsNegative() {
        consumerBuilderImpl.patternAutoDiscoveryPeriod(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenPatternAutoDiscoveryPeriodPeriodIsNegative() {
        consumerBuilderImpl.patternAutoDiscoveryPeriod(-1, TimeUnit.MINUTES);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenBatchReceivePolicyIsNull() {
        consumerBuilderImpl.batchReceivePolicy(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConsumerBuilderImplWhenBatchReceivePolicyIsNotValid() {
        consumerBuilderImpl.batchReceivePolicy(BatchReceivePolicy.builder()
                .maxNumMessages(0)
                .maxNumBytes(0)
                .timeout(0, TimeUnit.MILLISECONDS)
                .build());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRedeliverCountOfDeadLetterPolicy() {
        consumerBuilderImpl.deadLetterPolicy(DeadLetterPolicy.builder()
                .maxRedeliverCount(0)
                .deadLetterTopic("test-dead-letter-topic")
                .retryLetterTopic("test-retry-letter-topic")
                .build());
    }

    @Test
    public void testNullDeadLetterPolicy() {
        consumerBuilderImpl.deadLetterPolicy(null);
        verify(consumerBuilderImpl.getConf()).setDeadLetterPolicy(null);
    }

    @Test
    public void testConsumerBuilderImplWhenNumericPropertiesAreValid() {
        consumerBuilderImpl.negativeAckRedeliveryDelay(1, TimeUnit.MILLISECONDS);
        consumerBuilderImpl.priorityLevel(1);
        consumerBuilderImpl.maxTotalReceiverQueueSizeAcrossPartitions(1);
        consumerBuilderImpl.patternAutoDiscoveryPeriod(1);
        consumerBuilderImpl.patternAutoDiscoveryPeriod(1, TimeUnit.SECONDS);
    }

    @Test
    public void testConsumerMode() {
        consumerBuilderImpl.subscriptionMode(SubscriptionMode.NonDurable)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
    }

    @Test
    public void testNegativeAckRedeliveryBackoff() {
        consumerBuilderImpl.negativeAckRedeliveryBackoff(MultiplierRedeliveryBackoff.builder()
                .minDelayMs(1000)
                .maxDelayMs(10 * 1000)
                .build());
    }

    @Test
    public void testStartPaused() {
        consumerBuilderImpl.startPaused(true);
        verify(consumerBuilderImpl.getConf()).setStartPaused(true);
    }
    @Test
    public void testLoadConf() throws Exception {
        ConsumerBuilderImpl<byte[]> consumerBuilder = createConsumerBuilder();

        String jsonConf = ("{\n"
            + "    'topicNames' : [ 'new-topic' ],\n"
            + "    'topicsPattern' : 'new-topics-pattern',\n"
            + "    'subscriptionName' : 'new-subscription',\n"
            + "    'subscriptionType' : 'Key_Shared',\n"
            + "    'subscriptionProperties' : {\n"
            + "      'new-sub-prop' : 'new-sub-prop-value'\n"
            + "    },\n"
            + "    'subscriptionMode' : 'NonDurable',\n"
            + "    'receiverQueueSize' : 2,\n"
            + "    'acknowledgementsGroupTimeMicros' : 2,\n"
            + "    'negativeAckRedeliveryDelayMicros' : 2,\n"
            + "    'maxTotalReceiverQueueSizeAcrossPartitions' : 2,\n"
            + "    'consumerName' : 'new-consumer',\n"
            + "    'ackTimeoutMillis' : 2,\n"
            + "    'tickDurationMillis' : 2,\n"
            + "    'priorityLevel' : 2,\n"
            + "    'maxPendingChunkedMessage' : 2,\n"
            + "    'autoAckOldestChunkedMessageOnQueueFull' : true,\n"
            + "    'expireTimeOfIncompleteChunkedMessageMillis' : 2,\n"
            + "    'cryptoFailureAction' : 'DISCARD',\n"
            + "    'properties' : {\n"
            + "      'new-prop' : 'new-prop-value'\n"
            + "    },\n"
            + "    'readCompacted' : true,\n"
            + "    'subscriptionInitialPosition' : 'Earliest',\n"
            + "    'patternAutoDiscoveryPeriod' : 2,\n"
            + "    'regexSubscriptionMode' : 'AllTopics',\n"
            + "    'deadLetterPolicy' : {\n"
            + "      'retryLetterTopic' : 'new-retry',\n"
            + "      'initialSubscriptionName' : 'new-dlq-sub',\n"
            + "      'deadLetterTopic' : 'new-dlq',\n"
            + "      'maxRedeliverCount' : 2\n"
            + "    },\n"
            + "    'retryEnable' : true,\n"
            + "    'autoUpdatePartitions' : false,\n"
            + "    'autoUpdatePartitionsIntervalSeconds' : 2,\n"
            + "    'replicateSubscriptionState' : true,\n"
            + "    'resetIncludeHead' : true,\n"
            + "    'batchIndexAckEnabled' : true,\n"
            + "    'ackReceiptEnabled' : true,\n"
            + "    'poolMessages' : true,\n"
            + "    'startPaused' : true\n"
            + "  }").replace("'", "\"");

        Map<String, Object> conf = new ObjectMapper().readValue(jsonConf, new TypeReference<HashMap<String,Object>>() {});

        MessageListener<byte[]> messageListener = (consumer, message) -> {};
        conf.put("messageListener", messageListener);
        ConsumerEventListener consumerEventListener = mock(ConsumerEventListener.class);
        conf.put("consumerEventListener", consumerEventListener);
        RedeliveryBackoff negativeAckRedeliveryBackoff = MultiplierRedeliveryBackoff.builder().build();
        conf.put("negativeAckRedeliveryBackoff", negativeAckRedeliveryBackoff);
        RedeliveryBackoff ackTimeoutRedeliveryBackoff = MultiplierRedeliveryBackoff.builder().build();;
        conf.put("ackTimeoutRedeliveryBackoff", ackTimeoutRedeliveryBackoff);
        CryptoKeyReader cryptoKeyReader = DefaultCryptoKeyReader.builder().build();
        conf.put("cryptoKeyReader", cryptoKeyReader);
        MessageCrypto messageCrypto = new MessageCryptoBc("ctx2", true);
        conf.put("messageCrypto", messageCrypto);
        BatchReceivePolicy batchReceivePolicy = BatchReceivePolicy.builder().maxNumBytes(2).build();
        conf.put("batchReceivePolicy", batchReceivePolicy);
        KeySharedPolicy keySharedPolicy = KeySharedPolicy.stickyHashRange();
        conf.put("keySharedPolicy", keySharedPolicy);
        MessagePayloadProcessor payloadProcessor = mock(MessagePayloadProcessor.class);
        conf.put("payloadProcessor", payloadProcessor);

        consumerBuilder.loadConf(conf);

        ConsumerConfigurationData<byte[]> configurationData = consumerBuilder.getConf();
        assertEquals(configurationData.getTopicNames(), new HashSet<>(Collections.singletonList("new-topic")));
        assertEquals(configurationData.getTopicsPattern().pattern(), "new-topics-pattern");
        assertEquals(configurationData.getSubscriptionName(), "new-subscription");
        assertEquals(configurationData.getSubscriptionType(), SubscriptionType.Key_Shared);
        assertThat(configurationData.getSubscriptionProperties()).hasSize(1)
            .hasFieldOrPropertyWithValue("new-sub-prop", "new-sub-prop-value");
        assertEquals(configurationData.getSubscriptionMode(), SubscriptionMode.NonDurable);
        assertEquals(configurationData.getReceiverQueueSize(), 2);
        assertEquals(configurationData.getAcknowledgementsGroupTimeMicros(), 2);

        assertEquals(configurationData.getNegativeAckRedeliveryDelayMicros(), 2);
        assertEquals(configurationData.getMaxTotalReceiverQueueSizeAcrossPartitions(), 2);
        assertEquals(configurationData.getConsumerName(), "new-consumer");
        assertEquals(configurationData.getAckTimeoutMillis(), 2);
        assertEquals(configurationData.getTickDurationMillis(), 2);
        assertEquals(configurationData.getPriorityLevel(), 2);
        assertEquals(configurationData.getMaxPendingChunkedMessage(), 2);
        assertTrue(configurationData.isAutoAckOldestChunkedMessageOnQueueFull());
        assertEquals(configurationData.getExpireTimeOfIncompleteChunkedMessageMillis(), 2);
        assertEquals(configurationData.getCryptoFailureAction(), ConsumerCryptoFailureAction.DISCARD);
        assertThat(configurationData.getProperties()).hasSize(1)
            .hasFieldOrPropertyWithValue("new-prop", "new-prop-value");
        assertTrue(configurationData.isReadCompacted());
        assertEquals(configurationData.getSubscriptionInitialPosition(), SubscriptionInitialPosition.Earliest);
        assertEquals(configurationData.getPatternAutoDiscoveryPeriod(), 2);
        assertEquals(configurationData.getRegexSubscriptionMode(), RegexSubscriptionMode.AllTopics);
        assertEquals(configurationData.getDeadLetterPolicy().getDeadLetterTopic(), "new-dlq");
        assertEquals(configurationData.getDeadLetterPolicy().getRetryLetterTopic(), "new-retry");
        assertEquals(configurationData.getDeadLetterPolicy().getInitialSubscriptionName(), "new-dlq-sub");
        assertEquals(configurationData.getDeadLetterPolicy().getMaxRedeliverCount(), 2);
        assertTrue(configurationData.isRetryEnable());
        assertFalse(configurationData.isAutoUpdatePartitions());
        assertEquals(configurationData.getAutoUpdatePartitionsIntervalSeconds(), 2);
        assertTrue(configurationData.isReplicateSubscriptionState());
        assertTrue(configurationData.isResetIncludeHead());
        assertTrue(configurationData.isBatchIndexAckEnabled());
        assertTrue(configurationData.isAckReceiptEnabled());
        assertTrue(configurationData.isPoolMessages());
        assertTrue(configurationData.isStartPaused());

        assertNull(configurationData.getMessageListener());
        assertNull(configurationData.getConsumerEventListener());
        assertNull(configurationData.getNegativeAckRedeliveryBackoff());
        assertNull(configurationData.getAckTimeoutRedeliveryBackoff());
        assertNull(configurationData.getMessageListener());
        assertNull(configurationData.getMessageCrypto());
        assertNull(configurationData.getCryptoKeyReader());
        assertNull(configurationData.getBatchReceivePolicy());
        assertNull(configurationData.getKeySharedPolicy());
        assertNull(configurationData.getPayloadProcessor());
    }

    @Test
    public void testLoadConfNotModified() {
        ConsumerBuilderImpl<byte[]> consumerBuilder = createConsumerBuilder();

        consumerBuilder.loadConf(new HashMap<>());

        ConsumerConfigurationData<byte[]> configurationData = consumerBuilder.getConf();
        assertEquals(configurationData.getTopicNames(), new HashSet<>(Collections.singletonList("topic")));
        assertEquals(configurationData.getTopicsPattern().pattern(), "topics-pattern");
        assertEquals(configurationData.getSubscriptionName(), "subscription");
        assertEquals(configurationData.getSubscriptionType(), SubscriptionType.Exclusive);
        assertThat(configurationData.getSubscriptionProperties()).hasSize(1)
            .hasFieldOrPropertyWithValue("sub-prop", "sub-prop-value");
        assertEquals(configurationData.getSubscriptionMode(), SubscriptionMode.Durable);
        assertEquals(configurationData.getReceiverQueueSize(), 1000);
        assertEquals(configurationData.getAcknowledgementsGroupTimeMicros(), TimeUnit.MILLISECONDS.toMicros(100));
        assertEquals(configurationData.getNegativeAckRedeliveryDelayMicros(), TimeUnit.MINUTES.toMicros(1));
        assertEquals(configurationData.getMaxTotalReceiverQueueSizeAcrossPartitions(), 50000);
        assertEquals(configurationData.getConsumerName(), "consumer");
        assertEquals(configurationData.getAckTimeoutMillis(), 30000);
        assertEquals(configurationData.getTickDurationMillis(), 1000);
        assertEquals(configurationData.getPriorityLevel(), 0);
        assertEquals(configurationData.getMaxPendingChunkedMessage(), 10);
        assertFalse(configurationData.isAutoAckOldestChunkedMessageOnQueueFull());
        assertEquals(configurationData.getExpireTimeOfIncompleteChunkedMessageMillis(), TimeUnit.MINUTES.toMillis(1));
        assertEquals(configurationData.getCryptoFailureAction(), ConsumerCryptoFailureAction.FAIL);
        assertThat(configurationData.getProperties()).hasSize(1)
            .hasFieldOrPropertyWithValue("prop", "prop-value");
        assertFalse(configurationData.isReadCompacted());
        assertEquals(configurationData.getSubscriptionInitialPosition(), SubscriptionInitialPosition.Latest);
        assertEquals(configurationData.getPatternAutoDiscoveryPeriod(), 60);
        assertEquals(configurationData.getRegexSubscriptionMode(), RegexSubscriptionMode.PersistentOnly);
        assertEquals(configurationData.getDeadLetterPolicy().getDeadLetterTopic(), "dlq");
        assertEquals(configurationData.getDeadLetterPolicy().getRetryLetterTopic(), "retry");
        assertEquals(configurationData.getDeadLetterPolicy().getInitialSubscriptionName(), "dlq-sub");
        assertEquals(configurationData.getDeadLetterPolicy().getMaxRedeliverCount(), 1);
        assertFalse(configurationData.isRetryEnable());
        assertTrue(configurationData.isAutoUpdatePartitions());
        assertEquals(configurationData.getAutoUpdatePartitionsIntervalSeconds(), 60);
        assertFalse(configurationData.isReplicateSubscriptionState());
        assertFalse(configurationData.isResetIncludeHead());
        assertFalse(configurationData.isBatchIndexAckEnabled());
        assertFalse(configurationData.isAckReceiptEnabled());
        assertFalse(configurationData.isPoolMessages());
        assertFalse(configurationData.isStartPaused());

        assertNull(configurationData.getMessageListener());
        assertNull(configurationData.getConsumerEventListener());
        assertNull(configurationData.getNegativeAckRedeliveryBackoff());
        assertNull(configurationData.getAckTimeoutRedeliveryBackoff());
        assertNull(configurationData.getMessageListener());
        assertNull(configurationData.getMessageCrypto());
        assertNull(configurationData.getCryptoKeyReader());
        assertNull(configurationData.getBatchReceivePolicy());
        assertNull(configurationData.getKeySharedPolicy());
        assertNull(configurationData.getPayloadProcessor());
    }

    private ConsumerBuilderImpl<byte[]> createConsumerBuilder() {
        ConsumerBuilderImpl<byte[]> consumerBuilder = new ConsumerBuilderImpl<>(null, Schema.BYTES);
        Map<String, String> properties = new HashMap<>();
        properties.put("prop", "prop-value");
        Map<String, String> subscriptionProperties = new HashMap<>();
        subscriptionProperties.put("sub-prop", "sub-prop-value");
        consumerBuilder
            .topic("topic")
            .topicsPattern("topics-pattern")
            .subscriptionName("subscription")
            .subscriptionProperties(subscriptionProperties)
            .messageListener((consumer, message) -> {})
            .consumerEventListener(mock(ConsumerEventListener.class))
            .negativeAckRedeliveryBackoff(MultiplierRedeliveryBackoff.builder().build())
            .ackTimeoutRedeliveryBackoff(MultiplierRedeliveryBackoff.builder().build())
            .consumerName("consumer")
            .cryptoKeyReader(DefaultCryptoKeyReader.builder().build())
            .messageCrypto(new MessageCryptoBc("ctx1", true))
            .properties(properties)
            .deadLetterPolicy(DeadLetterPolicy.builder().deadLetterTopic("dlq").retryLetterTopic("retry").initialSubscriptionName("dlq-sub").maxRedeliverCount(1).build())
            .batchReceivePolicy(BatchReceivePolicy.builder().maxNumBytes(1).build())
            .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
            .messagePayloadProcessor(mock(MessagePayloadProcessor.class));
        return consumerBuilder;
    }
}

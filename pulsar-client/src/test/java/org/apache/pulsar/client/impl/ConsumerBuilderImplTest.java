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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Unit tests of {@link ConsumerBuilderImpl}.
 */
public class ConsumerBuilderImplTest {

    private static final String TOPIC_NAME = "testTopicName";
    private ConsumerBuilderImpl consumerBuilderImpl;

    @BeforeTest
    public void setup() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConsumerConfigurationData consumerConfigurationData = mock(ConsumerConfigurationData.class);
        when(consumerConfigurationData.getTopicsPattern()).thenReturn(Pattern.compile("\\w+"));
        when(consumerConfigurationData.getSubscriptionName()).thenReturn("testSubscriptionName");
        consumerBuilderImpl = new ConsumerBuilderImpl(client, consumerConfigurationData, Schema.BYTES);
    }


    public void testConsumerBuilderImpl() throws PulsarClientException {
        Consumer consumer = mock(Consumer.class);
        when(consumerBuilderImpl.subscribeAsync())
                .thenReturn(CompletableFuture.completedFuture(consumer));
        assertNotNull(consumerBuilderImpl.topic(TOPIC_NAME).subscribe());
    }

    public void testConsumerBuilderImplWhenTopicNamesVarargsIsNull() {
        consumerBuilderImpl.topic(null);
    }

    
    public void testConsumerBuilderImplWhenTopicNamesVarargsHasNullTopic() {
        consumerBuilderImpl.topic("my-topic", null);
    }

    
    public void testConsumerBuilderImplWhenTopicNamesVarargsHasBlankTopic() {
        consumerBuilderImpl.topic("my-topic", "  ");
    }

    
    public void testConsumerBuilderImplWhenTopicNamesIsNull() {
        consumerBuilderImpl.topics(null);
    }

    
    public void testConsumerBuilderImplWhenTopicNamesIsEmpty() {
        consumerBuilderImpl.topics(Arrays.asList());
    }

    
    public void testConsumerBuilderImplWhenTopicNamesHasBlankTopic() {
        List<String> topicNames = Arrays.asList("my-topic", " ");
        consumerBuilderImpl.topics(topicNames);
    }

    
    public void testConsumerBuilderImplWhenTopicNamesHasNullTopic() {
        List<String> topicNames = Arrays.asList("my-topic", null);
        consumerBuilderImpl.topics(topicNames);
    }

    
    public void testConsumerBuilderImplWhenSubscriptionNameIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName(null);
    }

    
    public void testConsumerBuilderImplWhenSubscriptionNameIsBlank() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName(" ");
    }

    
    public void testConsumerBuilderImplWhenConsumerEventListenerIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME)
                .subscriptionName("subscriptionName")
                .consumerEventListener(null);
    }

    
    public void testConsumerBuilderImplWhenCryptoKeyReaderIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME)
                .subscriptionName("subscriptionName")
                .cryptoKeyReader(null);
    }

    
    public void testConsumerBuilderImplWhenDefaultCryptoKeyReaderIsNullString() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName("subscriptionName")
                .defaultCryptoKeyReader((String) null);
    }

    
    public void testConsumerBuilderImplWhenDefaultCryptoKeyReaderIsEmptyString() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName("subscriptionName").defaultCryptoKeyReader("");
    }

    
    public void testConsumerBuilderImplWhenDefaultCryptoKeyReaderIsNullMap() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName("subscriptionName")
                .defaultCryptoKeyReader((Map<String, String>) null);
    }

    
    public void testConsumerBuilderImplWhenDefaultCryptoKeyReaderIsEmptyMap() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionName("subscriptionName")
                .defaultCryptoKeyReader(new HashMap<String, String>());
    }

    
    public void testConsumerBuilderImplWhenCryptoFailureActionIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME)
                .subscriptionName("subscriptionName")
                .cryptoFailureAction(null);
    }

    
    public void testConsumerBuilderImplWhenConsumerNameIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).consumerName(null);
    }

    
    public void testConsumerBuilderImplWhenConsumerNameIsBlank() {
        consumerBuilderImpl.topic(TOPIC_NAME).consumerName(" ");
    }

    
    public void testConsumerBuilderImplWhenPropertyKeyIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).property(null, "Test-Value");
    }

    
    public void testConsumerBuilderImplWhenPropertyKeyIsBlank() {
        consumerBuilderImpl.topic(TOPIC_NAME).property("   ", "Test-Value");
    }

    
    public void testConsumerBuilderImplWhenPropertyValueIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).property("Test-Key", null);
    }

    
    public void testConsumerBuilderImplWhenPropertyValueIsBlank() {
        consumerBuilderImpl.topic(TOPIC_NAME).property("Test-Key", "   ");
    }


    public void testConsumerBuilderImplWhenPropertiesAreCorrect() {
        Map<String, String> properties = new HashMap<>();
        properties.put("Test-Key", "Test-Value");
        properties.put("Test-Key2", "Test-Value2");

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    
    public void testConsumerBuilderImplWhenPropertiesKeyIsNull() {
        Map<String, String> properties = new HashMap<>();
        properties.put(null, "Test-Value");

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    
    public void testConsumerBuilderImplWhenPropertiesKeyIsBlank() {
        Map<String, String> properties = new HashMap<>();
        properties.put("  ", "Test-Value");

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    
    public void testConsumerBuilderImplWhenPropertiesValueIsNull() {
        Map<String, String> properties = new HashMap<>();
        properties.put("Test-Key", null);

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    
    public void testConsumerBuilderImplWhenPropertiesValueIsBlank() {
        Map<String, String> properties = new HashMap<>();
        properties.put("Test-Key", "   ");

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }


    public void testConsumerBuilderImplWhenPropertiesIsEmpty() {
        Map<String, String> properties = new HashMap<>();

        consumerBuilderImpl.topic(TOPIC_NAME).properties(properties);
    }

    
    public void testConsumerBuilderImplWhenPropertiesIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).properties(null);
    }

    
    public void testConsumerBuilderImplWhenSubscriptionInitialPositionIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionInitialPosition(null);
    }

    
    public void testConsumerBuilderImplWhenSubscriptionTopicsModeIsNull() {
        consumerBuilderImpl.topic(TOPIC_NAME).subscriptionTopicsMode(null);
    }

    
    public void testConsumerBuilderImplWhenNegativeAckRedeliveryDelayPropertyIsNegative() {
        consumerBuilderImpl.negativeAckRedeliveryDelay(-1, TimeUnit.MILLISECONDS);
    }

    
    public void testConsumerBuilderImplWhenPriorityLevelPropertyIsNegative() {
        consumerBuilderImpl.priorityLevel(-1);
    }

    
    public void testConsumerBuilderImplWhenMaxTotalReceiverQueueSizeAcrossPartitionsPropertyIsNegative() {
        consumerBuilderImpl.maxTotalReceiverQueueSizeAcrossPartitions(-1);
    }

    
    public void testConsumerBuilderImplWhenPatternAutoDiscoveryPeriodPeriodInMinutesIsNegative() {
        consumerBuilderImpl.patternAutoDiscoveryPeriod(-1);
    }

    
    public void testConsumerBuilderImplWhenPatternAutoDiscoveryPeriodPeriodIsNegative() {
        consumerBuilderImpl.patternAutoDiscoveryPeriod(-1, TimeUnit.MINUTES);
    }

    
    public void testConsumerBuilderImplWhenBatchReceivePolicyIsNull() {
        consumerBuilderImpl.batchReceivePolicy(null);
    }

    
    public void testConsumerBuilderImplWhenBatchReceivePolicyIsNotValid() {
        consumerBuilderImpl.batchReceivePolicy(BatchReceivePolicy.builder()
                .maxNumMessages(0)
                .maxNumBytes(0)
                .timeout(0, TimeUnit.MILLISECONDS)
                .build());
    }

    
    public void testRedeliverCountOfDeadLetterPolicy() {
        consumerBuilderImpl.deadLetterPolicy(DeadLetterPolicy.builder()
                .maxRedeliverCount(0)
                .deadLetterTopic("test-dead-letter-topic")
                .retryLetterTopic("test-retry-letter-topic")
                .build());
    }


    public void testNullDeadLetterPolicy() {
        consumerBuilderImpl.deadLetterPolicy(null);
        verify(consumerBuilderImpl.getConf()).setDeadLetterPolicy(null);
    }


    public void testConsumerBuilderImplWhenNumericPropertiesAreValid() {
        consumerBuilderImpl.negativeAckRedeliveryDelay(1, TimeUnit.MILLISECONDS);
        consumerBuilderImpl.priorityLevel(1);
        consumerBuilderImpl.maxTotalReceiverQueueSizeAcrossPartitions(1);
        consumerBuilderImpl.patternAutoDiscoveryPeriod(1);
        consumerBuilderImpl.patternAutoDiscoveryPeriod(1, TimeUnit.SECONDS);
    }


    public void testConsumerMode() {
        consumerBuilderImpl.subscriptionMode(SubscriptionMode.NonDurable)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
    }


    public void testNegativeAckRedeliveryBackoff() {
        consumerBuilderImpl.negativeAckRedeliveryBackoff(MultiplierRedeliveryBackoff.builder()
                .minDelayMs(1000)
                .maxDelayMs(10 * 1000)
                .build());
    }


    public void testStartPaused() {
        consumerBuilderImpl.startPaused(true);
        verify(consumerBuilderImpl.getConf()).setStartPaused(true);
    }
}

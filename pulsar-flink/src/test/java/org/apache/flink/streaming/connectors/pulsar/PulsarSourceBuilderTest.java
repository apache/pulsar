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
package org.apache.flink.streaming.connectors.pulsar;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Pattern;

/**
 * Tests for PulsarSourceBuilder
 */
public class PulsarSourceBuilderTest {

    private PulsarSourceBuilder pulsarSourceBuilder;

    @BeforeMethod
    public void before() {
        pulsarSourceBuilder = PulsarSourceBuilder.builder(new TestDeserializationSchema());
    }

    @Test
    public void testBuild() throws PulsarClientException {
        SourceFunction sourceFunction = pulsarSourceBuilder
                .serviceUrl("testServiceUrl")
                .topic("testTopic")
                .subscriptionName("testSubscriptionName")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .build();
        Assert.assertNotNull(sourceFunction);
    }


    @Test
    public void testBuildWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(new HashSet<>(Arrays.asList("testTopic")));
        consumerConf.setSubscriptionName("testSubscriptionName");
        consumerConf.setSubscriptionInitialPosition(SubscriptionInitialPosition.Earliest);

        SourceFunction sourceFunction = pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
        Assert.assertNotNull(sourceFunction);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBuildWithoutSettingRequiredProperties() throws PulsarClientException {
        pulsarSourceBuilder.build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testServiceUrlWithNull() {
        pulsarSourceBuilder.serviceUrl(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testServiceUrlWithBlank() {
        pulsarSourceBuilder.serviceUrl(" ");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTopicWithNull() {
        pulsarSourceBuilder.topic(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTopicWithBlank() {
        pulsarSourceBuilder.topic(" ");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTopicsWithNull() {
        pulsarSourceBuilder.topics(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTopicsWithBlank() {
        pulsarSourceBuilder.topics(Arrays.asList(" ", " "));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTopicPatternWithNull() {
        pulsarSourceBuilder.topicsPattern(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTopicPatternAlreadySet() {
        pulsarSourceBuilder.topicsPattern(Pattern.compile("persistent://tenants/ns/topic-*"));
        pulsarSourceBuilder.topicsPattern(Pattern.compile("persistent://tenants/ns/topic-my-*"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTopicPattenStringWithNull() {
        pulsarSourceBuilder.topicsPatternString(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSubscriptionNameWithNull() {
        pulsarSourceBuilder.subscriptionName(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSubscriptionNameWithBlank() {
        pulsarSourceBuilder.subscriptionName(" ");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testSubscriptionInitialPosition() {
        pulsarSourceBuilder.subscriptionInitialPosition(null);
    }

    private class TestDeserializationSchema<T> implements DeserializationSchema<T> {

        @Override
        public T deserialize(byte[] bytes) throws IOException {
            return null;
        }

        @Override
        public boolean isEndOfStream(T t) {
            return false;
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return null;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testServiceUrlNullWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(null);

        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(new HashSet<String>(Arrays.asList("testServiceUrl")));
        consumerConf.setSubscriptionName("testSubscriptionName");

        pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testServiceUrlWithBlankWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(StringUtils.EMPTY);

        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(new HashSet<String>(Arrays.asList("testTopic")));
        consumerConf.setSubscriptionName("testSubscriptionName");

        pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTopicPatternWithNullWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");
        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicsPattern(null);
        consumerConf.setSubscriptionName("testSubscriptionName");

        pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSubscriptionNameWithNullWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(new HashSet<String>(Arrays.asList("testTopic")));
        consumerConf.setSubscriptionName(null);

        pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSubscriptionNameWithBlankWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(new HashSet<String>(Arrays.asList("testTopic")));
        consumerConf.setSubscriptionName(StringUtils.EMPTY);

        pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
    }

}

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
package org.apache.pulsar.functions.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.proto.FunctionDetails.ClientApi;
import org.apache.pulsar.functions.proto.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.SubscriptionType;
import org.testng.annotations.Test;

/**
 * Unit test of {@link ClientApiResolver}.
 */
public class ClientApiResolverTest {

    private static final String SCALABLE_IN = "topic://public/default/in";
    private static final String SCALABLE_OUT = "topic://public/default/out";
    private static final String PERSISTENT_IN = "persistent://public/default/in";
    private static final String PERSISTENT_OUT = "persistent://public/default/out";

    private static FunctionDetails function(ClientApi clientApi, String input, String output) {
        FunctionDetails details = new FunctionDetails()
                .setRuntime(FunctionDetails.Runtime.JAVA)
                .setClientApi(clientApi);
        if (input != null) {
            details.setSource().putInputSpecs(input).setIsRegexPattern(false);
        } else {
            details.setSource();
        }
        if (output != null) {
            details.setSink().setTopic(output);
        }
        return details;
    }

    @Test
    public void testAutoPicksFromTopicDomain() {
        assertThat(ClientApiResolver.resolve(function(ClientApi.AUTO, PERSISTENT_IN, PERSISTENT_OUT)))
                .isEqualTo(ClientApi.V4);
        assertThat(ClientApiResolver.resolve(function(ClientApi.AUTO, "in", null))).isEqualTo(ClientApi.V4);
        assertThat(ClientApiResolver.resolve(function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT)))
                .isEqualTo(ClientApi.V5);
        assertThat(ClientApiResolver.resolve(function(ClientApi.AUTO, null, SCALABLE_OUT)))
                .isEqualTo(ClientApi.V5);
        assertThat(ClientApiResolver.resolve(function(ClientApi.AUTO, null, null))).isEqualTo(ClientApi.V4);
    }

    @Test
    public void testExplicitClientApi() {
        assertThat(ClientApiResolver.resolve(function(ClientApi.V5, PERSISTENT_IN, PERSISTENT_OUT)))
                .isEqualTo(ClientApi.V5);
        assertThat(ClientApiResolver.resolve(function(ClientApi.V4, PERSISTENT_IN, PERSISTENT_OUT)))
                .isEqualTo(ClientApi.V4);
        assertThat(ClientApiResolver.resolve(function(ClientApi.V5, SCALABLE_IN, null))).isEqualTo(ClientApi.V5);
    }

    @Test
    public void testRejectsMixedDomains() {
        assertThatThrownBy(() -> ClientApiResolver.resolve(function(ClientApi.AUTO, SCALABLE_IN, PERSISTENT_OUT)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot mix topic://");
        FunctionDetails withLogTopic = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT)
                .setLogTopic("persistent://public/default/log");
        assertThatThrownBy(() -> ClientApiResolver.resolve(withLogTopic))
                .hasMessageContaining("persistent://public/default/log");
        FunctionDetails withDlq = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT);
        withDlq.setRetryDetails().setMaxMessageRetries(3).setDeadLetterTopic("dlq");
        assertThatThrownBy(() -> ClientApiResolver.resolve(withDlq)).hasMessageContaining("'dlq'");
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testLegacySerDeInputs() {
        FunctionDetails details = function(ClientApi.AUTO, null, null);
        details.getSource().putTopicsToSerDeClassName(SCALABLE_IN, "serde");
        assertThat(ClientApiResolver.resolve(details)).isEqualTo(ClientApi.V5);
        details.getSource().setSubscriptionType(SubscriptionType.FAILOVER);
        details.setClientApi(ClientApi.V5);
        details.getSource().clearTopicsToSerDeClassName();
        details.getSource().putTopicsToSerDeClassName(PERSISTENT_IN, "serde");
        assertThatThrownBy(() -> ClientApiResolver.resolve(details))
                .hasMessageContaining("needs topic:// (scalable) input topics");
    }

    @Test
    public void testRejectsSegmentTopic() {
        assertThatThrownBy(() -> ClientApiResolver.resolve(
                function(ClientApi.AUTO, "segment://public/default/in/0000-7fff-1", null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("segment of a scalable topic");
    }

    @Test
    public void testRejectsV4WithScalableTopic() {
        assertThatThrownBy(() -> ClientApiResolver.resolve(function(ClientApi.V4, SCALABLE_IN, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clientApi V4 cannot be used with topic '" + SCALABLE_IN + "'");
    }

    @Test
    public void testRejectsV5WithNonPersistentTopic() {
        assertThatThrownBy(() -> ClientApiResolver.resolve(
                function(ClientApi.V5, null, "non-persistent://public/default/out")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not support non-persistent topics");
    }

    @Test
    public void testRejectsV5WithTopicPattern() {
        FunctionDetails details = function(ClientApi.V5, null, SCALABLE_OUT);
        details.getSource().putInputSpecs("persistent://public/default/in-.*").setIsRegexPattern(true);
        assertThatThrownBy(() -> ClientApiResolver.resolve(details))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not support topic patterns");
        // a pattern alone does not select V5
        FunctionDetails auto = function(ClientApi.AUTO, null, PERSISTENT_OUT);
        auto.getSource().putInputSpecs("persistent://public/default/in-.*").setIsRegexPattern(true);
        assertThat(ClientApiResolver.resolve(auto)).isEqualTo(ClientApi.V4);
    }

    @Test
    public void testRejectsV5WithNonJavaRuntime() {
        FunctionDetails details = function(ClientApi.AUTO, SCALABLE_IN, null)
                .setRuntime(FunctionDetails.Runtime.PYTHON);
        assertThatThrownBy(() -> ClientApiResolver.resolve(details))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only supported by the Java runtime");
    }

    @Test
    public void testEffectivelyOnce() {
        FunctionDetails withOutput = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT)
                .setProcessingGuarantees(ProcessingGuarantees.EFFECTIVELY_ONCE);
        withOutput.getSource().setSubscriptionType(SubscriptionType.FAILOVER);
        assertThatThrownBy(() -> ClientApiResolver.resolve(withOutput))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("EFFECTIVELY_ONCE");
        // a sink connector has no output topic, so a scalable input is enough
        FunctionDetails sink = function(ClientApi.AUTO, SCALABLE_IN, null)
                .setProcessingGuarantees(ProcessingGuarantees.EFFECTIVELY_ONCE);
        sink.getSource().setSubscriptionType(SubscriptionType.FAILOVER);
        assertThat(ClientApiResolver.resolve(sink)).isEqualTo(ClientApi.V5);
    }

    @Test
    public void testOrderedSubscriptionsNeedScalableInputs() {
        for (SubscriptionType type : new SubscriptionType[]{SubscriptionType.FAILOVER, SubscriptionType.KEY_SHARED}) {
            FunctionDetails scalable = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT);
            scalable.getSource().setSubscriptionType(type);
            assertThat(ClientApiResolver.resolve(scalable)).isEqualTo(ClientApi.V5);

            FunctionDetails persistent = function(ClientApi.V5, PERSISTENT_IN, PERSISTENT_OUT);
            persistent.getSource().setSubscriptionType(type);
            assertThatThrownBy(() -> ClientApiResolver.resolve(persistent))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("needs topic:// (scalable) input topics");

            // v4 is unaffected
            FunctionDetails v4 = function(ClientApi.AUTO, PERSISTENT_IN, PERSISTENT_OUT);
            v4.getSource().setSubscriptionType(type);
            assertThat(ClientApiResolver.resolve(v4)).isEqualTo(ClientApi.V4);
        }
        // a V5 source has no inputs, so its subscription type does not matter
        FunctionDetails source = function(ClientApi.V5, null, PERSISTENT_OUT);
        source.getSource().setSubscriptionType(SubscriptionType.FAILOVER);
        assertThat(ClientApiResolver.resolve(source)).isEqualTo(ClientApi.V5);
    }

    @Test
    public void testRejectsSettingsTheV5ClientCannotHonor() {
        FunctionDetails consumerCrypto = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT);
        consumerCrypto.getSource().getInputSpecs(SCALABLE_IN).setCryptoSpec().setCryptoKeyReaderClassName("Reader");
        assertThatThrownBy(() -> ClientApiResolver.resolve(consumerCrypto))
                .hasMessageContaining("consumer encryption");

        FunctionDetails payloadProcessor = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT);
        payloadProcessor.getSource().getInputSpecs(SCALABLE_IN).setMessagePayloadProcessorSpec()
                .setClassName("Processor");
        assertThatThrownBy(() -> ClientApiResolver.resolve(payloadProcessor))
                .hasMessageContaining("message payload processors");

        FunctionDetails consumerProperties = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT);
        consumerProperties.getSource().getInputSpecs(SCALABLE_IN).putConsumerProperties("ackTimeoutMillis", "1");
        assertThatThrownBy(() -> ClientApiResolver.resolve(consumerProperties))
                .hasMessageContaining("consumer properties");

        FunctionDetails skipToLatest = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT);
        skipToLatest.getSource().setSkipToLatest(true);
        assertThatThrownBy(() -> ClientApiResolver.resolve(skipToLatest)).hasMessageContaining("skipToLatest");

        FunctionDetails producerCrypto = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT);
        producerCrypto.getSink().setProducerSpec().setCryptoSpec().setCryptoKeyReaderClassName("Reader");
        assertThatThrownBy(() -> ClientApiResolver.resolve(producerCrypto))
                .hasMessageContaining("producer encryption");

        // v4 components keep these settings
        FunctionDetails v4 = function(ClientApi.AUTO, PERSISTENT_IN, PERSISTENT_OUT);
        v4.getSource().setSkipToLatest(true);
        v4.getSource().getInputSpecs(PERSISTENT_IN).setCryptoSpec().setCryptoKeyReaderClassName("Reader");
        assertThat(ClientApiResolver.resolve(v4)).isEqualTo(ClientApi.V4);
    }

    @Test
    public void testOrderedSubscriptionsRejectRedeliverySettings() {
        FunctionDetails retries = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT);
        retries.getSource().setSubscriptionType(SubscriptionType.FAILOVER);
        retries.setRetryDetails().setMaxMessageRetries(3);
        assertThatThrownBy(() -> ClientApiResolver.resolve(retries)).hasMessageContaining("maxMessageRetries");

        FunctionDetails timeout = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT);
        timeout.getSource().setSubscriptionType(SubscriptionType.KEY_SHARED).setTimeoutMs(1000);
        assertThatThrownBy(() -> ClientApiResolver.resolve(timeout)).hasMessageContaining("timeoutMs");

        // a shared subscription is a V5 queue, which honors them
        FunctionDetails shared = function(ClientApi.AUTO, SCALABLE_IN, SCALABLE_OUT);
        shared.setRetryDetails().setMaxMessageRetries(3);
        shared.getSource().setTimeoutMs(1000);
        assertThat(ClientApiResolver.resolve(shared)).isEqualTo(ClientApi.V5);
    }
}

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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

@Slf4j
public class PulsarSourceTest {

    private static final String SUBSCRIPTION_NAME = "test/test-namespace/example";
    private static Map<String, String> topicSerdeClassNameMap = new HashMap<>();
    static {
        topicSerdeClassNameMap.put("persistent://sample/standalone/ns1/test_result", DefaultSerDe.class.getName());
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
     * Verify that JavaInstance does not support functions that take Void type as input
     */

    private static PulsarClient getPulsarClient() throws PulsarClientException {
        PulsarClient pulsarClient = mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = mock(ConsumerBuilder.class);
        doReturn(consumerBuilder).when(consumerBuilder).topics(anyList());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionName(anyString());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionType(any());
        doReturn(consumerBuilder).when(consumerBuilder).ackTimeout(anyLong(), any());
        Consumer consumer = mock(Consumer.class);
        doReturn(consumer).when(consumerBuilder).subscribe();
        doReturn(consumerBuilder).when(pulsarClient).newConsumer();
        return pulsarClient;
    }


    private static Map<String, Object> getSourceConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(PulsarConstants.PROCESSING_GUARANTEES, FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        configs.put(PulsarConstants.SUBSCRIPTION_TYPE, FunctionConfig.SubscriptionType.FAILOVER);
        configs.put(PulsarConstants.SUBSCRIPTION_NAME, SUBSCRIPTION_NAME);
        configs.put(PulsarConstants.TOPIC_SERDE_CLASS_NAME_MAP, topicSerdeClassNameMap);
        configs.put(PulsarConstants.TYPE_CLASS_NAME, String.class.getName());
        return configs;
    }

    @Test
    public void testVoidInputClasses() throws IOException {
        Map<String, Object> configs = getSourceConfigs();
        // set type to void
        configs.put(PulsarConstants.TYPE_CLASS_NAME, Void.class.getName());
        PulsarSource pulsarSource = new PulsarSource(getPulsarClient(), PulsarConfig.load(configs));

        try {
            pulsarSource.open(configs);
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
        Map<String, Object> configs = getSourceConfigs();
        // set type to be inconsistent to that of SerDe
        configs.put(PulsarConstants.TYPE_CLASS_NAME, Integer.class.getName());
        Map<String, String> topicSerdeClassNameMap = new HashMap<>();
        topicSerdeClassNameMap.put("persistent://sample/standalone/ns1/test_result", TestSerDe.class.getName());
        configs.put(PulsarConstants.TOPIC_SERDE_CLASS_NAME_MAP, topicSerdeClassNameMap);
        PulsarSource pulsarSource = new PulsarSource(getPulsarClient(), PulsarConfig.load(configs));
        try {
            pulsarSource.open(configs);
            fail("Should fail constructing java instance if function type is inconsistent with serde type");
        } catch (RuntimeException ex) {
            log.error("RuntimeException: {}", ex, ex);
            assertTrue(ex.getMessage().startsWith("Inconsistent types found between function input type and input serde type:"));
        } catch (Exception ex) {
            log.error("Exception: {}", ex, ex);
            assertTrue(false);
        }
    }
}

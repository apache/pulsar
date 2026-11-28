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
package org.apache.pulsar.broker.intercept;

import static org.testng.Assert.assertEquals;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerInterceptorLoneCreateFailureTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    public void setup() throws Exception {
        conf.setSystemTopicEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        enableBrokerInterceptor = true;
        BrokerInterceptorPartialFailureTest.SelectiveFailingInterceptor.FAIL_CREATE.set(true);
        BrokerInterceptorPartialFailureTest.SelectiveFailingInterceptor.FAIL_CLOSE.set(false);
        BrokerInterceptorPartialFailureTest.SelectiveFailingInterceptor.resetCloseInvocations();
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        pulsarTestContextBuilder.brokerInterceptor(
                new BrokerInterceptorPartialFailureTest.SelectiveFailingInterceptor());
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testProducerCloseSkippedWhenCreateFails() throws PulsarClientException {
        String topic = "persistent://public/default/lone-create-fail-producer-" + UUID.randomUUID();
        @Cleanup
        Producer<Boolean> producer = pulsarClient.newProducer(Schema.BOOL).topic(topic).create();
        producer.close();
        Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(
                        BrokerInterceptorPartialFailureTest.SelectiveFailingInterceptor.PRODUCER_CLOSE_INVOCATIONS
                                .get(),
                        0));
    }

    @Test
    public void testConsumerCloseSkippedWhenCreateFails() throws PulsarClientException {
        String topic = "persistent://public/default/lone-create-fail-consumer-" + UUID.randomUUID();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        consumer.close();
        Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(
                        BrokerInterceptorPartialFailureTest.SelectiveFailingInterceptor.CONSUMER_CLOSE_INVOCATIONS
                                .get(),
                        0));
    }
}
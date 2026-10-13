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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PublishContextCompletionListenerTest extends ProducerConsumerBase {

    private CounterBrokerInterceptor counterBrokerInterceptor;

    @BeforeMethod
    public void setup() throws Exception {
        this.enableBrokerInterceptor = true;
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        Map<String, BrokerInterceptorWithClassLoader> listenerMap = new HashMap<>();
        counterBrokerInterceptor = new CounterBrokerInterceptor();
        // The filter chain uses this context class loader for resource and service-provider lookup.
        NarClassLoader narClassLoader = mock(NarClassLoader.class,
                delegatesTo(new URLClassLoader(new URL[0], getClass().getClassLoader())));
        listenerMap.put(CounterBrokerInterceptor.NAME,
                new BrokerInterceptorWithClassLoader(counterBrokerInterceptor, narClassLoader));
        pulsarTestContextBuilder.brokerInterceptor(new BrokerInterceptors(listenerMap));
    }

    @Test
    public void testPublishCompletionListenerOnSuccess() throws PulsarClientException {
        counterBrokerInterceptor.clearCompletionOutcomes();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("test-publish-completion-success")
                .create();

        producer.send("hello world");

        Awaitility.await().untilAsserted(() -> {
            assertEquals(counterBrokerInterceptor.getCompletionOutcomes().size(), 1);
            CounterBrokerInterceptor.CompletionOutcome outcome =
                    counterBrokerInterceptor.getCompletionOutcomes().get(0);
            assertTrue(outcome.getLedgerId() >= 0);
            assertTrue(outcome.getEntryId() >= 0);
            assertNull(outcome.getError());
        });
    }

    @Test
    public void testPublishCompletionListenerOnFailure() throws Exception {
        String topic = "persistent://public/default/test-publish-completion-failure";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .create();

        // A topic-level limit below the client-side 5MB default makes the broker reject the publish
        // deterministically, without tearing down the producer connection.
        admin.topicPolicies().setMaxMessageSize(topic, 10);
        Awaitility.await().untilAsserted(() -> {
            AbstractTopic abstractTopic =
                    (AbstractTopic) pulsar.getBrokerService().getTopicIfExists(topic).join().orElseThrow();
            assertEquals(abstractTopic.getHierarchyTopicPolicies().getTopicMaxMessageSize().get(),
                    Integer.valueOf(10));
        });

        // The policy update itself publishes to the __change_events system topic through the interceptor
        // chain; clear the recorded outcomes so only the rejected publish is accounted for.
        counterBrokerInterceptor.clearCompletionOutcomes();

        assertThrows(PulsarClientException.class, () -> producer.send(new byte[1024]));

        Awaitility.await().untilAsserted(() -> {
            assertEquals(counterBrokerInterceptor.getCompletionOutcomes().size(), 1);
            CounterBrokerInterceptor.CompletionOutcome outcome =
                    counterBrokerInterceptor.getCompletionOutcomes().get(0);
            assertEquals(outcome.getLedgerId(), -1L);
            assertEquals(outcome.getEntryId(), -1L);
            assertNotNull(outcome.getError());
        });
    }

    @Test
    public void testPublishCompletionListenerOnNonPersistentTopic() throws PulsarClientException {
        counterBrokerInterceptor.clearCompletionOutcomes();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("non-persistent://public/default/test-publish-completion-non-persistent")
                .create();

        producer.send("hello world");

        // NonPersistentTopic completes with the fixed 0:0 position: same terminal notification,
        // no persisted entry.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(counterBrokerInterceptor.getCompletionOutcomes().size(), 1);
            CounterBrokerInterceptor.CompletionOutcome outcome =
                    counterBrokerInterceptor.getCompletionOutcomes().get(0);
            assertEquals(outcome.getLedgerId(), 0L);
            assertEquals(outcome.getEntryId(), 0L);
            assertNull(outcome.getError());
        });
    }

    @Test
    public void testPublishCompletionListenerExceptionContainment() throws PulsarClientException {
        counterBrokerInterceptor.clearCompletionOutcomes();
        counterBrokerInterceptor.setFailCompletionListener(true);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("test-publish-completion-exception-containment")
                .create();

        assertNotNull(producer.send("hello world"));

        Awaitility.await().untilAsserted(() -> {
            assertEquals(counterBrokerInterceptor.getCompletionOutcomes().size(), 1);
            CounterBrokerInterceptor.CompletionOutcome outcome =
                    counterBrokerInterceptor.getCompletionOutcomes().get(0);
            assertNull(outcome.getError());
        });
    }
}

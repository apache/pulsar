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
package org.apache.pulsar.broker.service;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicFencedException;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AbstractTopicTest {
    private AbstractSubscription subscription;
    private AbstractTopic topic;

    @BeforeMethod
    public void beforeMethod() {
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);
        BacklogQuotaManager backlogQuotaManager = mock(BacklogQuotaManager.class);
        subscription = mock(AbstractSubscription.class);

        when(brokerService.pulsar()).thenReturn(pulsarService);
        doReturn(pulsarService).when(brokerService).getPulsar();
        when(pulsarService.getConfiguration()).thenReturn(serviceConfiguration);
        when(brokerService.getBacklogQuotaManager()).thenReturn(backlogQuotaManager);
        doReturn(AsyncTokenBucket.DEFAULT_SNAPSHOT_CLOCK).when(pulsarService).getMonotonicClock();
        when(brokerService.checkTopicNsOwnership(anyString())).thenReturn(CompletableFuture.completedFuture(null));

        topic = mock(AbstractTopic.class, withSettings()
                .useConstructor("topic", brokerService)
                .defaultAnswer(CALLS_REAL_METHODS));

        final var subscriptions = new ConcurrentHashMap<String, Subscription>();
        subscriptions.put("subscription", subscription);
        when(topic.getSubscriptions()).thenAnswer(invocation -> subscriptions);
    }

    @Test
    public void testGetMsgOutCounter() {
        topic.msgOutFromRemovedSubscriptions.add(1L);
        when(subscription.getMsgOutCounter()).thenReturn(2L);
        assertEquals(topic.getMsgOutCounter(), 3L);
    }

    @Test
    public void testGetBytesOutCounter() {
        topic.bytesOutFromRemovedSubscriptions.add(1L);
        when(subscription.getBytesOutCounter()).thenReturn(2L);
        assertEquals(topic.getBytesOutCounter(), 3L);
    }

    @Test
    public void testOverwriteOldProducerAfterTopicFenced() {
        TransportCnx oldCnx = mock(TransportCnx.class);
        Producer oldProducer = mockProducer(oldCnx);
        topic.addProducer(oldProducer, new CompletableFuture<>()).join();

        // The old producer's connection is gone, but the liveness check completes only after the topic is fenced
        CompletableFuture<Optional<Boolean>> liveness = new CompletableFuture<>();
        when(oldCnx.checkConnectionLiveness()).thenReturn(liveness);

        Producer newProducer = mockProducer(mock(TransportCnx.class));
        CompletableFuture<Optional<Long>> addFuture = topic.addProducer(newProducer, new CompletableFuture<>());
        assertFalse(addFuture.isDone());

        // Topic close fences the topic and disconnects the producers it knows about
        topic.isFenced = true;
        topic.getProducers().remove(oldProducer.getProducerName(), oldProducer);
        liveness.complete(Optional.of(false));

        CompletionException e = expectThrows(CompletionException.class, addFuture::join);
        assertTrue(e.getCause() instanceof TopicFencedException, e.getCause().toString());
        assertTrue(topic.getProducers().isEmpty());
    }

    @Test
    public void testOverwriteOldProducerWithDeadConnection() {
        TransportCnx oldCnx = mock(TransportCnx.class);
        Producer oldProducer = mockProducer(oldCnx);
        topic.addProducer(oldProducer, new CompletableFuture<>()).join();

        CompletableFuture<Optional<Boolean>> liveness = new CompletableFuture<>();
        when(oldCnx.checkConnectionLiveness()).thenReturn(liveness);

        Producer newProducer = mockProducer(mock(TransportCnx.class));
        CompletableFuture<Optional<Long>> addFuture = topic.addProducer(newProducer, new CompletableFuture<>());
        topic.getProducers().remove(oldProducer.getProducerName(), oldProducer);
        liveness.complete(Optional.of(false));

        addFuture.join();
        assertSame(topic.getProducers().get(newProducer.getProducerName()), newProducer);
    }

    private Producer mockProducer(TransportCnx cnx) {
        Producer producer = mock(Producer.class);
        when(producer.getTopic()).thenReturn(topic);
        when(producer.getProducerName()).thenReturn("producer");
        when(producer.getAccessMode()).thenReturn(ProducerAccessMode.Shared);
        when(producer.getCnx()).thenReturn(cnx);
        return producer;
    }
}

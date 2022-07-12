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
package org.apache.pulsar.broker.service;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
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
        when(pulsarService.getConfiguration()).thenReturn(serviceConfiguration);
        when(brokerService.getBacklogQuotaManager()).thenReturn(backlogQuotaManager);

        topic = mock(AbstractTopic.class, withSettings()
                .useConstructor("topic", brokerService)
                .defaultAnswer(CALLS_REAL_METHODS));

        ConcurrentOpenHashMap<String, Subscription> subscriptions =
                ConcurrentOpenHashMap.<String, Subscription>newBuilder()
                        .expectedItems(16)
                        .concurrencyLevel(1)
                        .build();
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
}

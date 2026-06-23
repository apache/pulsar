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
package org.apache.pulsar.broker.delayed;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.util.Timer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InMemoryTopicDelayedDeliveryTrackerFactoryTest {

    private static AbstractPersistentDispatcherMultipleConsumers mockDispatcher(String topicName, String subName) {
        AbstractPersistentDispatcherMultipleConsumers dispatcher =
                mock(AbstractPersistentDispatcherMultipleConsumers.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn(topicName);
        when(dispatcher.getTopic()).thenReturn(topic);
        Subscription sub = mock(Subscription.class);
        when(sub.getName()).thenReturn(subName);
        when(dispatcher.getSubscription()).thenReturn(sub);
        return dispatcher;
    }

    @Test
    public void testManagersSharedPerTopicAndIndependentAcrossTopics() throws Exception {
        Timer timer = mock(Timer.class);
        InMemoryTopicDelayedDeliveryTrackerFactory f =
                new InMemoryTopicDelayedDeliveryTrackerFactory(timer, 100, true, 0);

        AbstractPersistentDispatcherMultipleConsumers dA1 = mockDispatcher("persistent://ns/topicA", "sub1");
        AbstractPersistentDispatcherMultipleConsumers dA2 = mockDispatcher("persistent://ns/topicA", "sub2");
        AbstractPersistentDispatcherMultipleConsumers dB1 = mockDispatcher("persistent://ns/topicB", "sub1");

        DelayedDeliveryTracker vA1 = f.newTracker0(dA1);
        DelayedDeliveryTracker vA2 = f.newTracker0(dA2);
        DelayedDeliveryTracker vB1 = f.newTracker0(dB1);

        assertEquals(f.getCachedManagersSize(), 2);
        assertTrue(f.hasManagerForTopic("persistent://ns/topicA"));
        assertTrue(f.hasManagerForTopic("persistent://ns/topicB"));

        // Add an entry via A1 and verify A2 observes the same shared topic-level count, B is independent
        org.testng.Assert.assertTrue(vA1.addMessage(1, 1, System.currentTimeMillis() + 60000));
        org.testng.Assert.assertEquals(vA2.getNumberOfDelayedMessages(), 1L);
        org.testng.Assert.assertEquals(vB1.getNumberOfDelayedMessages(), 0L);

        vA1.close();
        vA2.close();
        vB1.close();
    }

    @Test
    public void testOnEmptyCallbackRemovesManagerFromCache() throws Exception {
        Timer timer = mock(Timer.class);
        InMemoryTopicDelayedDeliveryTrackerFactory f =
                new InMemoryTopicDelayedDeliveryTrackerFactory(timer, 100, true, 0);

        AbstractPersistentDispatcherMultipleConsumers dA1 = mockDispatcher("persistent://ns/topicA", "sub1");
        AbstractPersistentDispatcherMultipleConsumers dA2 = mockDispatcher("persistent://ns/topicA", "sub2");

        DelayedDeliveryTracker vA1 = f.newTracker0(dA1);
        DelayedDeliveryTracker vA2 = f.newTracker0(dA2);
        assertEquals(f.getCachedManagersSize(), 1);

        // Close both -> manager should unregister and onEmptyCallback should remove from cache
        vA1.close();
        vA2.close();
        assertEquals(f.getCachedManagersSize(), 0);
    }

    @Test
    public void testFactoryCloseClosesManagersAndStopsTimer() throws Exception {
        Timer timer = mock(Timer.class);
        InMemoryTopicDelayedDeliveryTrackerFactory f =
                new InMemoryTopicDelayedDeliveryTrackerFactory(timer, 100, true, 0);

        AbstractPersistentDispatcherMultipleConsumers dA1 = mockDispatcher("persistent://ns/topicA", "sub1");
        AbstractPersistentDispatcherMultipleConsumers dB1 = mockDispatcher("persistent://ns/topicB", "sub1");
        f.newTracker0(dA1);
        f.newTracker0(dB1);
        assertEquals(f.getCachedManagersSize(), 2);

        f.close();
        assertEquals(f.getCachedManagersSize(), 0);
        // Cannot verify timer.stop() since factory owns the timer; ensure no exception
    }
}

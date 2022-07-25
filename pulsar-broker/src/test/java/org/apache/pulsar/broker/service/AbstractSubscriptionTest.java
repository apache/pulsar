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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Collections;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AbstractSubscriptionTest {
    private Consumer consumer;
    private AbstractSubscription subscription;

    @BeforeMethod
    public void beforeMethod() {
        Dispatcher dispatcher = mock(Dispatcher.class);
        consumer = mock(Consumer.class);
        subscription = spy(AbstractSubscription.class);

        when(subscription.getDispatcher()).thenReturn(dispatcher);
        when(dispatcher.getConsumers()).thenReturn(Collections.singletonList(consumer));
    }

    @Test
    public void testGetMsgOutCounter() {
        subscription.msgOutFromRemovedConsumer.add(1L);
        when(consumer.getMsgOutCounter()).thenReturn(2L);
        assertEquals(subscription.getMsgOutCounter(), 3L);
    }

    @Test
    public void testGetBytesOutCounter() {
        subscription.bytesOutFromRemovedConsumers.add(1L);
        when(consumer.getBytesOutCounter()).thenReturn(2L);
        assertEquals(subscription.getBytesOutCounter(), 3L);
    }
}

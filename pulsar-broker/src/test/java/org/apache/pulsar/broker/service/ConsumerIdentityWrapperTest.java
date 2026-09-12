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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ConsumerIdentityWrapperTest {
    private static Consumer mockConsumer() {
        return mockConsumer("consumer");
    }

    private static Consumer mockConsumer(String consumerName) {
        Consumer consumer = mock(Consumer.class);
        when(consumer.consumerName()).thenReturn(consumerName);
        return consumer;
    }

    @Test
    public void testEquals() {
        Consumer consumer = mockConsumer();
        assertEquals(new ConsumerIdentityWrapper(consumer), new ConsumerIdentityWrapper(consumer));
    }

    @Test
    public void testHashCode() {
        Consumer consumer = mockConsumer();
        assertEquals(new ConsumerIdentityWrapper(consumer).hashCode(),
                new ConsumerIdentityWrapper(consumer).hashCode());
    }

    @Test
    public void testEqualsAndHashCode() {
        Consumer consumer1 = mockConsumer();
        Consumer consumer2 = mockConsumer();
        ConsumerIdentityWrapper wrapper1 = new ConsumerIdentityWrapper(consumer1);
        ConsumerIdentityWrapper wrapper2 = new ConsumerIdentityWrapper(consumer1);
        ConsumerIdentityWrapper wrapper3 = new ConsumerIdentityWrapper(consumer2);

        // Test equality
        assertEquals(wrapper1, wrapper2);
        assertNotEquals(wrapper1, wrapper3);

        // Test hash code
        assertEquals(wrapper1.hashCode(), wrapper2.hashCode());
        assertNotEquals(wrapper1.hashCode(), wrapper3.hashCode());
    }
}
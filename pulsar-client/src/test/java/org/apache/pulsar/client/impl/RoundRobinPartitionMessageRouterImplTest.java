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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Message;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link RoundRobinPartitionMessageRouterImpl}.
 */
@PrepareForTest({ RoundRobinPartitionMessageRouterImpl.class })
public class RoundRobinPartitionMessageRouterImplTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @Test
    public void testChoosePartitionWithoutKey() {
        Message<?> msg = mock(Message.class);
        when(msg.getKey()).thenReturn(null);

        RoundRobinPartitionMessageRouterImpl router = new RoundRobinPartitionMessageRouterImpl(HashingScheme.JavaStringHash, 0);
        for (int i = 0; i < 10; i++) {
            assertEquals(i % 5, router.choosePartition(msg, new TopicMetadataImpl(5)));
        }
    }

    @Test
    public void testChoosePartitionWithKey() {
        String key1 = "key1";
        String key2 = "key2";
        Message<?> msg1 = mock(Message.class);
        when(msg1.hasKey()).thenReturn(true);
        when(msg1.getKey()).thenReturn(key1);
        Message<?> msg2 = mock(Message.class);
        when(msg2.hasKey()).thenReturn(true);
        when(msg2.getKey()).thenReturn(key2);

        RoundRobinPartitionMessageRouterImpl router = new RoundRobinPartitionMessageRouterImpl(HashingScheme.JavaStringHash, 0);
        TopicMetadataImpl metadata = new TopicMetadataImpl(100);

        assertEquals(key1.hashCode() % 100, router.choosePartition(msg1, metadata));
        assertEquals(key2.hashCode() % 100, router.choosePartition(msg2, metadata));
    }

    @Test
    public void testBatchingAwareness() throws Exception {
        Message<?> msg = mock(Message.class);
        when(msg.getKey()).thenReturn(null);

        PowerMockito.mockStatic(System.class);

        RoundRobinPartitionMessageRouterImpl router = new RoundRobinPartitionMessageRouterImpl(HashingScheme.JavaStringHash, 0, true, 10);
        TopicMetadataImpl metadata = new TopicMetadataImpl(100);

        // time at `12345*` milliseconds
        for (int i = 0; i < 10; i++) {
            PowerMockito.when(System.currentTimeMillis()).thenReturn(123450L + i);

            assertEquals(45, router.choosePartition(msg, metadata));
        }

        // time at `12346*` milliseconds
        for (int i = 0; i < 10; i++) {
            PowerMockito.when(System.currentTimeMillis()).thenReturn(123460L + i);

            assertEquals(46, router.choosePartition(msg, metadata));
        }
    }
}

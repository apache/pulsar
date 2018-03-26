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
package org.apache.pulsar.functions.instance;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.impl.Hash;
import org.apache.pulsar.client.impl.Murmur3_32Hash;
import org.apache.pulsar.client.impl.RoundRobinPartitionMessageRouterImpl;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionResultRouter}.
 */
@PrepareForTest({ RoundRobinPartitionMessageRouterImpl.class })
public class FunctionResultRouterTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private Hash hash;

    @BeforeMethod
    public void setup() {
        this.hash = Murmur3_32Hash.getInstance();
    }

    @Test
    public void testChoosePartitionWithoutKeyWithoutSequenceId() {
        Message msg = mock(Message.class);
        when(msg.hasKey()).thenReturn(false);
        when(msg.getKey()).thenReturn(null);
        when(msg.getSequenceId()).thenReturn(-1L);
        TopicMetadata topicMetadata = mock(TopicMetadata.class);
        when(topicMetadata.numPartitions()).thenReturn(5);

        PowerMockito.mockStatic(System.class);

        FunctionResultRouter router = new FunctionResultRouter(0);
        for (int i = 0; i < 10; i++) {
            PowerMockito.when(System.currentTimeMillis()).thenReturn(123450L + i);
            assertEquals(i % 5, router.choosePartition(msg, topicMetadata));
        }
    }

    @Test
    public void testChoosePartitionWithoutKeySequenceId() {
        TopicMetadata topicMetadata = mock(TopicMetadata.class);
        when(topicMetadata.numPartitions()).thenReturn(5);

        FunctionResultRouter router = new FunctionResultRouter(0);
        for (int i = 0; i < 10; i++) {
            Message msg = mock(Message.class);
            when(msg.hasKey()).thenReturn(false);
            when(msg.getKey()).thenReturn(null);
            when(msg.getSequenceId()).thenReturn((long) (2 * i));
            assertEquals((2 * i) % 5, router.choosePartition(msg, topicMetadata));
        }
    }

    @Test
    public void testChoosePartitionWithKeyWithoutSequenceId() {
        String key1 = "key1";
        String key2 = "key2";
        Message msg1 = mock(Message.class);
        when(msg1.hasKey()).thenReturn(true);
        when(msg1.getKey()).thenReturn(key1);
        when(msg1.getSequenceId()).thenReturn(-1L);
        Message msg2 = mock(Message.class);
        when(msg2.hasKey()).thenReturn(true);
        when(msg2.getKey()).thenReturn(key2);
        when(msg1.getSequenceId()).thenReturn(-1L);

        FunctionResultRouter router = new FunctionResultRouter(0);
        TopicMetadata metadata = mock(TopicMetadata.class);
        when(metadata.numPartitions()).thenReturn(100);

        assertEquals(hash.makeHash(key1) % 100, router.choosePartition(msg1, metadata));
        assertEquals(hash.makeHash(key2) % 100, router.choosePartition(msg2, metadata));
    }

    @Test
    public void testChoosePartitionWithKeySequenceId() {
        String key1 = "key1";
        String key2 = "key2";
        Message msg1 = mock(Message.class);
        when(msg1.hasKey()).thenReturn(true);
        when(msg1.getKey()).thenReturn(key1);
        // make sure sequence id is different from hashcode, so the test can be tested correctly.
        when(msg1.getSequenceId()).thenReturn((long) ((key1.hashCode() % 100) + 1));
        Message msg2 = mock(Message.class);
        when(msg2.hasKey()).thenReturn(true);
        when(msg2.getKey()).thenReturn(key2);
        when(msg1.getSequenceId()).thenReturn((long) ((key2.hashCode() % 100) + 1));

        FunctionResultRouter router = new FunctionResultRouter(0);
        TopicMetadata metadata = mock(TopicMetadata.class);
        when(metadata.numPartitions()).thenReturn(100);

        assertEquals(hash.makeHash(key1) % 100, router.choosePartition(msg1, metadata));
        assertEquals(hash.makeHash(key2) % 100, router.choosePartition(msg2, metadata));
    }

}

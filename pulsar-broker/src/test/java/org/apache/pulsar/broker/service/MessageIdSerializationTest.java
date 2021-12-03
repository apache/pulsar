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

import static org.testng.Assert.assertEquals;
import java.io.IOException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MessageIdSerializationTest {

    @Test
    public void testProtobufSerialization1() throws Exception {
        MessageId id = new MessageIdImpl(1, 2, 3);
        byte[] serializedId = id.toByteArray();
        assertEquals(MessageId.fromByteArray(serializedId), id);
    }

    @Test
    public void testProtobufSerialization2() throws Exception {
        MessageId id = new MessageIdImpl(1, 2, -1);
        byte[] serializedId = id.toByteArray();
        assertEquals(MessageId.fromByteArray(serializedId), id);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testProtobufSerializationNull() throws Exception {
        MessageId.fromByteArray(null);
    }

    @Test(expectedExceptions = IOException.class)
    void testProtobufSerializationEmpty() throws Exception {
        MessageId.fromByteArray(new byte[0]);
    }

    @Test
    public void testMultiMessageIdTypeSerialization() throws Exception {
        MessageId id = new MessageIdImpl(1L, 2L, -1);
        MessageId partitionedMessageId = new MessageIdImpl(1L, 2L, 2);
        BatchMessageIdImpl batchMessageId = new BatchMessageIdImpl(1L, 2L, -1, 4);
        BatchMessageIdImpl batchPartitionedMessageId = new BatchMessageIdImpl(1L, 2L, 2, 4);
        String topic = "topic-01";

        MessageId newId = MessageId.fromByteArray(id.toByteArray());
        assert(newId instanceof MessageIdImpl);

        newId = MessageId.fromByteArray(batchMessageId.toByteArray());
        assert(newId instanceof BatchMessageIdImpl);

        newId = MessageId.fromByteArrayWithTopic(id.toByteArray(), topic);
        assert(newId instanceof MessageIdImpl);

        newId = MessageId.fromByteArrayWithTopic(batchMessageId.toByteArray(), topic);
        assert(newId instanceof MessageIdImpl);

        newId = MessageId.fromByteArrayWithTopic(partitionedMessageId.toByteArray(), topic);
        assert(newId instanceof TopicMessageIdImpl);

        newId = MessageId.fromByteArrayWithTopic(batchPartitionedMessageId.toByteArray(), topic);
        assert(newId instanceof TopicMessageIdImpl);

        newId = MessageIdImpl.fromByteArrayWithTopic(id.toByteArray(), topic, true);
        assert(newId instanceof TopicMessageIdImpl);

        newId = MessageIdImpl.fromByteArrayWithTopic(batchMessageId.toByteArray(), topic, true);
        assert(newId instanceof TopicMessageIdImpl);
    }
}

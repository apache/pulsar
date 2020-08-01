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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.testng.annotations.Test;

/**
 * Unit test of {@link Message} methods.
 */
public class MessageTest {

    @Test
    public void testMessageImplReplicatedInfo() {
        String from = "ClusterNameOfReplicatedFrom";
        MessageMetadata.Builder builder = MessageMetadata.newBuilder().setReplicatedFrom(from);
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        Message<byte[]> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertTrue(msg.isReplicated());
        assertEquals(msg.getReplicatedFrom(), from);
    }

    @Test
    public void testMessageImplNoReplicatedInfo() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder();
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        Message<byte[]> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertFalse(msg.isReplicated());
        assertTrue(msg.getReplicatedFrom().isEmpty());
    }

    @Test
    public void testTopicMessageImplReplicatedInfo() {
        String from = "ClusterNameOfReplicatedFromForTopicMessage";
        String topicName = "myTopic";
        MessageMetadata.Builder builder = MessageMetadata.newBuilder().setReplicatedFrom(from);
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<byte[]> msg = MessageImpl.create(builder, payload, Schema.BYTES);
        msg.setMessageId(new MessageIdImpl(-1, -1, -1));
        TopicMessageImpl<byte[]> topicMessage = new TopicMessageImpl<>(topicName, topicName, msg);

        assertTrue(topicMessage.isReplicated());
        assertEquals(msg.getReplicatedFrom(), from);
    }

    @Test
    public void testTopicMessageImplNoReplicatedInfo() {
        String topicName = "myTopic";
        MessageMetadata.Builder builder = MessageMetadata.newBuilder();
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<byte[]> msg = MessageImpl.create(builder, payload, Schema.BYTES);
        msg.setMessageId(new MessageIdImpl(-1, -1, -1));
        TopicMessageImpl<byte[]> topicMessage = new TopicMessageImpl<>(topicName, topicName, msg);

        assertFalse(topicMessage.isReplicated());
        assertTrue(topicMessage.getReplicatedFrom().isEmpty());
    }
}

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
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.Test;

public class TopicMessageIdImplTest {
    @Test
    public void hashCodeTest() {
        MessageIdImpl msgId1 = new MessageIdImpl(0, 0, 0);
        MessageIdImpl msgId2 = new BatchMessageIdImpl(1, 1, 1, 1);
        TopicMessageIdImpl topicMsgId1 = new TopicMessageIdImpl("topic-partition-1", "topic", msgId1);
        TopicMessageIdImpl topic2MsgId1 = new TopicMessageIdImpl("topic2-partition-1", "topic2", msgId1);
        TopicMessageIdImpl topicMsgId2 = new TopicMessageIdImpl("topic-partition-2", "topic", msgId2);

        assertEquals(topicMsgId1.hashCode(), topicMsgId1.hashCode());
        assertEquals(topic2MsgId1.hashCode(), topic2MsgId1.hashCode());
        assertEquals(topicMsgId1.hashCode(), msgId1.hashCode());
        assertNotEquals(topicMsgId1.hashCode(), topicMsgId2.hashCode());
        assertEquals(topicMsgId2.hashCode(), msgId2.hashCode());
    }

    @Test
    public void equalsTest() {
        MessageIdImpl msgId1 = new MessageIdImpl(0, 0, 0);
        MessageIdImpl msgId2 = new BatchMessageIdImpl(1, 1, 1, 1);
        TopicMessageIdImpl topicMsgId1 = new TopicMessageIdImpl("topic-partition-1", "topic", msgId1);
        TopicMessageIdImpl topic2MsgId1 = new TopicMessageIdImpl("topic2-partition-1", "topic2", msgId1);
        TopicMessageIdImpl topicMsgId2 = new TopicMessageIdImpl("topic-partition-2", "topic", msgId2);

        assertEquals(topicMsgId1, topicMsgId1);
        assertEquals(topicMsgId1, topic2MsgId1);
        assertEquals(topicMsgId1, msgId1);
        assertEquals(msgId1, topicMsgId1);
        assertNotEquals(topicMsgId1, topicMsgId2);
    }

}

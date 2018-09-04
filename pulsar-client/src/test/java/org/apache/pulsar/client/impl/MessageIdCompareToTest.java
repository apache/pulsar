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

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

/**
 * Test compareTo method in MessageIdImpl and BatchMessageIdImpl
 */
public class MessageIdCompareToTest  {

    @Test
    public void testEqual() {
        MessageIdImpl messageIdImpl1 = new MessageIdImpl(123L, 345L, 567);
        MessageIdImpl messageIdImpl2 = new MessageIdImpl(123L, 345L, 567);

        BatchMessageIdImpl batchMessageId1 = new BatchMessageIdImpl(234L, 345L, 456, 567);
        BatchMessageIdImpl batchMessageId2 = new BatchMessageIdImpl(234L, 345L, 456, 567);

        assertTrue(messageIdImpl1.compareTo(messageIdImpl2) == 0, "Expected to be equal");
        assertTrue(batchMessageId1.compareTo(batchMessageId2) == 0, "Expected to be equal");
    }

    @Test
    public void testGreaterThan() {
        MessageIdImpl messageIdImpl1 = new MessageIdImpl(124L, 345L, 567);
        MessageIdImpl messageIdImpl2 = new MessageIdImpl(123L, 345L, 567);
        MessageIdImpl messageIdImpl3 = new MessageIdImpl(123L, 344L, 567);
        MessageIdImpl messageIdImpl4 = new MessageIdImpl(123L, 344L, 566);

        BatchMessageIdImpl batchMessageId1 = new BatchMessageIdImpl(235L, 345L, 456, 567);
        BatchMessageIdImpl batchMessageId2 = new BatchMessageIdImpl(234L, 346L, 456, 567);
        BatchMessageIdImpl batchMessageId3 = new BatchMessageIdImpl(234L, 345L, 456, 568);
        BatchMessageIdImpl batchMessageId4 = new BatchMessageIdImpl(234L, 345L, 457, 567);
        BatchMessageIdImpl batchMessageId5 = new BatchMessageIdImpl(234L, 345L, 456, 567);

        assertTrue(messageIdImpl1.compareTo(messageIdImpl2) > 0, "Expected to be greater than");
        assertTrue(messageIdImpl1.compareTo(messageIdImpl3) > 0, "Expected to be greater than");
        assertTrue(messageIdImpl1.compareTo(messageIdImpl4) > 0, "Expected to be greater than");
        assertTrue(messageIdImpl2.compareTo(messageIdImpl3) > 0, "Expected to be greater than");
        assertTrue(messageIdImpl2.compareTo(messageIdImpl4) > 0, "Expected to be greater than");
        assertTrue(messageIdImpl3.compareTo(messageIdImpl4) > 0, "Expected to be greater than");

        assertTrue(batchMessageId1.compareTo(batchMessageId2) > 0, "Expected to be greater than");
        assertTrue(batchMessageId1.compareTo(batchMessageId3) > 0, "Expected to be greater than");
        assertTrue(batchMessageId1.compareTo(batchMessageId4) > 0, "Expected to be greater than");
        assertTrue(batchMessageId1.compareTo(batchMessageId5) > 0, "Expected to be greater than");
        assertTrue(batchMessageId2.compareTo(batchMessageId3) > 0, "Expected to be greater than");
        assertTrue(batchMessageId2.compareTo(batchMessageId4) > 0, "Expected to be greater than");
        assertTrue(batchMessageId2.compareTo(batchMessageId5) > 0, "Expected to be greater than");
        assertTrue(batchMessageId3.compareTo(batchMessageId4) > 0, "Expected to be greater than");
        assertTrue(batchMessageId3.compareTo(batchMessageId5) > 0, "Expected to be greater than");
        assertTrue(batchMessageId4.compareTo(batchMessageId5) > 0, "Expected to be greater than");
    }

    @Test
    public void testLessThan() {
        MessageIdImpl messageIdImpl1 = new MessageIdImpl(124L, 345L, 567);
        MessageIdImpl messageIdImpl2 = new MessageIdImpl(123L, 345L, 567);
        MessageIdImpl messageIdImpl3 = new MessageIdImpl(123L, 344L, 567);
        MessageIdImpl messageIdImpl4 = new MessageIdImpl(123L, 344L, 566);

        BatchMessageIdImpl batchMessageId1 = new BatchMessageIdImpl(235L, 345L, 456, 567);
        BatchMessageIdImpl batchMessageId2 = new BatchMessageIdImpl(234L, 346L, 456, 567);
        BatchMessageIdImpl batchMessageId3 = new BatchMessageIdImpl(234L, 345L, 456, 568);
        BatchMessageIdImpl batchMessageId4 = new BatchMessageIdImpl(234L, 345L, 457, 567);
        BatchMessageIdImpl batchMessageId5 = new BatchMessageIdImpl(234L, 345L, 456, 567);

        assertTrue(messageIdImpl2.compareTo(messageIdImpl1) < 0, "Expected to be less than");
        assertTrue(messageIdImpl3.compareTo(messageIdImpl1) < 0, "Expected to be less than");
        assertTrue(messageIdImpl4.compareTo(messageIdImpl1) < 0, "Expected to be less than");
        assertTrue(messageIdImpl3.compareTo(messageIdImpl2) < 0, "Expected to be less than");
        assertTrue(messageIdImpl4.compareTo(messageIdImpl2) < 0, "Expected to be less than");
        assertTrue(messageIdImpl4.compareTo(messageIdImpl3) < 0, "Expected to be less than");

        assertTrue(batchMessageId2.compareTo(batchMessageId1) < 0, "Expected to be less than");
        assertTrue(batchMessageId3.compareTo(batchMessageId1) < 0, "Expected to be less than");
        assertTrue(batchMessageId4.compareTo(batchMessageId1) < 0, "Expected to be less than");
        assertTrue(batchMessageId5.compareTo(batchMessageId1) < 0, "Expected to be less than");
        assertTrue(batchMessageId3.compareTo(batchMessageId2) < 0, "Expected to be less than");
        assertTrue(batchMessageId4.compareTo(batchMessageId2) < 0, "Expected to be less than");
        assertTrue(batchMessageId5.compareTo(batchMessageId2) < 0, "Expected to be less than");
        assertTrue(batchMessageId4.compareTo(batchMessageId3) < 0, "Expected to be less than");
        assertTrue(batchMessageId5.compareTo(batchMessageId3) < 0, "Expected to be less than");
        assertTrue(batchMessageId5.compareTo(batchMessageId4) < 0, "Expected to be less than");
    }

    @Test
    public void testCompareDifferentType() {
        MessageIdImpl messageIdImpl = new MessageIdImpl(123L, 345L, 567);
        BatchMessageIdImpl batchMessageId1 = new BatchMessageIdImpl(123L, 345L, 566, 789);
        BatchMessageIdImpl batchMessageId2 = new BatchMessageIdImpl(123L, 345L, 567, 789);
        BatchMessageIdImpl batchMessageId3 = new BatchMessageIdImpl(messageIdImpl);
        assertTrue(messageIdImpl.compareTo(batchMessageId1) > 0, "Expected to be greater than");
        assertTrue(messageIdImpl.compareTo(batchMessageId2) == 0, "Expected to be equal");
        assertTrue(messageIdImpl.compareTo(batchMessageId3) == 0, "Expected to be equal");
        assertTrue(batchMessageId1.compareTo(messageIdImpl) < 0, "Expected to be less than");
        assertTrue(batchMessageId2.compareTo(messageIdImpl) > 0, "Expected to be greater than");
        assertTrue(batchMessageId3.compareTo(messageIdImpl) == 0, "Expected to be equal");
    }

    @Test
    public void testMessageIdImplCompareToTopicMessageId() {
        MessageIdImpl messageIdImpl = new MessageIdImpl(123L, 345L, 567);
        TopicMessageIdImpl topicMessageId1 = new TopicMessageIdImpl(
            "test-topic-partition-0",
            "test-topic",
            new BatchMessageIdImpl(123L, 345L, 566, 789));
        TopicMessageIdImpl topicMessageId2 = new TopicMessageIdImpl(
            "test-topic-partition-0",
            "test-topic",
            new BatchMessageIdImpl(123L, 345L, 567, 789));
        TopicMessageIdImpl topicMessageId3 = new TopicMessageIdImpl(
            "test-topic-partition-0",
            "test-topic",
            new BatchMessageIdImpl(messageIdImpl));
        assertTrue(messageIdImpl.compareTo(topicMessageId1) > 0, "Expected to be greater than");
        assertTrue(messageIdImpl.compareTo(topicMessageId2) == 0, "Expected to be equal");
        assertTrue(messageIdImpl.compareTo(topicMessageId3) == 0, "Expected to be equal");
        assertTrue(topicMessageId1.compareTo(messageIdImpl) < 0, "Expected to be less than");
        assertTrue(topicMessageId2.compareTo(messageIdImpl) > 0, "Expected to be greater than");
        assertTrue(topicMessageId3.compareTo(messageIdImpl) == 0, "Expected to be equal");
    }

    @Test
    public void testBatchMessageIdImplCompareToTopicMessageId() {
        BatchMessageIdImpl messageIdImpl1 = new BatchMessageIdImpl(123L, 345L, 567, 789);
        BatchMessageIdImpl messageIdImpl2 = new BatchMessageIdImpl(123L, 345L, 567, 0);
        BatchMessageIdImpl messageIdImpl3 = new BatchMessageIdImpl(123L, 345L, 567, -1);
        TopicMessageIdImpl topicMessageId1 = new TopicMessageIdImpl(
            "test-topic-partition-0",
            "test-topic",
            new MessageIdImpl(123L, 345L, 566));
        TopicMessageIdImpl topicMessageId2 = new TopicMessageIdImpl(
            "test-topic-partition-0",
            "test-topic",
            new MessageIdImpl(123L, 345L, 567));
        assertTrue(messageIdImpl1.compareTo(topicMessageId1) > 0, "Expected to be greater than");
        assertTrue(messageIdImpl1.compareTo(topicMessageId2) > 0, "Expected to be greater than");
        assertTrue(messageIdImpl2.compareTo(topicMessageId2) > 0, "Expected to be greater than");
        assertTrue(messageIdImpl3.compareTo(topicMessageId2) == 0, "Expected to be equal");
        assertTrue(topicMessageId1.compareTo(messageIdImpl1) < 0, "Expected to be less than");
        assertTrue(topicMessageId2.compareTo(messageIdImpl1) == 0, "Expected to be equal");
        assertTrue(topicMessageId2.compareTo(messageIdImpl2) == 0, "Expected to be equal");
        assertTrue(topicMessageId2.compareTo(messageIdImpl2) == 0, "Expected to be equal");
    }

}

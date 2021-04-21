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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.pulsar.client.api.MessageId;
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

        assertEquals(messageIdImpl1.compareTo(messageIdImpl2), 0, "Expected to be equal");
        assertEquals(batchMessageId1.compareTo(batchMessageId2), 0, "Expected to be equal");
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
        assertTrue(batchMessageId4.compareTo(batchMessageId3) > 0, "Expected to be greater than");
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
        assertTrue(batchMessageId3.compareTo(batchMessageId4) < 0, "Expected to be less than");
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
        assertTrue(messageIdImpl.compareTo(batchMessageId2) < 0, "Expected to be less than");
        assertEquals(messageIdImpl.compareTo(batchMessageId3), 0, "Expected to be equal");
        assertTrue(batchMessageId1.compareTo(messageIdImpl) < 0, "Expected to be less than");
        assertTrue(batchMessageId2.compareTo(messageIdImpl) > 0, "Expected to be greater than");
        assertEquals(batchMessageId3.compareTo(messageIdImpl), 0, "Expected to be equal");
    }

    @Test
    public void compareToSymmetricTest() {
        MessageIdImpl simpleMessageId = new MessageIdImpl(123L, 345L, 567);
        // batchIndex is -1 if message is non-batched message and has the batchIndex for a batch message
        BatchMessageIdImpl batchMessageId1 = new BatchMessageIdImpl(123L, 345L, 567, -1);
        BatchMessageIdImpl batchMessageId2 = new BatchMessageIdImpl(123L, 345L, 567, 1);
        BatchMessageIdImpl batchMessageId3 = new BatchMessageIdImpl(123L, 345L, 566, 1);
        BatchMessageIdImpl batchMessageId4 = new BatchMessageIdImpl(123L, 345L, 566, -1);

        assertEquals(simpleMessageId.compareTo(batchMessageId1), 0, "Expected to be equal");
        assertEquals(batchMessageId1.compareTo(simpleMessageId), 0, "Expected to be equal");
        assertTrue(batchMessageId2.compareTo(simpleMessageId) > 0, "Expected to be greater than");
        assertTrue(simpleMessageId.compareTo(batchMessageId2) < 0, "Expected to be less than");
        assertTrue(simpleMessageId.compareTo(batchMessageId3) > 0, "Expected to be greater than");
        assertTrue(batchMessageId3.compareTo(simpleMessageId) < 0, "Expected to be less than");
        assertTrue(simpleMessageId.compareTo(batchMessageId4) > 0, "Expected to be greater than");
        assertTrue(batchMessageId4.compareTo(simpleMessageId) < 0, "Expected to be less than");
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
        assertTrue(messageIdImpl.compareTo(topicMessageId2) < 0, "Expected to be less than");
        assertEquals(messageIdImpl.compareTo(topicMessageId3), 0, "Expected to be equal");
        assertTrue(topicMessageId1.compareTo(messageIdImpl) < 0, "Expected to be less than");
        assertTrue(topicMessageId2.compareTo(messageIdImpl) > 0, "Expected to be greater than");
        assertEquals(topicMessageId3.compareTo(messageIdImpl), 0, "Expected to be equal");
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
        assertEquals(messageIdImpl3.compareTo(topicMessageId2), 0, "Expected to be equal");
        assertTrue(topicMessageId1.compareTo(messageIdImpl1) < 0, "Expected to be less than");
        assertTrue(topicMessageId2.compareTo(messageIdImpl1) < 0, "Expected to be less than");
        assertTrue(topicMessageId2.compareTo(messageIdImpl2) < 0, "Expected to be less than");
        assertTrue(topicMessageId2.compareTo(messageIdImpl2) < 0, "Expected to be less than");
    }

    @Test
    public void testMultiMessageIdEqual() {
        // null
        MultiMessageIdImpl null1 = new MultiMessageIdImpl(null);
        MultiMessageIdImpl null2 = new MultiMessageIdImpl(null);
        assertEquals(null1, null2);

        // empty
        MultiMessageIdImpl empty1 = new MultiMessageIdImpl(Collections.emptyMap());
        MultiMessageIdImpl empty2 = new MultiMessageIdImpl(Collections.emptyMap());
        assertEquals(empty1, empty2);

        // null empty
        assertEquals(null1, empty2);
        assertEquals(empty2, null1);

        // 1 item
        String topic1 = "topicName1";
        MessageIdImpl messageIdImpl1 = new MessageIdImpl(123L, 345L, 567);
        MessageIdImpl messageIdImpl2 = new MessageIdImpl(123L, 345L, 567);
        MessageIdImpl messageIdImpl3 = new MessageIdImpl(345L, 456L, 567);

        MultiMessageIdImpl item1 = new MultiMessageIdImpl(Collections.singletonMap(topic1, messageIdImpl1));
        MultiMessageIdImpl item2 = new MultiMessageIdImpl(Collections.singletonMap(topic1, messageIdImpl2));
        assertEquals(item1, item2);

        // 1 item, empty not equal
        assertNotEquals(item1, null1);
        assertNotEquals(null1, item1);

        // key not equal
        String topic2 = "topicName2";
        MultiMessageIdImpl item3 = new MultiMessageIdImpl(Collections.singletonMap(topic2, messageIdImpl2));
        assertNotEquals(item1, item3);
        assertNotEquals(item3, item1);

        // value not equal
        MultiMessageIdImpl item4 = new MultiMessageIdImpl(Collections.singletonMap(topic1, messageIdImpl3));
        assertNotEquals(item1, item4);
        assertNotEquals(item4, item1);

        // key value not equal
        assertNotEquals(item3, item4);
        assertNotEquals(item4, item3);

        // 2 items
        Map<String, MessageId> map1 = Maps.newHashMap();
        Map<String, MessageId> map2 = Maps.newHashMap();
        map1.put(topic1, messageIdImpl1);
        map1.put(topic2, messageIdImpl2);
        map2.put(topic2, messageIdImpl2);
        map2.put(topic1, messageIdImpl1);

        MultiMessageIdImpl item5 = new MultiMessageIdImpl(map1);
        MultiMessageIdImpl item6 = new MultiMessageIdImpl(map2);

        assertEquals(item5, item6);

        assertNotEquals(item5, null1);
        assertNotEquals(item5, empty1);
        assertNotEquals(item5, item1);
        assertNotEquals(item5, item3);
        assertNotEquals(item5, item4);

        assertNotEquals(null1, item5);
        assertNotEquals(empty1, item5);
        assertNotEquals(item1, item5);
        assertNotEquals(item3, item5);
        assertNotEquals(item4, item5);

        map2.put(topic1, messageIdImpl3);
        MultiMessageIdImpl item7 = new MultiMessageIdImpl(map2);
        assertNotEquals(item5, item7);
        assertNotEquals(item7, item5);
    }

    @Test
    public void testMultiMessageIdCompareto() {
        // null
        MultiMessageIdImpl null1 = new MultiMessageIdImpl(null);
        MultiMessageIdImpl null2 = new MultiMessageIdImpl(null);
        assertEquals(0, null1.compareTo(null2));

        // empty
        MultiMessageIdImpl empty1 = new MultiMessageIdImpl(Collections.emptyMap());
        MultiMessageIdImpl empty2 = new MultiMessageIdImpl(Collections.emptyMap());
        assertEquals(0, empty1.compareTo(empty2));

        // null empty
        assertEquals(0, null1.compareTo(empty2));
        assertEquals(0, empty2.compareTo(null1));

        // 1 item
        String topic1 = "topicName1";
        MessageIdImpl messageIdImpl1 = new MessageIdImpl(123L, 345L, 567);
        MessageIdImpl messageIdImpl2 = new MessageIdImpl(123L, 345L, 567);
        MessageIdImpl messageIdImpl3 = new MessageIdImpl(345L, 456L, 567);

        MultiMessageIdImpl item1 = new MultiMessageIdImpl(Collections.singletonMap(topic1, messageIdImpl1));
        MultiMessageIdImpl item2 = new MultiMessageIdImpl(Collections.singletonMap(topic1, messageIdImpl2));
        assertEquals(0, item1.compareTo(item2));

        // 1 item, empty not equal
        try {
            item1.compareTo(null1);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            null1.compareTo(item1);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // key not equal
        String topic2 = "topicName2";
        MultiMessageIdImpl item3 = new MultiMessageIdImpl(Collections.singletonMap(topic2, messageIdImpl2));
        try {
            item1.compareTo(item3);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            item3.compareTo(item1);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // value not equal
        MultiMessageIdImpl item4 = new MultiMessageIdImpl(Collections.singletonMap(topic1, messageIdImpl3));
        assertTrue(item1.compareTo(item4) < 0);
        assertTrue(item4.compareTo(item1) > 0);

        // key value not equal
        try {
            item3.compareTo(item4);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            item4.compareTo(item3);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // 2 items
        Map<String, MessageId> map1 = Maps.newHashMap();
        Map<String, MessageId> map2 = Maps.newHashMap();
        map1.put(topic1, messageIdImpl1);
        map1.put(topic2, messageIdImpl2);
        map2.put(topic2, messageIdImpl2);
        map2.put(topic1, messageIdImpl1);

        MultiMessageIdImpl item5 = new MultiMessageIdImpl(map1);
        MultiMessageIdImpl item6 = new MultiMessageIdImpl(map2);

        assertTrue(item5.compareTo(item6) == 0);

        try {
            item5.compareTo(null1);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            item5.compareTo(empty1);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            item5.compareTo(item1);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            item5.compareTo(item3);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            item5.compareTo(item4);
            fail("should throw exception for not comparable");
        } catch (IllegalArgumentException e) {
            // expected
        }

        map2.put(topic1, messageIdImpl3);
        MultiMessageIdImpl item7 = new MultiMessageIdImpl(map2);

        assertTrue(item7.compareTo(item5) > 0);
        assertTrue(item5.compareTo(item7) < 0);

        Map<String, MessageId> map3 = Maps.newHashMap();
        map3.put(topic1, messageIdImpl3);
        map3.put(topic2, messageIdImpl3);
        MultiMessageIdImpl item8 = new MultiMessageIdImpl(map3);
        assertTrue(item8.compareTo(item5) > 0);
        assertTrue(item8.compareTo(item7) > 0);

        assertTrue(item5.compareTo(item8) < 0);
        assertTrue(item7.compareTo(item8) < 0);
    }
}

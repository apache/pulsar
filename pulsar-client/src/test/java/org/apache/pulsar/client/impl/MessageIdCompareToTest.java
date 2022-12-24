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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.MessageId;
import org.testng.annotations.Test;

/**
 * Test compareTo method in MessageIdImpl and BatchMessageIdImpl
 */
public class MessageIdCompareToTest  {

    private final static String BATCH_MESSAGE_ID_COMPARE_WITH_MESSAGE_ID_THROW_EXCEPTION_MESSAGE = "org.apache.pulsar"
            + ".client.impl.BatchMessageIdImpl can't compare with "
            + "org.apache.pulsar.client.impl.MessageIdImpl when they "
            + "have the same `LedgerId`, `EntryId` and `PartitionIndex`.";

    private final static String MESSAGE_ID_COMPARE_WITH_BATCH_MESSAGE_ID_THROW_EXCEPTION_MESSAGE = "org.apache.pulsar"
            + ".client.impl.MessageIdImpl can't compare with "
            + "org.apache.pulsar.client.impl.BatchMessageIdImpl "
            + "when they have the same `LedgerId`, `EntryId` and `PartitionIndex`.";

    // MessageIdImpl
    private final MessageIdImpl normalMessageIdImpl = new MessageIdImpl(123L, 345L, 567);

    // BatchMessageIdImpl
    private final BatchMessageIdImpl normalBatchMessageId =
            new BatchMessageIdImpl(123L, 345L, 567, 567);
    private final TopicMessageIdImpl normalTopicMessageIdImplByMessageIdImpl = new TopicMessageIdImpl(
            "test-topic-partition-0", "test-topic",
            new MessageIdImpl(123L, 345L, 567));

    private final TopicMessageIdImpl normalTopicMessageIdImplByBatchMessageIdImpl = new TopicMessageIdImpl(
            "test-topic-partition-0", "test-topic",
            new BatchMessageIdImpl(123L, 345L, 567, 567));


    @Test
    public void testEqual() {
        MessageIdImpl messageIdImpl1 = new MessageIdImpl(123L, 345L, 567);

        BatchMessageIdImpl batchMessageId1 = new BatchMessageIdImpl(123L, 345L, 567, 567);

        TopicMessageIdImpl topicMessageIdWithMessageId = new TopicMessageIdImpl(
                "test-topic-partition-0", "test-topic",
                new MessageIdImpl(123L, 345L, 567));

        TopicMessageIdImpl topicMessageIdWithBatchMessageId = new TopicMessageIdImpl(
                "test-topic-partition-0", "test-topic",
                new BatchMessageIdImpl(123L, 345L, 567, 567));


        assertEquals(messageIdImpl1.compareTo(normalMessageIdImpl), 0, "Expected to be equal");
        assertEquals(messageIdImpl1.compareTo(normalTopicMessageIdImplByMessageIdImpl), 0, "Expected to be equal");

        assertEquals(batchMessageId1.compareTo(normalBatchMessageId), 0, "Expected to be equal");
        assertEquals(batchMessageId1.compareTo(normalTopicMessageIdImplByBatchMessageIdImpl),
                0, "Expected to be equal");

        assertEquals(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(normalBatchMessageId), 0, "Expected to be equal");
        assertEquals(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(topicMessageIdWithBatchMessageId), 0, "Expected to be equal");

        assertEquals(normalTopicMessageIdImplByMessageIdImpl.compareTo(normalMessageIdImpl),
                0, "Expected to be equal");
        assertEquals(normalTopicMessageIdImplByMessageIdImpl.compareTo(topicMessageIdWithMessageId),
                0, "Expected to be equal");
    }

//    @Test
//    public void testMessageIdAndBatchIdSpecialEquals() {
//        BatchMessageIdImpl minBatchMessageId = new BatchMessageIdImpl(-1, -1, -1, -1);
//        BatchMessageIdImpl maxBatchMessageId = new BatchMessageIdImpl(Long.MAX_VALUE, Long.MAX_VALUE, -1, -1);
//
//        assertEquals(minBatchMessageId.compareTo(MessageId.earliest), 0, "Expected to be equal");
//        assertEquals(maxBatchMessageId.compareTo(MessageId.latest), 0, "Expected to be equal");
//
//        assertEquals(MessageId.earliest.compareTo(minBatchMessageId), 0, "Expected to be equal");
//        assertEquals(MessageId.latest.compareTo(maxBatchMessageId), 0, "Expected to be equal");
//
//        BatchMessageIdImpl batchMessageIdWithMinEntryId = new BatchMessageIdImpl(123, -1, -1, -1);
//        MessageIdImpl messageIdWithMinEntryId = new MessageIdImpl(123, -1, -1);
//
//        assertEquals(batchMessageIdWithMinEntryId.compareTo(messageIdWithMinEntryId), 0,
//                "Expected to be equal");
//        assertEquals(messageIdWithMinEntryId.compareTo(batchMessageIdWithMinEntryId), 0,
//                "Expected to be equal");
//    }

    @Test
    public void testGreaterThan() {

        // MessageIdImpl
        MessageIdImpl messageIdWithSmallerLedgerId = new MessageIdImpl(122L, 345L, 567);
        MessageIdImpl messageIdWithSmallerEntryId = new MessageIdImpl(123L, 344L, 567);
        MessageIdImpl messageIdWithSmallerPartitionIndex = new MessageIdImpl(123L, 345L, 566);

        // BatchMessageIdImpl
        BatchMessageIdImpl batchMessageIdWithSmallerLedgerId =
                new BatchMessageIdImpl(122L, 345L, 567, 567);
        BatchMessageIdImpl batchMessageIdWithSmallerEntryId =
                new BatchMessageIdImpl(123L, 344L, 567, 567);
        BatchMessageIdImpl batchMessageIdWithSmallerPartitionIndex  =
                new BatchMessageIdImpl(123L, 345L, 566, 567);
        BatchMessageIdImpl batchMessageIdWithSmallerBatchIndex =
                new BatchMessageIdImpl(123L, 345L, 567, 566);

        // TopicMessageIdImpl with BatchMessageIdImpl
        TopicMessageIdImpl topicMessageIdWithSmallerLedgerIdByBatchMessageId =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new BatchMessageIdImpl(122L, 345L, 567, 567));
        TopicMessageIdImpl topicMessageIdWithSmallerEntryIdByBatchMessageId =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new BatchMessageIdImpl(123L, 344L, 567, 567));
        TopicMessageIdImpl topicMessageIdWithSmallerPartitionIndexByBatchMessageId =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new BatchMessageIdImpl(123L, 345L, 566, 567));
        TopicMessageIdImpl topicMessageIdWithSmallerBatchIndexByBatchMessageId =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new BatchMessageIdImpl(123L, 345L, 567, 566));

        // TopicMessageIdImpl with MessageIdImpl
        TopicMessageIdImpl topicMessageIdWithSmallerLedgerIdByMessageId =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new MessageIdImpl(122L, 345L, 567));
        TopicMessageIdImpl topicMessageIdWithSmallerEntryIdByMessageId =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new MessageIdImpl(123L, 344L, 567));
        TopicMessageIdImpl topicMessageIdWithSmallerPartitionIndexByMessageId =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new MessageIdImpl(123L, 345L, 566));


        // MessageIdImpl compare with MessageIdImpl
        assertTrue(normalMessageIdImpl.compareTo(messageIdWithSmallerLedgerId) > 0,
                "Expected to be greater than");
        assertTrue(normalMessageIdImpl.compareTo(messageIdWithSmallerEntryId) > 0,
                "Expected to be greater than");
        assertTrue(normalMessageIdImpl.compareTo(messageIdWithSmallerPartitionIndex) > 0,
                "Expected to be greater than");

        // MessageIdImpl compare with BatchMessageIdImpl
        assertTrue(normalMessageIdImpl.compareTo(batchMessageIdWithSmallerLedgerId) > 0,
                "Expected to be greater than");
        assertTrue(normalMessageIdImpl.compareTo(batchMessageIdWithSmallerEntryId) > 0,
                "Expected to be greater than");
        assertTrue(normalMessageIdImpl.compareTo(batchMessageIdWithSmallerPartitionIndex) > 0,
                "Expected to be greater than");

        // MessageIdImpl compare with TopicMessageIdImpl by MessageIdImpl
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithSmallerLedgerIdByMessageId) > 0,
                "Expected to be greater than");
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithSmallerEntryIdByMessageId) > 0,
                "Expected to be greater than");
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithSmallerPartitionIndexByMessageId) > 0,
                "Expected to be greater than");

        // MessageIdImpl compare with TopicMessageIdImpl by BatchMessageIdImpl
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithSmallerLedgerIdByBatchMessageId) > 0,
                "Expected to be greater than");
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithSmallerEntryIdByBatchMessageId) > 0,
                "Expected to be greater than");
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithSmallerPartitionIndexByBatchMessageId) > 0,
                "Expected to be greater than");

        // BatchMessageIdImpl compare with BatchMessageIdImpl
        assertTrue(normalBatchMessageId.compareTo(batchMessageIdWithSmallerLedgerId) > 0,
                "Expected to be greater than");
        assertTrue(normalBatchMessageId.compareTo(batchMessageIdWithSmallerEntryId) > 0,
                "Expected to be greater than");
        assertTrue(normalBatchMessageId.compareTo(batchMessageIdWithSmallerPartitionIndex) > 0,
                "Expected to be greater than");
        assertTrue(normalBatchMessageId.compareTo(batchMessageIdWithSmallerBatchIndex) > 0,
                "Expected to be greater than");

        // BatchMessageIdImpl compare with MessageIdImpl
        assertTrue(normalBatchMessageId.compareTo(messageIdWithSmallerLedgerId) > 0,
                "Expected to be greater than");
        assertTrue(normalBatchMessageId.compareTo(messageIdWithSmallerEntryId) > 0,
                "Expected to be greater than");
        assertTrue(normalBatchMessageId.compareTo(messageIdWithSmallerPartitionIndex) > 0,
                "Expected to be greater than");

        // BatchMessageIdImpl compare with TopicMessageIdImpl by BatchMessageIdImpl
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithSmallerLedgerIdByBatchMessageId) > 0,
                "Expected to be greater than");
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithSmallerEntryIdByBatchMessageId) > 0,
                "Expected to be greater than");
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithSmallerPartitionIndexByBatchMessageId) > 0,
                "Expected to be greater than");
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithSmallerBatchIndexByBatchMessageId) > 0,
                "Expected to be greater than");

        // BatchMessageIdImpl compare with TopicMessageIdImpl by MessageIdImpl
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithSmallerLedgerIdByMessageId) > 0,
                "Expected to be greater than");
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithSmallerEntryIdByMessageId) > 0,
                "Expected to be greater than");
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithSmallerPartitionIndexByMessageId) > 0,
                "Expected to be greater than");


        // TopicMessageIdImpl by MessageIdImpl compare with TopicMessageIdImpl by MessageIdImpl
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(topicMessageIdWithSmallerLedgerIdByMessageId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl.
                compareTo(topicMessageIdWithSmallerEntryIdByMessageId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(topicMessageIdWithSmallerPartitionIndexByMessageId) > 0, "Expected to be greater than");

        // TopicMessageIdImpl by MessageIdImpl compare with TopicMessageIdImpl by BatchMessageIdImpl
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(topicMessageIdWithSmallerLedgerIdByBatchMessageId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(topicMessageIdWithSmallerEntryIdByBatchMessageId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                        .compareTo(topicMessageIdWithSmallerPartitionIndexByBatchMessageId) > 0,
                "Expected to be greater than");

        // TopicMessageIdImpl by MessageIdImpl compare with MessageIdImpl
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(messageIdWithSmallerLedgerId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl.
                compareTo(messageIdWithSmallerEntryId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(messageIdWithSmallerPartitionIndex) > 0, "Expected to be greater than");

        // TopicMessageIdImpl by MessageIdImpl compare with BatchMessageIdImpl
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(batchMessageIdWithSmallerLedgerId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(batchMessageIdWithSmallerEntryId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(batchMessageIdWithSmallerPartitionIndex) > 0, "Expected to be greater than");

        // TopicMessageIdImpl by BatchMessageIdImpl compare with TopicMessageIdImpl by BatchMessageIdImpl
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(topicMessageIdWithSmallerLedgerIdByBatchMessageId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl.
                compareTo(topicMessageIdWithSmallerEntryIdByBatchMessageId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(topicMessageIdWithSmallerPartitionIndexByBatchMessageId) > 0,
                "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                        .compareTo(topicMessageIdWithSmallerBatchIndexByBatchMessageId) > 0,
                "Expected to be greater than");


        // TopicMessageIdImpl by BatchMessageIdImpl compare with TopicMessageIdImpl by MessageIdImpl
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(topicMessageIdWithSmallerLedgerIdByMessageId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl.
                compareTo(topicMessageIdWithSmallerEntryIdByMessageId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(topicMessageIdWithSmallerPartitionIndexByMessageId) > 0, "Expected to be greater than");

        // TopicMessageIdImpl by BatchMessageIdImpl compare with BatchMessageIdImpl
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(batchMessageIdWithSmallerLedgerId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl.
                compareTo(batchMessageIdWithSmallerEntryId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                        .compareTo(batchMessageIdWithSmallerPartitionIndex) > 0,
                "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                        .compareTo(batchMessageIdWithSmallerBatchIndex) > 0,
                "Expected to be greater than");


        // TopicMessageIdImpl by BatchMessageIdImpl compare with MessageIdImpl
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(messageIdWithSmallerLedgerId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl.
                compareTo(messageIdWithSmallerEntryId) > 0, "Expected to be greater than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(messageIdWithSmallerPartitionIndex) > 0, "Expected to be greater than");
    }

    @Test
    public void testLessThan() {
        // MessageIdImpl
        MessageIdImpl messageIdImplWithLargerLedgerId = new MessageIdImpl(124L, 345L, 567);
        MessageIdImpl messageIdImplWithLargerEntryId = new MessageIdImpl(123L, 346L, 567);
        MessageIdImpl messageIdImplWithLargerPartitionIndex = new MessageIdImpl(124L, 345L, 567);

        // BatchMessageIdImpl
        BatchMessageIdImpl batchMessageIdWithLargerLedgerId  =
                new BatchMessageIdImpl(124L, 345L, 567, 567);
        BatchMessageIdImpl batchMessageIdWithLargerEntryId =
                new BatchMessageIdImpl(123L, 346L, 567, 567);
        BatchMessageIdImpl batchMessageIdWithLargerPartitionIndex  =
                new BatchMessageIdImpl(123L, 345L, 568, 567);
        BatchMessageIdImpl batchMessageIdWithLargerBatchIndex  =
                new BatchMessageIdImpl(123L, 345L, 567, 568);

        // TopicMessageIdImpl with BatchMessageIdImpl
        TopicMessageIdImpl topicMessageIdWithLargerLedgerIdByBatchMessageIdImpl =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new BatchMessageIdImpl(124L, 345L, 567, 567));
        TopicMessageIdImpl topicMessageIdWithLargerEntryIdByBatchMessageIdImpl =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new BatchMessageIdImpl(123L, 346L, 567, 567));
        TopicMessageIdImpl topicMessageIdWithLargerPartitionIndexByBatchMessageIdImpl =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new BatchMessageIdImpl(123L, 345L, 568, 567));
        TopicMessageIdImpl topicMessageIdWithLargerBatchIndexByBatchMessageIdImpl =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new BatchMessageIdImpl(123L, 345L, 567, 568));

        // TopicMessageIdImpl with MessageIdImpl
        TopicMessageIdImpl topicMessageIdWithLargerLedgerIdByMessageIdImpl =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new MessageIdImpl(124L, 345L, 567));
        TopicMessageIdImpl topicMessageIdWithLargerEntryIdByMessageIdImpl =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new MessageIdImpl(123L, 346L, 567));
        TopicMessageIdImpl topicMessageIdWithLargerPartitionIndexMessageIdImpl =
                new TopicMessageIdImpl("test-topic-partition-0",
                        "test-topic", new MessageIdImpl(123L, 345L, 568));

        // MessageIdImpl compare with MessageIdImpl
        assertTrue(normalMessageIdImpl.compareTo(messageIdImplWithLargerLedgerId) < 0,
                "Expected to be less than");
        assertTrue(normalMessageIdImpl.compareTo(messageIdImplWithLargerEntryId) < 0,
                "Expected to be less than");
        assertTrue(normalMessageIdImpl.compareTo(messageIdImplWithLargerPartitionIndex) < 0,
                "Expected to be less than");

        // MessageIdImpl compare with BatchMessageIdImpl
        assertTrue(normalMessageIdImpl.compareTo(batchMessageIdWithLargerLedgerId) < 0,
                "Expected to be less than");
        assertTrue(normalMessageIdImpl.compareTo(batchMessageIdWithLargerEntryId) < 0,
                "Expected to be less than");
        assertTrue(normalMessageIdImpl.compareTo(batchMessageIdWithLargerPartitionIndex) < 0,
                "Expected to be less than");

        // MessageIdImpl compare with TopicMessageIdImpl by MessageIdImpl
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithLargerLedgerIdByMessageIdImpl) < 0,
                "Expected to be less than");
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithLargerEntryIdByMessageIdImpl) < 0,
                "Expected to be less than");
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithLargerPartitionIndexMessageIdImpl) < 0,
                "Expected to be less than");

        // MessageIdImpl compare with TopicMessageIdImpl by BatchMessageIdImpl
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithLargerLedgerIdByBatchMessageIdImpl) < 0,
                "Expected to be less than");
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithLargerEntryIdByBatchMessageIdImpl) < 0,
                "Expected to be less than");
        assertTrue(normalMessageIdImpl.compareTo(topicMessageIdWithLargerPartitionIndexByBatchMessageIdImpl) < 0,
                "Expected to be less than");

        // BatchMessageIdImpl compare with BatchMessageIdImpl
        assertTrue(normalBatchMessageId.compareTo(batchMessageIdWithLargerLedgerId) < 0,
                "Expected to be less than");
        assertTrue(normalBatchMessageId.compareTo(batchMessageIdWithLargerEntryId) < 0,
                "Expected to be less than");
        assertTrue(normalBatchMessageId.compareTo(batchMessageIdWithLargerPartitionIndex) < 0,
                "Expected to be less than");
        assertTrue(normalBatchMessageId.compareTo(batchMessageIdWithLargerBatchIndex) < 0,
                "Expected to be less than");

        // BatchMessageIdImpl compare with MessageIdImpl
        assertTrue(normalBatchMessageId.compareTo(messageIdImplWithLargerLedgerId) < 0,
                "Expected to be less than");
        assertTrue(normalBatchMessageId.compareTo(messageIdImplWithLargerEntryId) < 0,
                "Expected to be less than");
        assertTrue(normalBatchMessageId.compareTo(messageIdImplWithLargerPartitionIndex) < 0,
                "Expected to be less than");

        // BatchMessageIdImpl compare with TopicMessageIdImpl by MessageIdImpl
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithLargerLedgerIdByMessageIdImpl) < 0,
                "Expected to be less than");
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithLargerEntryIdByMessageIdImpl) < 0,
                "Expected to be less than");
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithLargerPartitionIndexMessageIdImpl) < 0,
                "Expected to be less than");

        // BatchMessageIdImpl compare with TopicMessageIdImpl by BatchMessageIdImpl
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithLargerLedgerIdByBatchMessageIdImpl) < 0,
                "Expected to be less than");
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithLargerEntryIdByBatchMessageIdImpl) < 0,
                "Expected to be less than");
        assertTrue(normalBatchMessageId
                .compareTo(topicMessageIdWithLargerPartitionIndexByBatchMessageIdImpl) < 0,
                "Expected to be less than");
        assertTrue(normalBatchMessageId.compareTo(topicMessageIdWithLargerBatchIndexByBatchMessageIdImpl) < 0,
                "Expected to be less than");


        // TopicMessageIdImpl by MessageIdImpl compare with TopicMessageIdImpl by MessageIdImpl
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(topicMessageIdWithLargerLedgerIdByMessageIdImpl) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl.
                compareTo(topicMessageIdWithLargerEntryIdByMessageIdImpl) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(topicMessageIdWithLargerPartitionIndexMessageIdImpl) < 0, "Expected to be less than");

        // TopicMessageIdImpl by MessageIdImpl compare with TopicMessageIdImpl by BatchMessageIdImpl
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(topicMessageIdWithLargerLedgerIdByBatchMessageIdImpl) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(topicMessageIdWithLargerEntryIdByBatchMessageIdImpl) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                        .compareTo(topicMessageIdWithLargerPartitionIndexByBatchMessageIdImpl) < 0,
                "Expected to be less than");

        // TopicMessageIdImpl by MessageIdImpl compare with MessageIdImpl
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(messageIdImplWithLargerLedgerId) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl.
                compareTo(messageIdImplWithLargerEntryId) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(messageIdImplWithLargerPartitionIndex) < 0, "Expected to be less than");

        // TopicMessageIdImpl by MessageIdImpl compare with BatchMessageIdImpl
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(batchMessageIdWithLargerLedgerId) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                .compareTo(batchMessageIdWithLargerEntryId) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByMessageIdImpl
                        .compareTo(batchMessageIdWithLargerPartitionIndex) < 0,
                "Expected to be less than");


        // TopicMessageIdImpl by BatchMessageIdImpl compare with TopicMessageIdImpl by BatchMessageIdImpl
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(topicMessageIdWithLargerLedgerIdByBatchMessageIdImpl) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl.
                compareTo(topicMessageIdWithLargerEntryIdByBatchMessageIdImpl) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                        .compareTo(topicMessageIdWithLargerPartitionIndexByBatchMessageIdImpl) < 0,
                "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                        .compareTo(topicMessageIdWithLargerBatchIndexByBatchMessageIdImpl) < 0,
                "Expected to be less than");

        // TopicMessageIdImpl by BatchMessageIdImpl compare with TopicMessageIdImpl by MessageIdImpl
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(topicMessageIdWithLargerLedgerIdByMessageIdImpl) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl.
                compareTo(topicMessageIdWithLargerEntryIdByMessageIdImpl) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(topicMessageIdWithLargerPartitionIndexMessageIdImpl) < 0, "Expected to be less than");

        // TopicMessageIdImpl by BatchMessageIdImpl compare with BatchMessageIdImpl
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(batchMessageIdWithLargerLedgerId) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl.
                compareTo(batchMessageIdWithLargerEntryId) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                        .compareTo(batchMessageIdWithLargerPartitionIndex) < 0,
                "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                        .compareTo(batchMessageIdWithLargerBatchIndex) < 0,
                "Expected to be less than");

        // TopicMessageIdImpl by BatchMessageIdImpl compare with MessageIdImpl
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(messageIdImplWithLargerLedgerId) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl.
                compareTo(messageIdImplWithLargerEntryId) < 0, "Expected to be less than");
        assertTrue(normalTopicMessageIdImplByBatchMessageIdImpl
                .compareTo(messageIdImplWithLargerPartitionIndex) < 0, "Expected to be less than");
    }

    @Test
    public void testMessageIdImplAndBatchMessageIdImplWithSameLedgerIdAndEntryIdAndPartitionIndex() {
        try {
            normalMessageIdImpl.compareTo(normalBatchMessageId);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
            assertEquals(e.getMessage(), MESSAGE_ID_COMPARE_WITH_BATCH_MESSAGE_ID_THROW_EXCEPTION_MESSAGE);
        }

        try {
            normalMessageIdImpl.compareTo(normalTopicMessageIdImplByBatchMessageIdImpl);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
            assertEquals(e.getMessage(), MESSAGE_ID_COMPARE_WITH_BATCH_MESSAGE_ID_THROW_EXCEPTION_MESSAGE);
        }

        try {
            normalBatchMessageId.compareTo(normalMessageIdImpl);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
            assertEquals(e.getMessage(), BATCH_MESSAGE_ID_COMPARE_WITH_MESSAGE_ID_THROW_EXCEPTION_MESSAGE);
        }

        try {
            normalBatchMessageId.compareTo(normalTopicMessageIdImplByMessageIdImpl);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
            assertEquals(e.getMessage(), BATCH_MESSAGE_ID_COMPARE_WITH_MESSAGE_ID_THROW_EXCEPTION_MESSAGE);
        }

        try {
            normalTopicMessageIdImplByMessageIdImpl.compareTo(normalTopicMessageIdImplByBatchMessageIdImpl);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
            assertEquals(e.getMessage(), MESSAGE_ID_COMPARE_WITH_BATCH_MESSAGE_ID_THROW_EXCEPTION_MESSAGE);
        }

        try {
            normalTopicMessageIdImplByMessageIdImpl.compareTo(normalBatchMessageId);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
            assertEquals(e.getMessage(), MESSAGE_ID_COMPARE_WITH_BATCH_MESSAGE_ID_THROW_EXCEPTION_MESSAGE);
        }

        try {
            normalTopicMessageIdImplByBatchMessageIdImpl.compareTo(normalTopicMessageIdImplByMessageIdImpl);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
            assertEquals(e.getMessage(), BATCH_MESSAGE_ID_COMPARE_WITH_MESSAGE_ID_THROW_EXCEPTION_MESSAGE);
        }

        try {
            normalTopicMessageIdImplByBatchMessageIdImpl.compareTo(normalMessageIdImpl);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
            assertEquals(e.getMessage(), BATCH_MESSAGE_ID_COMPARE_WITH_MESSAGE_ID_THROW_EXCEPTION_MESSAGE);
        }

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
        Map<String, MessageId> map1 = new HashMap<>();
        Map<String, MessageId> map2 = new HashMap<>();
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
        Map<String, MessageId> map1 = new HashMap<>();
        Map<String, MessageId> map2 = new HashMap<>();
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

        Map<String, MessageId> map3 = new HashMap<>();
        map3.put(topic1, messageIdImpl3);
        map3.put(topic2, messageIdImpl3);
        MultiMessageIdImpl item8 = new MultiMessageIdImpl(map3);
        assertTrue(item8.compareTo(item5) > 0);
        assertTrue(item8.compareTo(item7) > 0);

        assertTrue(item5.compareTo(item8) < 0);
        assertTrue(item7.compareTo(item8) < 0);
    }
}

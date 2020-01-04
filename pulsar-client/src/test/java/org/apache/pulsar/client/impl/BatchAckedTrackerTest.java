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

import org.apache.pulsar.client.api.MessageId;
import org.testng.annotations.Test;

public class BatchAckedTrackerTest  {

    @Test
    public void testDeliveredAndAcked() throws Exception {
        BatchAckedTracker tracker = new BatchAckedTracker();

        MessageId nonBatchMessageId = new MessageIdImpl(1, 1, 1);

        assertTrue(tracker.deliver(nonBatchMessageId));
        assertEquals(tracker.ackedBatches.size(), 0);

        int batchSize = 3;
        BatchMessageAcker acker = BatchMessageAcker.newAcker(batchSize);
        BatchMessageIdImpl batchMessageId = new BatchMessageIdImpl(1, 1, -1, 2, acker);
        assertEquals(batchMessageId.getBatchIndex(), 2);
        assertEquals(batchMessageId.getBatchSize(), batchSize);

        // ensure message can be delivered to clients
        assertTrue(tracker.deliver(batchMessageId));
        assertEquals(tracker.ackedBatches.size(), 0);

        // ensure the first message ack will be tracked
        assertFalse(tracker.ack(batchMessageId));
        assertEquals(tracker.ackedBatches.size(), 1);

        // redeliver an acked message will return false
        assertFalse(tracker.deliver(batchMessageId));

        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 1, -1, 1, acker)));
        assertEquals(tracker.ackedBatches.size(), 1);

        // all message are acked in a batch of three messages
        assertTrue(tracker.ack(new BatchMessageIdImpl(1, 1, -1, 0, acker)));
        assertEquals(tracker.ackedBatches.size(), 0);

    }

    @Test
    public void testMultipleBatchDeliveredAndAcked() throws Exception {
        BatchAckedTracker tracker = new BatchAckedTracker();

        int batch1Size = 3;
        int batch2Size = 5;
        int batch3Size = 8;

        BatchMessageAcker acker1 = BatchMessageAcker.newAcker(batch1Size);
        BatchMessageAcker acker2 = BatchMessageAcker.newAcker(batch2Size);
        BatchMessageAcker acker3 = BatchMessageAcker.newAcker(batch3Size);

        BatchMessageIdImpl batch1MessageId1 = new BatchMessageIdImpl(1, 1, -1, 0, acker1);
        BatchMessageIdImpl batch1MessageId2 = new BatchMessageIdImpl(1, 1, -1, 1, acker1);

        // partial first batch acked
        assertTrue(tracker.deliver(batch1MessageId1));
        assertFalse(tracker.ack(batch1MessageId1));
        assertTrue(tracker.deliver(batch1MessageId2));
        assertFalse(tracker.ack(batch1MessageId2));
        assertEquals(tracker.ackedBatches.size(), 1);

        // ensure the first message ack will be tracked
        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 2, -1, 0, acker2)));
        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 2, -1, 1, acker2)));
        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 2, -1, 2, acker2)));
        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 2, -1, 3, acker2)));
        assertEquals(tracker.ackedBatches.size(), 2);
        assertTrue(tracker.deliver(new BatchMessageIdImpl(1, 2, -1, 4, acker2)));

        // redeliver an acked message will return false
        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 3, -1, 0, acker3)));
        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 3, -1, 1, acker3)));
        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 3, -1, 2, acker3)));
        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 3, -1, 3, acker3)));
        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 3, -1, 4, acker3)));
        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 3, -1, 5, acker3)));
        assertEquals(tracker.ackedBatches.size(), 3);
        assertTrue(tracker.deliver(new BatchMessageIdImpl(1, 3, -1, 7, acker3)));

        // all message are acked in a batch of three messages
        assertTrue(tracker.ack(new BatchMessageIdImpl(1, 1, -1, 2, acker1)));
        assertEquals(tracker.ackedBatches.size(), 2);

        assertTrue(tracker.ack(new BatchMessageIdImpl(1, 2, -1, 4, acker2)));
        assertEquals(tracker.ackedBatches.size(), 1);

        assertFalse(tracker.ack(new BatchMessageIdImpl(1, 3, -1, 6, acker3)));
        assertTrue(tracker.ack(new BatchMessageIdImpl(1, 3, -1, 7, acker3)));
        assertEquals(tracker.ackedBatches.size(), 0);
    }

}

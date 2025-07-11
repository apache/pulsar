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
package org.apache.pulsar.io.kinesis;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.pulsar.io.core.SourceContext;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class KinesisRecordProcessorTest {

    private KinesisSourceConfig config;
    private SourceContext sourceContext;
    private LinkedBlockingQueue<KinesisRecord> queue;
    private KinesisRecordProcessor recordProcessor;
    private RecordProcessorCheckpointer checkpointer;

    @BeforeMethod
    public void setup() {
        config = Mockito.mock(KinesisSourceConfig.class);
        sourceContext = Mockito.mock(SourceContext.class);
        queue = new LinkedBlockingQueue<>();
        checkpointer = Mockito.mock(RecordProcessorCheckpointer.class);

        // Configure the mock config for the processor
        when(config.getCheckpointInterval()).thenReturn(100L);
        when(config.getNumRetries()).thenReturn(1);
        when(config.getBackoffTime()).thenReturn(10L);
        when(config.getPropertiesToInclude()).thenReturn(Collections.emptySet());

        recordProcessor = new KinesisRecordProcessor(queue, config, sourceContext);
    }

    @Test
    public void testCheckpointAfterAck() throws Exception {
        // --- Setup: Prepare mock inputs ---
        String seqNum1 = "seq-1";
        String seqNum2 = "seq-2";
        KinesisClientRecord kcr1 = createMockKinesisRecord(seqNum1);
        KinesisClientRecord kcr2 = createMockKinesisRecord(seqNum2);
        ProcessRecordsInput processRecordsInput = createMockProcessRecordsInput(kcr1, kcr2);

        // --- Action 1: Process records ---
        recordProcessor.processRecords(processRecordsInput);

        // --- Assert 1: Records are in the queue ---
        assertEquals(queue.size(), 2);

        // --- Action 2: Simulate source reading and acking the first record ---
        KinesisRecord recordFromQueue1 = queue.take();
        recordFromQueue1.ack(); // This updates sequenceNumberToCheckpoint in the processor

        // --- Action 3: Advance time and trigger checkpoint logic ---
        Thread.sleep(config.getCheckpointInterval() + 50);
        recordProcessor.processRecords(createMockProcessRecordsInput()); // Empty input to trigger checkpoint

        // --- Assert 3: Verify checkpoint was called with the correct sequence number ---
        Mockito.verify(checkpointer, Mockito.times(1)).checkpoint(seqNum1);

        // --- Action 4: Ack the second record ---
        KinesisRecord recordFromQueue2 = queue.take();
        recordFromQueue2.ack();

        // --- Action 5: Trigger checkpoint again ---
        Thread.sleep(config.getCheckpointInterval() + 50);
        recordProcessor.processRecords(createMockProcessRecordsInput());

        // --- Assert 5: Verify checkpoint was called with the new, correct sequence number ---
        Mockito.verify(checkpointer, Mockito.times(1)).checkpoint(seqNum2);
    }

    @Test
    public void testNoCheckpointWithoutAck() throws Exception {
        // --- Setup ---
        String seqNum1 = "seq-1";
        KinesisClientRecord kcr1 = createMockKinesisRecord(seqNum1);
        ProcessRecordsInput processRecordsInput = createMockProcessRecordsInput(kcr1);

        // --- Action 1: Process a record ---
        recordProcessor.processRecords(processRecordsInput);
        assertEquals(queue.size(), 1);
        queue.take(); // Simulate reading but not acking

        // --- Action 2: Advance time and trigger checkpoint logic ---
        Thread.sleep(config.getCheckpointInterval() + 50);
        recordProcessor.processRecords(processRecordsInput);

        // --- Assert 2: Verify checkpoint was NEVER called because no record was acked ---
        Mockito.verify(checkpointer, Mockito.never()).checkpoint(Mockito.anyString());
    }

    @Test
    public void testFailTriggersFatal() throws Exception {
        KinesisClientRecord kcr1 = createMockKinesisRecord("seq-fail");
        ProcessRecordsInput processRecordsInput = createMockProcessRecordsInput(kcr1);

        recordProcessor.processRecords(processRecordsInput);
        KinesisRecord recordToFail = queue.take();
        recordToFail.fail();

        Mockito.verify(sourceContext, Mockito.times(1)).fatal(Mockito.any(Exception.class));
    }

    private KinesisClientRecord createMockKinesisRecord(String sequenceNumber) {
        KinesisClientRecord mockRecord = Mockito.mock(KinesisClientRecord.class);
        when(mockRecord.partitionKey()).thenReturn("test-key");
        when(mockRecord.sequenceNumber()).thenReturn(sequenceNumber);
        when(mockRecord.approximateArrivalTimestamp()).thenReturn(Instant.now());
        when(mockRecord.encryptionType()).thenReturn(EncryptionType.NONE);
        when(mockRecord.data()).thenReturn(ByteBuffer.wrap("data".getBytes()));
        return mockRecord;
    }

    private ProcessRecordsInput createMockProcessRecordsInput(KinesisClientRecord... records) {
        ProcessRecordsInput input = Mockito.mock(ProcessRecordsInput.class);
        when(input.records()).thenReturn(Arrays.asList(records));
        when(input.checkpointer()).thenReturn(checkpointer);
        return input;
    }
}
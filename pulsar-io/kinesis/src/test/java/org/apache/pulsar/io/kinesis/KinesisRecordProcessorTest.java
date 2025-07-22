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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.io.core.SourceContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class KinesisRecordProcessorTest {

    private KinesisSourceConfig config;
    private SourceContext sourceContext;
    private LinkedBlockingQueue<KinesisRecord> queue;
    private KinesisRecordProcessor recordProcessor;
    private RecordProcessorCheckpointer checkpointer;
    private ScheduledExecutorService checkpointExecutor;
    private ArgumentCaptor<Runnable> scheduledTaskCaptor;

    @BeforeMethod
    public void setup() {
        config = Mockito.mock(KinesisSourceConfig.class);
        sourceContext = Mockito.mock(SourceContext.class);
        queue = new LinkedBlockingQueue<>();
        checkpointer = Mockito.mock(RecordProcessorCheckpointer.class);
        checkpointExecutor = Mockito.mock(ScheduledExecutorService.class);
        scheduledTaskCaptor = ArgumentCaptor.forClass(Runnable.class);

        when(config.getCheckpointInterval()).thenReturn(60000L);
        when(config.getNumRetries()).thenReturn(1);
        when(config.getBackoffTime()).thenReturn(100L);
        when(config.getPropertiesToInclude()).thenReturn(Collections.emptySet());

        recordProcessor = new KinesisRecordProcessor(queue, config, sourceContext, checkpointExecutor);
    }

    @Test
    public void testScheduledCheckpointAfterAck() throws Exception {
        // Arrange: Initialize the processor, which schedules the first checkpoint task.
        recordProcessor.initialize(createMockInitializationInput());
        verify(checkpointExecutor).schedule(scheduledTaskCaptor.capture(), anyLong(), any(TimeUnit.class));
        Runnable scheduledCheckpointTask = scheduledTaskCaptor.getValue();

        // Act: Process a record and ack it.
        ProcessRecordsInput processRecordsInput = createMockProcessRecordsInput(
                createMockKinesisRecord("seq-1", 0L));
        recordProcessor.processRecords(processRecordsInput);
        KinesisRecord recordFromQueue = queue.take();
        recordFromQueue.ack();

        // Simulate the scheduler firing the checkpoint task.
        scheduledCheckpointTask.run();

        // Assert: Verify the checkpoint was called with the correct sequence and sub-sequence numbers.
        verify(checkpointer, times(1)).checkpoint("seq-1", 0L);
    }

    @Test
    public void testCheckpointLogicOnlyAdvancesForward() throws Exception {
        // Arrange
        recordProcessor.initialize(createMockInitializationInput());
        verify(checkpointExecutor, Mockito.times(1))
                .schedule(scheduledTaskCaptor.capture(), anyLong(), any(TimeUnit.class));
        Runnable scheduledCheckpointTask = scheduledTaskCaptor.getValue();
        ProcessRecordsInput processRecordsInput = createMockProcessRecordsInput(checkpointer);
        recordProcessor.processRecords(processRecordsInput);

        // Act & Assert 1: Ack a later sub-sequence number first.
        recordProcessor.updateSequenceNumberToCheckpoint("seq-1", 5L);
        scheduledCheckpointTask.run();
        verify(checkpointer, times(1)).checkpoint("seq-1", 5L);

        // Act & Assert 2: Ack an earlier (out-of-order) sub-sequence number.
        // The checkpointToCommit is now older than lastSuccessfullyCheckpointed.
        recordProcessor.updateSequenceNumberToCheckpoint("seq-1", 3L);
        scheduledCheckpointTask.run();
        // Verify checkpoint was NOT called again, because the position is not new.
        verify(checkpointer, times(1)).checkpoint("seq-1", 5L);

        // Act & Assert 3: Ack a new, even later sub-sequence number.
        recordProcessor.updateSequenceNumberToCheckpoint("seq-1", 7L);
        scheduledCheckpointTask.run();
        // Verify checkpoint is now called with the new, advanced position.
        verify(checkpointer, times(1)).checkpoint("seq-1", 7L);
    }

    @Test(timeOut = 3000)
    public void testShardEndedTimeoutPathPerformsBestEffortCheckpoint() throws Exception {
        // Arrange: Process two records, but only ack one.
        recordProcessor.processRecords(createMockProcessRecordsInput(
                createMockKinesisRecord("seq-A", 0L),
                createMockKinesisRecord("seq-B", 1L)
        ));
        queue.take().ack(); // Ack "seq-A", leaves "seq-B" in-flight
        // Do not take/ack the second record.

        RecordProcessorCheckpointer shardEndCheckpointer = Mockito.mock(RecordProcessorCheckpointer.class);
        ShardEndedInput shardEndedInput = createMockShardEndedInput(shardEndCheckpointer);

        // Act: Call shardEnded. This should block for 10 seconds then time out.
        recordProcessor.shardEnded(shardEndedInput);

        // Assert: After timeout, a best-effort checkpoint should be made with the last ack'd sequence number.
        verify(shardEndCheckpointer, times(1)).checkpoint("seq-A", 0L);
        verify(shardEndCheckpointer, never()).checkpoint();
    }

    @Test(timeOut = 3000)
    public void testShutdownRequestedWaitsAndPerformsBestEffortCheckpoint() throws Exception {
        // Arrange: Process two records, only ack one.
        recordProcessor.processRecords(createMockProcessRecordsInput(
                createMockKinesisRecord("seq-shutdown-1", 0L),
                createMockKinesisRecord("seq-shutdown-2", 1L)
        ));
        queue.take().ack();

        RecordProcessorCheckpointer shutdownCheckpointer = Mockito.mock(RecordProcessorCheckpointer.class);
        ShutdownRequestedInput shutdownInput = createMockShutdownRequestedInput(shutdownCheckpointer);

        recordProcessor.shutdownRequested(shutdownInput);

        // Assert: A best-effort checkpoint is made with the last ack'd sequence number.
        verify(shutdownCheckpointer, times(1)).checkpoint("seq-shutdown-1", 0L);
    }

    private KinesisClientRecord createMockKinesisRecord(String sequenceNumber, long subSequenceNumber) {
        KinesisClientRecord mockRecord = Mockito.mock(KinesisClientRecord.class);
        when(mockRecord.partitionKey()).thenReturn("test-key");
        when(mockRecord.sequenceNumber()).thenReturn(sequenceNumber);
        when(mockRecord.subSequenceNumber()).thenReturn(subSequenceNumber);
        when(mockRecord.approximateArrivalTimestamp()).thenReturn(Instant.now());
        when(mockRecord.encryptionType()).thenReturn(EncryptionType.NONE);
        when(mockRecord.data()).thenReturn(ByteBuffer.wrap("data".getBytes()));
        return mockRecord;
    }

    private ProcessRecordsInput createMockProcessRecordsInput(KinesisClientRecord... records) {
        return createMockProcessRecordsInput(this.checkpointer, records);
    }

    private ProcessRecordsInput createMockProcessRecordsInput(RecordProcessorCheckpointer checkpointer,
                                                              KinesisClientRecord... records) {
        ProcessRecordsInput input = Mockito.mock(ProcessRecordsInput.class);
        when(input.records()).thenReturn(Arrays.asList(records));
        when(input.checkpointer()).thenReturn(checkpointer);
        return input;
    }

    private InitializationInput createMockInitializationInput() {
        return Mockito.mock(InitializationInput.class);
    }

    private ShardEndedInput createMockShardEndedInput(RecordProcessorCheckpointer checkpointer) {
        ShardEndedInput input = Mockito.mock(ShardEndedInput.class);
        when(input.checkpointer()).thenReturn(checkpointer);
        return input;
    }

    private ShutdownRequestedInput createMockShutdownRequestedInput(RecordProcessorCheckpointer checkpointer) {
        ShutdownRequestedInput input = Mockito.mock(ShutdownRequestedInput.class);
        when(input.checkpointer()).thenReturn(checkpointer);
        return input;
    }
}
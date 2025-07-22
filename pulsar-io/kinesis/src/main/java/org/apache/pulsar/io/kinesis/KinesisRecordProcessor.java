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

import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.io.core.SourceContext;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

@Slf4j
public class KinesisRecordProcessor implements ShardRecordProcessor {

    private record CheckpointSequenceNumber(String sequenceNumber, long subSequenceNumber) {}

    private final int numRetries;
    private final long checkpointInterval;
    private final long backoffTime;
    private final LinkedBlockingQueue<KinesisRecord> queue;
    private final SourceContext sourceContext;
    private final Set<String> propertiesToInclude;
    private final ScheduledExecutorService checkpointExecutor;
    private final AtomicReference<RecordProcessorCheckpointer> checkpointerRef = new AtomicReference<>();
    private final AtomicBoolean isCheckpointing = new AtomicBoolean(false);
    private String kinesisShardId;
    private final AtomicInteger numRecordsInFlight = new AtomicInteger(0);

    private volatile CheckpointSequenceNumber sequenceNumberNeedToCheckpoint = null;
    private volatile CheckpointSequenceNumber lastCheckpointSequenceNumber = null;

    public KinesisRecordProcessor(LinkedBlockingQueue<KinesisRecord> queue, KinesisSourceConfig config,
                                  SourceContext sourceContext, ScheduledExecutorService checkpointExecutor) {
        this.queue = queue;
        this.checkpointInterval = config.getCheckpointInterval();
        this.numRetries = config.getNumRetries();
        this.backoffTime = config.getBackoffTime();
        this.propertiesToInclude = config.getPropertiesToInclude();
        this.sourceContext = sourceContext;
        this.checkpointExecutor = checkpointExecutor;
    }

    private void tryCheckpointWithRetry(RecordProcessorCheckpointer checkpointer,
                                        CheckpointSequenceNumber checkpoint, int attempt) {
        try {
            log.info("Attempting checkpoint {}/{} for shard {} at {}. In-flight records: {}",
                    attempt, numRetries, kinesisShardId, checkpoint, numRecordsInFlight.get());
            checkpointer.checkpoint(checkpoint.sequenceNumber(), checkpoint.subSequenceNumber());
            lastCheckpointSequenceNumber = checkpoint;
            log.info("Successfully checkpointed shard {} at {}", kinesisShardId, checkpoint);
            isCheckpointing.set(false);
            checkpointExecutor.schedule(this::triggerCheckpoint, checkpointInterval, TimeUnit.MILLISECONDS);
        } catch (ThrottlingException | KinesisClientLibDependencyException e) {
            if (attempt >= numRetries) {
                log.error("Checkpoint for shard {} failed after {} attempts at {}. Terminating.",
                        kinesisShardId, numRetries, checkpoint, e);
                sourceContext.fatal(e);
            } else {
                log.warn("Throttling/Dependency error on checkpoint for shard {} at {}. Scheduling retry {} "
                                + "after {}ms.", kinesisShardId, checkpoint, attempt + 1, backoffTime);
                checkpointExecutor.schedule(() -> tryCheckpointWithRetry(checkpointer, checkpoint, attempt + 1),
                        backoffTime, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            log.error("Caught a non-retryable exception for shard {} during checkpoint at {}. Terminating.",
                    kinesisShardId, checkpoint, e);
            sourceContext.fatal(e);
        }
    }

    public void updateSequenceNumberToCheckpoint(String sequenceNumber, long subSequenceNumber) {
        CheckpointSequenceNumber newCheckpoint = new CheckpointSequenceNumber(sequenceNumber, subSequenceNumber);
        log.debug("{} Updating sequence number to checkpoint {}", kinesisShardId, newCheckpoint);
        this.sequenceNumberNeedToCheckpoint = newCheckpoint;
        this.numRecordsInFlight.decrementAndGet();
    }

    public void failed() {
        numRecordsInFlight.decrementAndGet();
        sourceContext.fatal(new PulsarClientException("Failed to process Kinesis records due send to pulsar topic"));
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        kinesisShardId = initializationInput.shardId();
        log.info("Initializing KinesisRecordProcessor for shard {}, extendedSequenceNumber: {}, pendingCheckSeq: {}",
                kinesisShardId, initializationInput.extendedSequenceNumber(),
                initializationInput.pendingCheckpointSequenceNumber());
        checkpointExecutor.schedule(this::triggerCheckpoint, checkpointInterval, TimeUnit.MILLISECONDS);
    }

    private void triggerCheckpoint() {
        try {
            if (isCheckpointing.compareAndSet(false, true)) {
                final RecordProcessorCheckpointer checkpointer = checkpointerRef.get();
                final CheckpointSequenceNumber currentCheckpoint = this.sequenceNumberNeedToCheckpoint;
                if (checkpointer != null && currentCheckpoint != null && !currentCheckpoint.equals(
                        lastCheckpointSequenceNumber)) {
                    tryCheckpointWithRetry(checkpointer, currentCheckpoint, 1);
                } else {
                    isCheckpointing.set(false);
                    checkpointExecutor.schedule(this::triggerCheckpoint, checkpointInterval, TimeUnit.MILLISECONDS);
                }
            }
        } catch (Throwable e) {
            log.error("Error while triggering checkpoint for shard {}. Terminating.", kinesisShardId, e);
            sourceContext.fatal(e);
        }
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        this.checkpointerRef.set(processRecordsInput.checkpointer());
        log.info("Processing {} records from {}", processRecordsInput.records().size(), kinesisShardId);
        long millisBehindLatest = processRecordsInput.millisBehindLatest();

        for (KinesisClientRecord record : processRecordsInput.records()) {
            log.debug("Add record with sequence number {}:{} to queue for shard {}.",
                    record.sequenceNumber(), record.subSequenceNumber(), kinesisShardId);
            try {
                queue.put(new KinesisRecord(record, this.kinesisShardId, millisBehindLatest,
                        propertiesToInclude, this));
            } catch (Exception e) {
                log.error("Unable to create and queue KinesisRecord for shard {}.", kinesisShardId, e);
                sourceContext.fatal(e);
            }
            numRecordsInFlight.incrementAndGet();
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.info("Lease lost for shard {}. Last checkpointed at {}.", kinesisShardId, lastCheckpointSequenceNumber);
    }

    private void finalizeAndCheckpoint(RecordProcessorCheckpointer checkpointer, boolean isShardEnd) {
        boolean processedInTime = false;
        log.info("Waiting up to {}s for {} in-flight records on shard {}.", numRetries,
                numRecordsInFlight.get(), kinesisShardId);
        try {
            for (int i = 0; i < numRetries; i++) {
                if (numRecordsInFlight.get() == 0) {
                    processedInTime = true;
                    break;
                }
                Thread.sleep(2000L);
            }
        } catch (Exception e) {
            log.warn("Error while waiting for in-flight records on shard {}.", kinesisShardId, e);
        }

        try {
            if (processedInTime && isShardEnd) {
                log.info("All records processed for shard {}. Performing SHARD_END checkpoint.", kinesisShardId);
                for (int i = 0; i < numRetries; i++) {
                    try {
                        checkpointer.checkpoint();
                        log.info("Successfully checkpointed shard {} at SHARD_END.", kinesisShardId);
                        return;
                    } catch (ThrottlingException | KinesisClientLibDependencyException ex) {
                        if (i >= numRetries - 1) {
                            throw ex;
                        }
                        Thread.sleep(backoffTime);
                    }
                }
            } else {
                log.warn("Not all records for shard {} were processed or not a shard end. "
                                + "Performing best-effort checkpoint.", kinesisShardId);
                final CheckpointSequenceNumber finalCheckpoint = this.sequenceNumberNeedToCheckpoint;
                if (finalCheckpoint != null) {
                    checkpointer.checkpoint(finalCheckpoint.sequenceNumber(), finalCheckpoint.subSequenceNumber());
                }
            }
        } catch (Exception e) {
            log.error("Failed to perform final checkpoint for shard {}. Data may be reprocessed.", kinesisShardId, e);
            sourceContext.fatal(e);
        }
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        log.info("Reached end of shard {}, starting final checkpoint process.", kinesisShardId);
        finalizeAndCheckpoint(shardEndedInput.checkpointer(), true);
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Shutdown requested for shard {}, starting final checkpoint process.", kinesisShardId);
        finalizeAndCheckpoint(shutdownRequestedInput.checkpointer(), false);
    }
}
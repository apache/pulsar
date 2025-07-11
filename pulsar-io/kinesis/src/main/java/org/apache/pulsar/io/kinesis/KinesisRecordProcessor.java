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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.io.core.SourceContext;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.ShutdownException;
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

    private final int numRetries;
    private final long checkpointInterval;
    private final long backoffTime;

    private final LinkedBlockingQueue<KinesisRecord> queue;
    private final SourceContext sourceContext;
    private final Set<String> propertiesToInclude;

    private long nextCheckpointTimeInNanos;
    private String kinesisShardId;
    private volatile String sequenceNumberToCheckpoint = null;
    private String lastCheckpointedSequenceNumber = null;

    public KinesisRecordProcessor(LinkedBlockingQueue<KinesisRecord> queue, KinesisSourceConfig config,
                                  SourceContext sourceContext) {
        this.queue = queue;
        this.checkpointInterval = config.getCheckpointInterval();
        this.numRetries = config.getNumRetries();
        this.backoffTime = config.getBackoffTime();
        this.propertiesToInclude = config.getPropertiesToInclude();
        this.sourceContext = sourceContext;
    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer, String sequenceNumber) {
        log.info("Checkpointing shard {} at sequence number {}", kinesisShardId, sequenceNumber);
        for (int i = 0; i < numRetries; i++) {
            try {
                checkpointer.checkpoint(sequenceNumber);
                lastCheckpointedSequenceNumber = sequenceNumber;
                break;
            } catch (ShutdownException se) {
                log.info("Caught shutdown exception, skipping checkpoint.", se);
                sourceContext.fatal(se);
                break;
            } catch (InvalidStateException e) {
                log.error("Cannot save checkpoint to the DynamoDB table.", e);
                sourceContext.fatal(e);
                break;
            } catch (ThrottlingException | KinesisClientLibDependencyException e) {
                if (i >= (numRetries - 1)) {
                    log.error("Checkpoint failed after {} attempts.", (i + 1), e);
                    sourceContext.fatal(e);
                    break;
                }
            }

            try {
                Thread.sleep(backoffTime);
            } catch (InterruptedException e) {
                log.debug("Interrupted sleep", e);
            }
        }
    }

    public void updateSequenceNumberToCheckpoint(String sequenceNumber) {
        this.sequenceNumberToCheckpoint = sequenceNumber;
    }

    public void failed() {
        sourceContext.fatal(new PulsarClientException("Failed to process Kinesis records due send to pulsar topic"));
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        kinesisShardId = initializationInput.shardId();
        log.info("Initializing KinesisRecordProcessor for shard {}. Config: checkpointInterval={}ms, numRetries={}, "
                        + "backoffTime={}ms, propertiesToInclude={}",
                kinesisShardId, checkpointInterval, numRetries, backoffTime, propertiesToInclude);
        nextCheckpointTimeInNanos = System.nanoTime() + checkpointInterval;
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        log.info("Processing {} records from {}", processRecordsInput.records().size(), kinesisShardId);
        long millisBehindLatest = processRecordsInput.millisBehindLatest();

        for (KinesisClientRecord record : processRecordsInput.records()) {
            try {
                queue.put(new KinesisRecord(record, this.kinesisShardId, millisBehindLatest,
                        propertiesToInclude, this));
            } catch (InterruptedException e) {
                log.warn("unable to create KinesisRecord ", e);
            }
        }

        // Checkpoint once every checkpoint interval.
        if (System.nanoTime() > nextCheckpointTimeInNanos) {
            if (sequenceNumberToCheckpoint != null
                    && !sequenceNumberToCheckpoint.equals(lastCheckpointedSequenceNumber)) {
                checkpoint(processRecordsInput.checkpointer(), sequenceNumberToCheckpoint);
            }
            nextCheckpointTimeInNanos = System.nanoTime() + checkpointInterval;
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.info("Lease lost for shard {} lastCheckPointedSequenceNumber {}, will terminate soon.",
                kinesisShardId, lastCheckpointedSequenceNumber);
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        log.info("Reached end of shard {}, will checkpoint.", kinesisShardId);
        if (sequenceNumberToCheckpoint != null) {
            checkpoint(shardEndedInput.checkpointer(), sequenceNumberToCheckpoint);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Shutdown requested for record processor on shard {}, will checkpoint.", kinesisShardId);
        if (sequenceNumberToCheckpoint != null) {
            checkpoint(shutdownRequestedInput.checkpointer(), sequenceNumberToCheckpoint);
        }
    }
}
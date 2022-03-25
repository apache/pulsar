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
package org.apache.pulsar.io.kinesis;

import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
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
    private long nextCheckpointTimeInNanos;
    private String kinesisShardId;
    public KinesisRecordProcessor(LinkedBlockingQueue<KinesisRecord> queue, KinesisSourceConfig config) {
        this.queue = queue;
        this.checkpointInterval = config.getCheckpointInterval();
        this.numRetries = config.getNumRetries();
        this.backoffTime = config.getBackoffTime();
    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        log.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < numRetries; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown.
                log.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (InvalidStateException e) {
                log.error("Cannot save checkpoint to the DynamoDB table.", e);
                break;
            } catch (ThrottlingException | KinesisClientLibDependencyException e) {
                // Back off and re-attempt checkpoint upon transient failures
                if (i >= (numRetries - 1)) {
                    log.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
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

    @Override
    public void initialize(InitializationInput initializationInput) {
        kinesisShardId = initializationInput.shardId();
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {

        log.info("Processing " + processRecordsInput.records().size() + " records from " + kinesisShardId);

        for (KinesisClientRecord record : processRecordsInput.records()) {
            try {
                queue.put(new KinesisRecord(record));
            } catch (InterruptedException e) {
                log.warn("unable to create KinesisRecord ", e);
            }
        }

        // Checkpoint once every checkpoint interval.
        if (System.nanoTime() > nextCheckpointTimeInNanos) {
            checkpoint(processRecordsInput.checkpointer());
            nextCheckpointTimeInNanos = System.nanoTime() + checkpointInterval;
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.info("lease lost, will terminate soon");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        log.info("reached end of shard, will checkpoint");
        checkpoint(shardEndedInput.checkpointer());
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Shutting down record processor for shard: " + kinesisShardId);
        checkpoint(shutdownRequestedInput.checkpointer());
    }
}

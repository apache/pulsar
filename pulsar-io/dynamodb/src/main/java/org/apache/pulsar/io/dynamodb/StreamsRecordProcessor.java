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
package org.apache.pulsar.io.dynamodb;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
// This is a direct adaptation of the kinesis record processor for kcl v1; no dynamo-specific logic
public class StreamsRecordProcessor implements IRecordProcessor {

    private final int numRetries;
    private final long checkpointInterval;
    private final long backoffTime;

    private final LinkedBlockingQueue<StreamsRecord> queue;
    private long nextCheckpointTimeInNanos;
    private String kinesisShardId;
    public StreamsRecordProcessor(LinkedBlockingQueue<StreamsRecord> queue, DynamoDBSourceConfig config) {
        this.queue = queue;
        this.checkpointInterval = config.getCheckpointInterval();
        this.numRetries = config.getNumRetries();
        this.backoffTime = config.getBackoffTime();
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
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
        kinesisShardId = initializationInput.getShardId();
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {

        log.debug("Processing {} records from {}", processRecordsInput.getRecords().size(), kinesisShardId);

        for (Record record : processRecordsInput.getRecords()) {
            try {
                queue.put(new StreamsRecord(record));
            } catch (InterruptedException e) {
                log.warn("unable to create KinesisRecord ", e);
            }
        }

        // Checkpoint once every checkpoint interval.
        if (System.nanoTime() > nextCheckpointTimeInNanos) {
            checkpoint(processRecordsInput.getCheckpointer());
            nextCheckpointTimeInNanos = System.nanoTime() + checkpointInterval;
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        log.info("Shutting down record processor for shard: {}", kinesisShardId);
        checkpoint(shutdownInput.getCheckpointer());
    }
}

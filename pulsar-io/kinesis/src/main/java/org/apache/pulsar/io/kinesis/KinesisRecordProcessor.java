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

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KinesisRecordProcessor implements IRecordProcessor {
    
    private final int numRetries;
    private final long checkpointInterval;
    private final long backoffTime;
    
    private final LinkedBlockingQueue<KinesisRecord> queue;
    private long nextCheckpointTimeInNanos;
    private String kinesisShardId;
    
    public KinesisRecordProcessor(LinkedBlockingQueue<KinesisRecord> queue, KinesisSourceConfig config) {
        this.queue = queue;
        this.backoffTime = config.getBackoffTime();
        this.checkpointInterval = config.getCheckpointInterval();
        this.numRetries = config.getNumRetries();
    }

    @Override
    public void initialize(String shardId) {
        kinesisShardId = shardId;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        log.info("Processing " + records.size() + " records from " + kinesisShardId);
        
        for (Record record : records) {
           try {
               queue.put(new KinesisRecord(record));
           } catch (InterruptedException e) {
               log.warn("unable to create KinesisRecord ", e);
           }
        }

       // Checkpoint once every checkpoint interval.
        if (System.nanoTime() > nextCheckpointTimeInNanos) {
            checkpoint(checkpointer);
            nextCheckpointTimeInNanos = System.nanoTime() + checkpointInterval;
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        log.info("Shutting down record processor for shard: " + kinesisShardId);
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
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
            } catch (ThrottlingException e) {
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

}

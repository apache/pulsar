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
package org.apache.pulsar.testclient;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.CheckpointConsumerBuilder;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.schema.Schema;
import picocli.CommandLine.Command;

/**
 * A client program to test pulsar reader performance with the V5 client API.
 *
 * <p>V5 has no {@code Reader}; the closest equivalent is the {@code CheckpointConsumer}, which is
 * what this command measures. Everything that is not V5-specific lives in
 * {@link PerformanceReaderBase}; the v4 {@code Reader} is driven by {@link PerformanceReaderV4}
 * under the {@code read-v4} name.
 */
@Command(name = "read", description = "Test pulsar reader performance.")
public class PerformanceReader
        extends PerformanceReaderBase<PulsarClient, CheckpointConsumer<byte[]>, Message<byte[]>> {

    private ExecutorService readerExec;

    public PerformanceReader() {
        super("read");
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        // V5 CheckpointConsumer accepts earliest / latest / a serialized Checkpoint byte array.
        // It does not expose the v4 "lid:eid" specific MessageId form, so reject it explicitly.
        if (!"earliest".equals(startMessageId) && !"latest".equals(startMessageId)) {
            throw new Exception(String.format("invalid start message ID '%s'. V5 CheckpointConsumer "
                    + "only accepts 'earliest' or 'latest'; the v4 'lid:eid' form is not supported. "
                    + "Use read-v4 for the v4 reader, which does support it.",
                    startMessageId));
        }
    }

    @Override
    protected void prepareRun() {
        if (this.useTls) {
            log.info("--use-tls has no effect on V5 (TLS is enabled automatically when the service URL "
                    + "uses pulsar+ssl:// — pass that scheme via --service-url instead).");
        }
        if (this.receiverQueueSize != 1000) {
            log.info("--receiver-queue-size has no effect on V5 CheckpointConsumer.");
        }
    }

    @Override
    protected PulsarClient createClient() throws PulsarClientException {
        return PerfClientUtils.createV5ClientBuilderFromArguments(this).build();
    }

    @Override
    protected void closeClient(PulsarClient client) {
        PerfClientUtils.closeClient(client);
    }

    @Override
    protected CompletableFuture<CheckpointConsumer<byte[]>> createReaderAsync(PulsarClient client, String topic) {
        Checkpoint startPosition = "earliest".equals(this.startMessageId)
                ? Checkpoint.earliest()
                : Checkpoint.latest();
        CheckpointConsumerBuilder<byte[]> b = client.newCheckpointConsumer(Schema.bytes())
                .topic(topic)
                .startPosition(startPosition);
        return b.createAsync();
    }

    @Override
    protected int messageSize(Message<byte[]> msg) {
        return msg.value().length;
    }

    @Override
    protected long publishTimeMillis(Message<byte[]> msg) {
        return msg.publishTime().toEpochMilli();
    }

    /**
     * V5 has no ReaderListener — drive each consumer from a dedicated poll thread that calls
     * receive(timeout) and runs the same per-message handler the v4 listener does.
     */
    @Override
    protected void startReading(List<CheckpointConsumer<byte[]>> readers) {
        readerExec = Executors.newCachedThreadPool(
                new DefaultThreadFactory("pulsar-perf-reader-poll"));
        for (CheckpointConsumer<byte[]> consumer : readers) {
            readerExec.submit(() -> readLoop(consumer));
        }
    }

    @Override
    protected void stopReading() {
        if (readerExec == null) {
            return;
        }
        readerExec.shutdownNow();
        try {
            if (!readerExec.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Reader poll executor did not terminate within timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void readLoop(CheckpointConsumer<byte[]> consumer) {
        while (!Thread.currentThread().isInterrupted()) {
            Message<byte[]> msg;
            try {
                msg = consumer.receive(Duration.ofSeconds(1));
            } catch (Exception e) {
                if (PerfClientUtils.hasInterruptedException(e)) {
                    Thread.currentThread().interrupt();
                    return;
                }
                log.warn().exception(e).log("receive failed; retrying");
                continue;
            }
            if (msg == null) {
                continue;
            }
            if (handleMessage(msg)) {
                return;
            }
        }
    }
}

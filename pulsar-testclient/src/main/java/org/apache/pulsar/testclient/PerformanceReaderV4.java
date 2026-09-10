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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.impl.MessageIdImpl;
import picocli.CommandLine.Command;

/**
 * The {@code read} benchmark driven by the v4 ({@code pulsar-client-original}) client.
 *
 * <p>This is the counterpart of {@link PerformanceReader}, which measures the V5
 * {@code CheckpointConsumer} — a different broker-side entity — so without this command nothing in
 * {@code pulsar-perf} exercises the v4 {@code Reader} at all. It also keeps the v4-only reader
 * behaviour working: a {@code lid:eid} start message id, {@code --receiver-queue-size},
 * {@code --use-tls}, and {@code ReaderListener} dispatch on the client's listener threads.
 */
@Command(name = "read-v4", description = "Test pulsar reader performance using the v4 client.")
public class PerformanceReaderV4
        extends PerformanceReaderBase<PulsarClient, Reader<byte[]>, Message<byte[]>> {

    private ReaderListener<byte[]> listener;

    public PerformanceReaderV4() {
        super("read-v4");
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        if (!"earliest".equals(startMessageId) && !"latest".equals(startMessageId)
                && (startMessageId.split(":")).length != 2) {
            String errMsg = String.format("invalid start message ID '%s', must be either 'earliest', "
                    + "'latest' or a specific message id by using 'lid:eid'", startMessageId);
            throw new Exception(errMsg);
        }
    }

    @Override
    protected void prepareRun() {
        this.listener = (reader, msg) -> handleMessage(msg);
    }

    @Override
    @SuppressWarnings("deprecation")
    protected PulsarClient createClient() throws PulsarClientException {
        ClientBuilder clientBuilder = PerfClientUtils.createClientBuilderFromArguments(this)
                .enableTls(this.useTls);
        return clientBuilder.build();
    }

    @Override
    protected void closeClient(PulsarClient client) {
        PerfClientUtils.closeClient(client);
    }

    @Override
    protected CompletableFuture<Reader<byte[]>> createReaderAsync(PulsarClient client, String topic) {
        ReaderBuilder<byte[]> readerBuilder = client.newReader()
                .readerListener(this.listener)
                .receiverQueueSize(this.receiverQueueSize)
                .startMessageId(parseStartMessageId())
                .topic(topic);
        return readerBuilder.createAsync();
    }

    private MessageId parseStartMessageId() {
        if ("earliest".equals(this.startMessageId)) {
            return MessageId.earliest;
        }
        if ("latest".equals(this.startMessageId)) {
            return MessageId.latest;
        }
        String[] parts = this.startMessageId.split(":");
        return new MessageIdImpl(Long.parseLong(parts[0]), Long.parseLong(parts[1]), -1);
    }

    @Override
    protected int messageSize(Message<byte[]> msg) {
        return msg.getData().length;
    }

    @Override
    protected long publishTimeMillis(Message<byte[]> msg) {
        return msg.getPublishTime();
    }
}

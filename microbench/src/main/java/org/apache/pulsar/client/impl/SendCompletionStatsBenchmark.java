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
package org.apache.pulsar.client.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.metrics.LatencyHistogram;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** Measures actual OpSendMsg completion for custom callbacks; guards the snapshot-preserving fallback. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class SendCompletionStatsBenchmark {
    @Param({"false", "true"})
    public boolean retainStats;

    private MessageImpl<byte[]> message;
    private ProducerImpl.OpSendMsg operation;
    private StatsCallback callback;

    @Setup
    public void setup() {
        message = MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap(new byte[128]), Schema.BYTES, "benchmark");
        callback = new StatsCallback();
        operation = ProducerImpl.OpSendMsg.create(LatencyHistogram.NOOP, message, null, 123, callback);
        operation.updateSentTimestamp();
    }

    @Benchmark
    public long complete() {
        operation.sendComplete(null);
        return callback.observed;
    }

    @TearDown
    public void tearDown() {
        operation.recycle();
        message.getDataBuffer().release();
        message.recycle();
    }

    private class StatsCallback implements SendCallback {
        private long observed;
        private OpSendMsgStats retained;

        @Override
        public void sendComplete(Throwable error, OpSendMsgStats stats) {
            observed = stats.getSequenceId() + stats.getUncompressedSize() + stats.getRetryCount();
            if (retainStats) {
                retained = stats;
            }
        }

        @Override
        public void addCallback(MessageImpl<?> message, SendCallback callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SendCallback getNextSendCallback() {
            return null;
        }

        @Override
        public MessageImpl<?> getNextMessage() {
            return null;
        }

        @Override
        public CompletableFuture<MessageId> getFuture() {
            return null;
        }
    }
}

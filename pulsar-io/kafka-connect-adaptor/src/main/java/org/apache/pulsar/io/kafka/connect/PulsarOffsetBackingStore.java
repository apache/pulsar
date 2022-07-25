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
package org.apache.pulsar.io.kafka.connect;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.pulsar.io.common.IOConfigUtils.loadConfigFromJsonString;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

/**
 * Implementation of {@link OffsetBackingStore} that uses a Pulsar topic to store offset data.
 */
@Slf4j
public class PulsarOffsetBackingStore implements OffsetBackingStore {

    private final Map<ByteBuffer, ByteBuffer> data = new ConcurrentHashMap<>();
    private PulsarClient client;
    private String topic;
    private Map<String, Object> readerConfigMap = new HashMap<>();
    private Producer<byte[]> producer;
    private Reader<byte[]> reader;
    private volatile CompletableFuture<Void> outstandingReadToEnd = null;

    public PulsarOffsetBackingStore(PulsarClient client) {
        checkArgument(client != null, "Pulsar Client must be provided");
        this.client = client;
    }

    @Override
    public void configure(WorkerConfig workerConfig) {
        this.topic = workerConfig.getString(PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG);
        checkArgument(!isBlank(topic), "Offset storage topic must be specified");
        try {
            this.readerConfigMap = loadConfigFromJsonString(
                    workerConfig.getString(PulsarKafkaWorkerConfig.OFFSET_STORAGE_READER_CONFIG));
        } catch (JsonProcessingException exception) {
            log.warn("The provided reader configs are invalid, "
                    + "will not passing any extra config to the reader builder.", exception);
        }

        log.info("Configure offset backing store on pulsar topic {}", topic);
    }

    void readToEnd(CompletableFuture<Void> future) {
        synchronized (this) {
            if (outstandingReadToEnd != null) {
                outstandingReadToEnd.whenComplete((result, cause) -> {
                    if (null != cause) {
                        future.completeExceptionally(cause);
                    } else {
                        future.complete(result);
                    }
                });
                // return if the outstanding read has been issued
                return;
            } else {
                outstandingReadToEnd = future;
                future.whenComplete((result, cause) -> {
                    synchronized (PulsarOffsetBackingStore.this) {
                        outstandingReadToEnd = null;
                    }
                });
            }
        }
        producer.flushAsync().whenComplete((ignored, cause) -> {
            if (null != cause) {
                future.completeExceptionally(cause);
            } else {
                checkAndReadNext(future);
            }
        });
    }

    private void checkAndReadNext(CompletableFuture<Void> endFuture) {
        reader.hasMessageAvailableAsync().whenComplete((hasMessageAvailable, cause) -> {
            if (null != cause) {
                endFuture.completeExceptionally(cause);
            } else {
                if (hasMessageAvailable) {
                    readNext(endFuture);
                } else {
                    endFuture.complete(null);
                }
            }
        });
    }

    private void readNext(CompletableFuture<Void> endFuture) {
        reader.readNextAsync().whenComplete((message, cause) -> {
            if (null != cause) {
                endFuture.completeExceptionally(cause);
            } else {
                processMessage(message);
                checkAndReadNext(endFuture);
            }
        });
    }

    void processMessage(Message<byte[]> message) {
        if (message.getKey() != null) {
            data.put(
                ByteBuffer.wrap(message.getKey().getBytes(UTF_8)),
                ByteBuffer.wrap(message.getValue()));
        } else {
            log.debug("Got message without key from the offset storage topic, skip it. message value: {}",
                    message.getValue());
        }
    }

    @Override
    public void start() {
        try {
            producer = client.newProducer(Schema.BYTES)
                .topic(topic)
                .create();
            log.info("Successfully created producer to produce updates to topic {}", topic);

            reader = client.newReader(Schema.BYTES)
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .loadConf(readerConfigMap)
                .create();
            log.info("Successfully created reader to replay updates from topic {}", topic);

            CompletableFuture<Void> endFuture = new CompletableFuture<>();
            readToEnd(endFuture);
            endFuture.get();
        } catch (PulsarClientException e) {
            log.error("Failed to setup pulsar producer/reader to cluster", e);
            throw new RuntimeException("Failed to setup pulsar producer/reader to cluster ",  e);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to start PulsarOffsetBackingStore", e);
            throw new RuntimeException("Failed to start PulsarOffsetBackingStore",  e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping PulsarOffsetBackingStore");
        if (null != producer) {
            try {
                producer.flush();
            } catch (PulsarClientException pce) {
                log.warn("Failed to flush the producer", pce);
            }
            try {
                producer.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close producer", e);
            }
            producer = null;
        }
        if (null != reader) {
            try {
                reader.close();
            } catch (IOException e) {
                log.warn("Failed to close reader", e);
            }
            reader = null;
        }
        data.clear();

        // do not close the client, it is provided by the sink context
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        CompletableFuture<Void> endFuture = new CompletableFuture<>();
        readToEnd(endFuture);
        return endFuture.thenApply(ignored -> {
            Map<ByteBuffer, ByteBuffer> values = new HashMap<>();
            for (ByteBuffer key : keys) {
                ByteBuffer value = data.get(key);
                if (null != value) {
                    values.put(key, value);
                }
            }
            return values;
        });
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
        values.forEach((key, value) -> {
            ByteBuf bb = Unpooled.wrappedBuffer(key);
            byte[] keyBytes = ByteBufUtil.getBytes(bb);
            bb = Unpooled.wrappedBuffer(value);
            byte[] valBytes = ByteBufUtil.getBytes(bb);
            producer.newMessage()
                .key(new String(keyBytes, UTF_8))
                .value(valBytes)
                .sendAsync();
        });
        return producer.flushAsync().whenComplete((ignored, cause) -> {
            if (null != callback) {
                callback.onCompletion(cause, ignored);
            }
            if (null == cause) {
                readToEnd(new CompletableFuture<>());
            }
        });
    }
}

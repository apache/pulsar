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
package org.apache.flink.streaming.connectors.pulsar;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.IOUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pulsar source (consumer) which receives messages from a topic and acknowledges messages.
 * When checkpointing is enabled, it guarantees at least once processing semantics.
 *
 * <p>When checkpointing is disabled, it auto acknowledges messages based on the number of messages it has
 * received. In this mode messages may be dropped.
 */
class PulsarConsumerSource<T> extends MessageAcknowledgingSourceBase<T, MessageId> implements PulsarSourceBase<T> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarConsumerSource.class);

    private final int messageReceiveTimeoutMs = 100;

    private ClientConfigurationData clientConfigurationData;
    private ConsumerConfigurationData<byte[]> consumerConfigurationData;

    private final DeserializationSchema<T> deserializer;

    private PulsarClient client;
    private Consumer<byte[]> consumer;

    private boolean isCheckpointingEnabled;

    private final long acknowledgementBatchSize;
    private long batchCount;

    private transient volatile boolean isRunning;

    PulsarConsumerSource(PulsarSourceBuilder<T> builder) {
        super(MessageId.class);

        clientConfigurationData = new ClientConfigurationData();
        consumerConfigurationData = new ConsumerConfigurationData<>();

        this.clientConfigurationData = builder.clientConfigurationData;
        this.consumerConfigurationData = builder.consumerConfigurationData;
        this.deserializer = builder.deserializationSchema;
        this.acknowledgementBatchSize = builder.acknowledgementBatchSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        final RuntimeContext context = getRuntimeContext();
        if (context instanceof StreamingRuntimeContext) {
            isCheckpointingEnabled = ((StreamingRuntimeContext) context).isCheckpointingEnabled();
        }

        client = getClient();
        consumer = createConsumer(client);

        isRunning = true;
    }

    @Override
    protected void acknowledgeIDs(long checkpointId, Set<MessageId> messageIds) {
        if (consumer == null) {
            LOG.error("null consumer unable to acknowledge messages");
            throw new RuntimeException("null pulsar consumer unable to acknowledge messages");
        }

        if (messageIds.isEmpty()) {
            LOG.info("no message ids to acknowledge");
            return;
        }

        Map<String, CompletableFuture<Void>> futures = new HashMap<>(messageIds.size());
        for (MessageId id : messageIds) {
            futures.put(id.toString(), consumer.acknowledgeAsync(id));
        }

        futures.forEach((k, f) -> {
            try {
                f.get();
            } catch (Exception e) {
                LOG.error("failed to acknowledge messageId " + k, e);
                throw new RuntimeException("Messages could not be acknowledged during checkpoint creation.", e);
            }
        });
    }

    @Override
    public void run(SourceContext<T> context) throws Exception {
        Message message;
        while (isRunning) {
            message = consumer.receive(messageReceiveTimeoutMs, TimeUnit.MILLISECONDS);
            if (message == null) {
                continue;
            }

            if (isCheckpointingEnabled) {
                emitCheckpointing(context, message);
            } else {
                emitAutoAcking(context, message);
            }
        }
    }

    private void emitCheckpointing(SourceContext<T> context, Message message) throws IOException {
        synchronized (context.getCheckpointLock()) {
            if (!addId(message.getMessageId())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("messageId=" + message.getMessageId().toString() + " already processed.");
                }
                return;
            }
            context.collect(deserialize(message));
        }
    }

    private void emitAutoAcking(SourceContext<T> context, Message message) throws IOException {
        context.collect(deserialize(message));
        batchCount++;
        if (batchCount >= acknowledgementBatchSize) {
            LOG.info("processed {} messages acknowledging messageId {}", batchCount, message.getMessageId());
            consumer.acknowledgeCumulative(message.getMessageId());
            batchCount = 0;
        }
    }

    protected T deserialize(Message message) throws IOException {
        return deserializer.deserialize(message.getData());
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        IOUtils.cleanup(LOG, consumer);
        IOUtils.cleanup(LOG, client);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    boolean isCheckpointingEnabled() {
        return isCheckpointingEnabled;
    }

    PulsarClient getClient() throws ExecutionException {
        return CachedPulsarClient.getOrCreate(clientConfigurationData);
    }

    Consumer<byte[]> createConsumer(PulsarClient client) throws PulsarClientException {
        return ((PulsarClientImpl) client).subscribeAsync(consumerConfigurationData).join();
    }
}

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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.protocol.Commands.readChecksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.bookkeeper.mledger.util.Rate;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicClosedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicTerminatedException;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NonPersistentPublisherStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.DateFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.pulsar.broker.service.AbstractReplicator.REPL_PRODUCER_NAME_DELIMITER;

/**
 * Represents a currently connected producer
 */
public class Producer {
    private final Topic topic;
    private final ServerCnx cnx;
    private final String producerName;
    private final long producerId;
    private final String appId;
    private Rate msgIn;
    // it records msg-drop rate only for non-persistent topic
    private final Rate msgDrop;
    private AuthenticationDataSource authenticationData;

    private volatile long pendingPublishAcks = 0;
    private static final AtomicLongFieldUpdater<Producer> pendingPublishAcksUpdater = AtomicLongFieldUpdater
            .newUpdater(Producer.class, "pendingPublishAcks");

    private boolean isClosed = false;
    private final CompletableFuture<Void> closeFuture;

    private final PublisherStats stats;
    private final boolean isRemote;
    private final String remoteCluster;
    private final boolean isNonPersistentTopic;
    private final boolean isEncrypted;

    private final Map<String, String> metadata;

    private final SchemaVersion schemaVersion;

    public Producer(Topic topic, ServerCnx cnx, long producerId, String producerName, String appId,
        boolean isEncrypted, Map<String, String> metadata, SchemaVersion schemaVersion) {
        this.topic = topic;
        this.cnx = cnx;
        this.producerId = producerId;
        this.producerName = checkNotNull(producerName);
        this.closeFuture = new CompletableFuture<>();
        this.appId = appId;
        this.authenticationData = cnx.authenticationData;
        this.msgIn = new Rate();
        this.isNonPersistentTopic = topic instanceof NonPersistentTopic;
        this.msgDrop = this.isNonPersistentTopic ? new Rate() : null;

        this.metadata = metadata != null ? metadata : Collections.emptyMap();

        this.stats = isNonPersistentTopic ? new NonPersistentPublisherStats() : new PublisherStats();
        stats.setAddress(cnx.clientAddress().toString());
        stats.setConnectedSince(DateFormatter.now());
        stats.setClientVersion(cnx.getClientVersion());
        stats.setProducerName(producerName);
        stats.producerId = producerId;
        stats.metadata = this.metadata;

        this.isRemote = producerName
                .startsWith(cnx.getBrokerService().pulsar().getConfiguration().getReplicatorPrefix());
        this.remoteCluster = isRemote ? producerName.split("\\.")[2].split(REPL_PRODUCER_NAME_DELIMITER)[0] : null;

        this.isEncrypted = isEncrypted;
        this.schemaVersion = schemaVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Producer) {
            Producer other = (Producer) obj;
            return Objects.equals(producerName, other.producerName) && Objects.equals(topic, other.topic);
        }

        return false;
    }

    public void publishMessage(long producerId, long sequenceId, ByteBuf headersAndPayload, long batchSize) {
        if (isClosed) {
            cnx.ctx().channel().eventLoop().execute(() -> {
                cnx.ctx().writeAndFlush(Commands.newSendError(producerId, sequenceId, ServerError.PersistenceError,
                        "Producer is closed"));
                cnx.completedSendOperation(isNonPersistentTopic);
            });

            return;
        }

        if (!verifyChecksum(headersAndPayload)) {
            cnx.ctx().channel().eventLoop().execute(() -> {
                cnx.ctx().writeAndFlush(
                        Commands.newSendError(producerId, sequenceId, ServerError.ChecksumError, "Checksum failed on the broker"));
                cnx.completedSendOperation(isNonPersistentTopic);
            });
            return;
        }

        if (topic.isEncryptionRequired()) {

            headersAndPayload.markReaderIndex();
            MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
            headersAndPayload.resetReaderIndex();

            // Check whether the message is encrypted or not
            if (msgMetadata.getEncryptionKeysCount() < 1) {
                log.warn("[{}] Messages must be encrypted", getTopic().getName());
                cnx.ctx().channel().eventLoop().execute(() -> {
                    cnx.ctx().writeAndFlush(Commands.newSendError(producerId, sequenceId, ServerError.MetadataError,
                            "Messages must be encrypted"));
                    cnx.completedSendOperation(isNonPersistentTopic);
                });
                return;
            }
        }

        startPublishOperation();
        topic.publishMessage(headersAndPayload,
                MessagePublishContext.get(this, sequenceId, msgIn, headersAndPayload.readableBytes(), batchSize,
                        System.nanoTime()));
    }

    private boolean verifyChecksum(ByteBuf headersAndPayload) {
        if (hasChecksum(headersAndPayload)) {
            int readerIndex = headersAndPayload.readerIndex();

            try {
                int checksum = readChecksum(headersAndPayload);
                long computedChecksum = computeChecksum(headersAndPayload);
                if (checksum == computedChecksum) {
                    return true;
                } else {
                    log.error("[{}] [{}] Failed to verify checksum", topic, producerName);
                    return false;
                }
            } finally {
                headersAndPayload.readerIndex(readerIndex);
            }
        } else {
            // ignore if checksum is not available
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Payload does not have checksum to verify", topic, producerName);
            }
            return true;
        }
    }

    private void startPublishOperation() {
        // A single thread is incrementing/decrementing this counter, so we can use lazySet which doesn't involve a mem
        // barrier
        pendingPublishAcksUpdater.lazySet(this, pendingPublishAcks + 1);
    }

    private void publishOperationCompleted() {
        long newPendingPublishAcks = this.pendingPublishAcks - 1;
        pendingPublishAcksUpdater.lazySet(this, newPendingPublishAcks);

        // Check the close future to avoid grabbing the mutex every time the pending acks goes down to 0
        if (newPendingPublishAcks == 0 && !closeFuture.isDone()) {
            synchronized (this) {
                if (isClosed && !closeFuture.isDone()) {
                    closeNow();
                }
            }
        }
    }

    public void recordMessageDrop(int batchSize) {
        if (this.isNonPersistentTopic) {
            msgDrop.recordEvent(batchSize);
        }
    }

    /**
     * Return the sequence id of
     * @return
     */
    public long getLastSequenceId() {
        if (isNonPersistentTopic) {
            return -1;
        } else {
            return ((PersistentTopic) topic).getLastPublishedSequenceId(producerName);
        }
    }

    private static final class MessagePublishContext implements PublishContext, Runnable {
        private Producer producer;
        private long sequenceId;
        private long ledgerId;
        private long entryId;
        private Rate rateIn;
        private int msgSize;
        private long batchSize;
        private long startTimeNs;

        private String originalProducerName;
        private long originalSequenceId;

        public String getProducerName() {
            return producer.getProducerName();
        }

        public long getSequenceId() {
            return sequenceId;
        }

        @Override
        public void setOriginalProducerName(String originalProducerName) {
            this.originalProducerName = originalProducerName;
        }

        @Override
        public void setOriginalSequenceId(long originalSequenceId) {
            this.originalSequenceId = originalSequenceId;
        }

        @Override
        public String getOriginalProducerName() {
            return originalProducerName;
        }

        @Override
        public long getOriginalSequenceId() {
            return originalSequenceId;
        }

        /**
         * Executed from managed ledger thread when the message is persisted
         */
        @Override
        public void completed(Exception exception, long ledgerId, long entryId) {
            if (exception != null) {
                ServerError serverError = (exception instanceof TopicTerminatedException)
                        ? ServerError.TopicTerminatedError : ServerError.PersistenceError;

                producer.cnx.ctx().channel().eventLoop().execute(() -> {
                    if (!(exception instanceof TopicClosedException)) {
                        // For TopicClosed exception there's no need to send explicit error, since the client was
                        // already notified
                        producer.cnx.ctx().writeAndFlush(Commands.newSendError(producer.producerId, sequenceId,
                                serverError, exception.getMessage()));
                    }
                    producer.cnx.completedSendOperation(producer.isNonPersistentTopic);
                    producer.publishOperationCompleted();
                    recycle();
                });
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] [{}] triggered send callback. cnx {}, sequenceId {}", producer.topic,
                            producer.producerName, producer.producerId, producer.cnx.clientAddress(), sequenceId);
                }

                this.ledgerId = ledgerId;
                this.entryId = entryId;
                producer.topic.recordAddLatency(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTimeNs));
                producer.cnx.ctx().channel().eventLoop().execute(this);
            }
        }

        /**
         * Executed from I/O thread when sending receipt back to client
         */
        @Override
        public void run() {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] [{}] Persisted message. cnx {}, sequenceId {}", producer.topic,
                        producer.producerName, producer.producerId, producer.cnx, sequenceId);
            }

            // stats
            rateIn.recordMultipleEvents(batchSize, msgSize);
            producer.cnx.ctx().writeAndFlush(
                    Commands.newSendReceipt(producer.producerId, sequenceId, ledgerId, entryId),
                    producer.cnx.ctx().voidPromise());
            producer.cnx.completedSendOperation(producer.isNonPersistentTopic);
            producer.publishOperationCompleted();
            recycle();
        }

        static MessagePublishContext get(Producer producer, long sequenceId, Rate rateIn, int msgSize,
                long batchSize, long startTimeNs) {
            MessagePublishContext callback = RECYCLER.get();
            callback.producer = producer;
            callback.sequenceId = sequenceId;
            callback.rateIn = rateIn;
            callback.msgSize = msgSize;
            callback.batchSize = batchSize;
            callback.originalProducerName = null;
            callback.originalSequenceId = -1;
            callback.startTimeNs = startTimeNs;
            return callback;
        }

        private final Handle<MessagePublishContext> recyclerHandle;

        private MessagePublishContext(Handle<MessagePublishContext> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<MessagePublishContext> RECYCLER = new Recycler<MessagePublishContext>() {
            protected MessagePublishContext newObject(Recycler.Handle<MessagePublishContext> handle) {
                return new MessagePublishContext(handle);
            }
        };

        public void recycle() {
            producer = null;
            sequenceId = -1;
            rateIn = null;
            msgSize = 0;
            ledgerId = -1;
            entryId = -1;
            batchSize = 0;
            startTimeNs = -1;
            recyclerHandle.recycle(this);
        }
    }

    public Topic getTopic() {
        return topic;
    }

    public String getProducerName() {
        return producerName;
    }

    public long getProducerId() {
        return producerId;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("topic", topic).add("client", cnx.clientAddress())
                .add("producerName", producerName).add("producerId", producerId).toString();
    }

    /**
     * Close the producer immediately if: a. the connection is dropped b. it's a graceful close and no pending publish
     * acks are left else wait for pending publish acks
     *
     * @return completable future indicate completion of close
     */
    public synchronized CompletableFuture<Void> close() {
        if (log.isDebugEnabled()) {
            log.debug("Closing producer {} -- isClosed={}", this, isClosed);
        }

        if (!isClosed) {
            isClosed = true;
            if (log.isDebugEnabled()) {
                log.debug("Trying to close producer {} -- cnxIsActive: {} -- pendingPublishAcks: {}", this,
                        cnx.isActive(), pendingPublishAcks);
            }
            if (!cnx.isActive() || pendingPublishAcks == 0) {
                closeNow();
            }
        }
        return closeFuture;
    }

    void closeNow() {
        topic.removeProducer(this);
        cnx.removedProducer(this);

        if (log.isDebugEnabled()) {
            log.debug("Removed producer: {}", this);
        }
        closeFuture.complete(null);
    }

    /**
     * It closes the producer from server-side and sends command to client to disconnect producer from existing
     * connection without closing that connection.
     *
     * @return Completable future indicating completion of producer close
     */
    public CompletableFuture<Void> disconnect() {
        if (!closeFuture.isDone()) {
            log.info("Disconnecting producer: {}", this);
            cnx.ctx().executor().execute(() -> {
                cnx.closeProducer(this);
                closeNow();
            });
        }
        return closeFuture;
    }

    public void updateRates() {
        msgIn.calculateRate();
        stats.msgRateIn = msgIn.getRate();
        stats.msgThroughputIn = msgIn.getValueRate();
        stats.averageMsgSize = msgIn.getAverageValue();
        if (this.isNonPersistentTopic) {
            msgDrop.calculateRate();
            ((NonPersistentPublisherStats) stats).msgDropRate = msgDrop.getRate();
        }
    }

    public boolean isRemote() {
        return isRemote;
    }

    public String getRemoteCluster() {
        return remoteCluster;
    }

    public PublisherStats getStats() {
        return stats;
    }

    public boolean isNonPersistentTopic() {
        return isNonPersistentTopic;
    }

    @VisibleForTesting
    long getPendingPublishAcks() {
        return pendingPublishAcks;
    }

    public void checkPermissions() {
        TopicName topicName = TopicName.get(topic.getName());
        if (cnx.getBrokerService().getAuthorizationService() != null) {
            try {
                if (cnx.getBrokerService().getAuthorizationService().canProduce(topicName, appId,
                        authenticationData)) {
                    return;
                }
            } catch (Exception e) {
                log.warn("[{}] Get unexpected error while autorizing [{}]  {}", appId, topic.getName(), e.getMessage(),
                        e);
            }
            log.info("[{}] is not allowed to produce on topic [{}] anymore", appId, topic.getName());
            disconnect();
        }
    }

    public void checkEncryption() {
        if (topic.isEncryptionRequired() && !isEncrypted) {
            log.info("[{}] [{}] Unencrypted producer is not allowed to produce on topic [{}] anymore",
                    producerId, producerName, topic.getName());
            disconnect();
        }
    }

    public SchemaVersion getSchemaVersion() {
        return schemaVersion;
    }

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

}

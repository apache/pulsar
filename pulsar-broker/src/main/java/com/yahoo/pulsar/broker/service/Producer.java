/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.yahoo.pulsar.broker.service.persistent.PersistentTopic.DATE_FORMAT;
import static com.yahoo.pulsar.checksum.utils.Crc32cChecksum.computeChecksum;
import static com.yahoo.pulsar.common.api.Commands.hasChecksum;
import static com.yahoo.pulsar.common.api.Commands.readChecksum;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.bookkeeper.mledger.util.Rate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.yahoo.pulsar.broker.service.Topic.PublishCallback;
import com.yahoo.pulsar.common.api.Commands;
import com.yahoo.pulsar.common.api.proto.PulsarApi.ServerError;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.policies.data.PublisherStats;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

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

    private volatile long pendingPublishAcks = 0;
    private static final AtomicLongFieldUpdater<Producer> pendingPublishAcksUpdater = AtomicLongFieldUpdater
            .newUpdater(Producer.class, "pendingPublishAcks");

    private boolean isClosed = false;
    private final CompletableFuture<Void> closeFuture;

    private final PublisherStats stats;
    private final boolean isRemote;
    private final String remoteCluster;

    public Producer(Topic topic, ServerCnx cnx, long producerId, String producerName, String appId) {
        this.topic = topic;
        this.cnx = cnx;
        this.producerId = producerId;
        this.producerName = checkNotNull(producerName);
        this.closeFuture = new CompletableFuture<>();
        this.appId = appId;
        this.msgIn = new Rate();

        this.stats = new PublisherStats();
        stats.address = cnx.clientAddress().toString();
        stats.connectedSince = DATE_FORMAT.format(Instant.now());
        stats.producerName = producerName;
        stats.producerId = producerId;

        this.isRemote = producerName
                .startsWith(cnx.getBrokerService().pulsar().getConfiguration().getReplicatorPrefix());
        this.remoteCluster = isRemote ? producerName.split("\\.")[2] : null;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(producerName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Producer) {
            Producer other = (Producer) obj;
            return Objects.equal(producerName, other.producerName) && Objects.equal(topic, other.topic);
        }

        return false;
    }

    public void publishMessage(long producerId, long sequenceId, ByteBuf headersAndPayload, long batchSize) {
        if (isClosed) {
            cnx.ctx().channel().eventLoop().execute(() -> {
                cnx.ctx().writeAndFlush(
                        Commands.newSendError(producerId, sequenceId, new IllegalStateException("Producer is closed")));
                cnx.completedSendOperation();
            });

            return;
        }

        if (!verifyChecksum(headersAndPayload)) {
            cnx.ctx().channel().eventLoop().execute(() -> {
                cnx.ctx().writeAndFlush(
                        Commands.newSendError(producerId, sequenceId, ServerError.ChecksumError, "Checksum failed on the broker"));
                cnx.completedSendOperation();
            });
            return;
        }

        startPublishOperation();
        topic.publishMessage(headersAndPayload,
                MessagePublishedCallback.get(this, sequenceId, msgIn, headersAndPayload.readableBytes(), batchSize));
    }

    private boolean verifyChecksum(ByteBuf headersAndPayload) {
        
        if (hasChecksum(headersAndPayload)) {
            int checksum = readChecksum(headersAndPayload).intValue();
            int readerIndex = headersAndPayload.readerIndex();
            try {
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

    private static final class MessagePublishedCallback implements PublishCallback, Runnable {
        private Producer producer;
        private long sequenceId;
        private long ledgerId;
        private long entryId;
        private Rate rateIn;
        private int msgSize;
        private long batchSize;

        /**
         * Executed from managed ledger thread when the message is persisted
         */
        @Override
        public void completed(Exception exception, long ledgerId, long entryId) {
            if (exception != null) {
                producer.cnx.ctx().channel().eventLoop().execute(() -> {
                    producer.cnx.ctx().writeAndFlush(Commands.newSendError(producer.producerId, sequenceId, exception));
                    producer.cnx.completedSendOperation();
                    producer.publishOperationCompleted();
                });
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] [{}] triggered send callback. cnx {}, sequenceId {}", producer.topic,
                            producer.producerName, producer.producerId, producer.cnx.clientAddress(), sequenceId);
                }

                this.ledgerId = ledgerId;
                this.entryId = entryId;
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
            producer.cnx.completedSendOperation();
            producer.publishOperationCompleted();
            recycle();
        }

        static MessagePublishedCallback get(Producer producer, long sequenceId, Rate rateIn, int msgSize,
                long batchSize) {
            MessagePublishedCallback callback = RECYCLER.get();
            callback.producer = producer;
            callback.sequenceId = sequenceId;
            callback.rateIn = rateIn;
            callback.msgSize = msgSize;
            callback.batchSize = batchSize;
            return callback;
        }

        private final Handle recyclerHandle;

        private MessagePublishedCallback(Handle recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<MessagePublishedCallback> RECYCLER = new Recycler<MessagePublishedCallback>() {
            protected MessagePublishedCallback newObject(Recycler.Handle handle) {
                return new MessagePublishedCallback(handle);
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
            RECYCLER.recycle(this, recyclerHandle);
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

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("topic", topic).add("client", cnx.clientAddress())
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

    @VisibleForTesting
    long getPendingPublishAcks() {
        return pendingPublishAcks;
    }

    public void checkPermissions() {
        DestinationName destination = DestinationName.get(topic.getName());
        if (cnx.getBrokerService().getAuthorizationManager() != null) {
            try {
                if (cnx.getBrokerService().getAuthorizationManager().canProduce(destination, appId)) {
                    return;
                }
            } catch (Exception e) {
                log.warn("[{}] Get unexpected error while autorizing [{}]  {}", appId, topic.getName(), e.getMessage(),
                        e);
            }
            log.info("[{}] is not allowed to produce from destination [{}] anymore", appId, topic.getName());
            disconnect();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Producer.class);
}

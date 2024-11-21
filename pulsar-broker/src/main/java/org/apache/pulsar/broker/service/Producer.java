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
package org.apache.pulsar.broker.service;

import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static org.apache.pulsar.broker.service.AbstractReplicator.REPL_PRODUCER_NAME_DELIMITER;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.protocol.Commands.readChecksum;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.opentelemetry.api.common.Attributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicClosedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicTerminatedException;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated.ResourceType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterPolicies.ClusterUrl;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.stats.NonPersistentPublisherStatsImpl;
import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a currently connected producer.
 */
public class Producer {
    private final Topic topic;
    private final TransportCnx cnx;
    private final String producerName;
    private final long epoch;
    private final boolean userProvidedProducerName;
    private final long producerId;
    private final String appId;
    private final BrokerInterceptor brokerInterceptor;

    private volatile long pendingPublishAcks = 0;
    private static final AtomicLongFieldUpdater<Producer> pendingPublishAcksUpdater = AtomicLongFieldUpdater
            .newUpdater(Producer.class, "pendingPublishAcks");

    private boolean isClosed = false;
    private final CompletableFuture<Void> closeFuture;

    private final PublisherStatsImpl stats;
    private volatile Attributes attributes = null;
    private static final AtomicReferenceFieldUpdater<Producer, Attributes> ATTRIBUTES_FIELD_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(Producer.class, Attributes.class, "attributes");

    private final boolean isRemote;
    private final String remoteCluster;
    private final boolean isNonPersistentTopic;
    private final boolean isShadowTopic;
    private final boolean isEncrypted;

    private final ProducerAccessMode accessMode;
    private Optional<Long> topicEpoch;

    private final Map<String, String> metadata;

    private final SchemaVersion schemaVersion;
    private final String clientAddress; // IP address only, no port number included
    private final AtomicBoolean isDisconnecting = new AtomicBoolean(false);

    public Producer(Topic topic, TransportCnx cnx, long producerId, String producerName, String appId,
            boolean isEncrypted, Map<String, String> metadata, SchemaVersion schemaVersion, long epoch,
            boolean userProvidedProducerName,
            ProducerAccessMode accessMode,
            Optional<Long> topicEpoch,
            boolean supportsPartialProducer) {
        final ServiceConfiguration serviceConf =  cnx.getBrokerService().pulsar().getConfiguration();

        this.topic = topic;
        this.cnx = cnx;
        this.producerId = producerId;
        this.producerName = Objects.requireNonNull(producerName);
        this.userProvidedProducerName = userProvidedProducerName;
        this.epoch = epoch;
        this.closeFuture = new CompletableFuture<>();
        this.appId = appId;
        this.isNonPersistentTopic = topic instanceof NonPersistentTopic;
        this.isShadowTopic =
                topic instanceof PersistentTopic && ((PersistentTopic) topic).getShadowSourceTopic().isPresent();

        this.metadata = metadata != null ? metadata : Collections.emptyMap();

        this.stats = isNonPersistentTopic ? new NonPersistentPublisherStatsImpl() : new PublisherStatsImpl();
        stats.setAddress(cnx.clientSourceAddressAndPort());
        stats.setConnectedSince(DateFormatter.now());
        stats.setClientVersion(cnx.getClientVersion());
        stats.setProducerName(producerName);
        stats.producerId = producerId;
        if (serviceConf.isAggregatePublisherStatsByProducerName() && stats.getProducerName() != null) {
            // If true and the client supports partial producer,
            // aggregate publisher stats of PartitionedTopicStats by producerName.
            // Otherwise, aggregate it by list index.
            stats.setSupportsPartialProducer(supportsPartialProducer);
        } else {
            // aggregate publisher stats of PartitionedTopicStats by list index.
            stats.setSupportsPartialProducer(false);
        }
        stats.metadata = this.metadata;
        stats.accessMode = Commands.convertProducerAccessMode(accessMode);


        String replicatorPrefix = serviceConf.getReplicatorPrefix() + ".";
        this.isRemote = producerName.startsWith(replicatorPrefix);
        this.remoteCluster = parseRemoteClusterName(producerName, isRemote, replicatorPrefix);

        this.isEncrypted = isEncrypted;
        this.schemaVersion = schemaVersion;
        this.accessMode = accessMode;
        this.topicEpoch = topicEpoch;

        this.clientAddress = cnx.clientSourceAddress();
        this.brokerInterceptor = cnx.getBrokerService().getInterceptor();
    }

    /**
     * Producer name for replicator is in format.
     * "replicatorPrefix.localCluster" (old)
     * "replicatorPrefix.localCluster-->remoteCluster" (new)
     */
    private String parseRemoteClusterName(String producerName, boolean isRemote, String replicatorPrefix) {
        if (isRemote) {
            String clusterName = producerName.substring(replicatorPrefix.length());
            return clusterName.contains(REPL_PRODUCER_NAME_DELIMITER)
                    ? clusterName.split(REPL_PRODUCER_NAME_DELIMITER)[0] : clusterName;
        }
        return null;
    }

    /**
     * Method to determine if this producer can replace another producer.
     * @param other - producer to compare to this one
     * @return true if this producer is a subsequent instantiation of the same logical producer. Otherwise, false.
     */
    public boolean isSuccessorTo(Producer other) {
        return Objects.equals(producerName, other.producerName)
                && Objects.equals(topic, other.topic)
                && producerId == other.producerId
                && Objects.equals(cnx, other.cnx)
                && other.getEpoch() < epoch;
    }

    public void publishMessage(long producerId, long sequenceId, ByteBuf headersAndPayload, int batchSize,
            boolean isChunked, boolean isMarker, Position position) {
        if (checkAndStartPublish(producerId, sequenceId, headersAndPayload, batchSize, position)) {
            publishMessageToTopic(headersAndPayload, sequenceId, batchSize, isChunked, isMarker, position);
        }
    }

    public void publishMessage(long producerId, long lowestSequenceId, long highestSequenceId,
            ByteBuf headersAndPayload, int batchSize, boolean isChunked, boolean isMarker, Position position) {
        if (lowestSequenceId > highestSequenceId) {
            cnx.execute(() -> {
                cnx.getCommandSender().sendSendError(producerId, highestSequenceId, ServerError.MetadataError,
                        "Invalid lowest or highest sequence id");
                cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
            });
            return;
        }
        if (checkAndStartPublish(producerId, highestSequenceId, headersAndPayload, batchSize, position)) {
            publishMessageToTopic(headersAndPayload, lowestSequenceId, highestSequenceId, batchSize, isChunked,
                    isMarker, position);
        }
    }

    public boolean checkAndStartPublish(long producerId, long sequenceId, ByteBuf headersAndPayload, int batchSize,
                                        Position position) {
        if (!isShadowTopic && position != null) {
            cnx.execute(() -> {
                cnx.getCommandSender().sendSendError(producerId, sequenceId, ServerError.NotAllowedError,
                        "Only shadow topic supports sending messages with messageId");
                cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
            });
            return false;
        }
        if (isShadowTopic && position == null) {
            cnx.execute(() -> {
                cnx.getCommandSender().sendSendError(producerId, sequenceId, ServerError.NotAllowedError,
                        "Cannot send messages to a shadow topic");
                cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
            });
            return false;
        }
        if (isClosed) {
            cnx.execute(() -> {
                cnx.getCommandSender().sendSendError(producerId, sequenceId, ServerError.PersistenceError,
                        "Producer is closed");
                cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
            });
            return false;
        }

        if (!verifyChecksum(headersAndPayload)) {
            cnx.execute(() -> {
                cnx.getCommandSender().sendSendError(producerId, sequenceId, ServerError.ChecksumError,
                        "Checksum failed on the broker");
                cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
            });
            return false;
        }

        if (topic.isEncryptionRequired()) {

            headersAndPayload.markReaderIndex();
            MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
            headersAndPayload.resetReaderIndex();
            int encryptionKeysCount = msgMetadata.getEncryptionKeysCount();
            // Check whether the message is encrypted or not
            if (encryptionKeysCount < 1) {
                log.warn("[{}] Messages must be encrypted", getTopic().getName());
                cnx.execute(() -> {
                    cnx.getCommandSender().sendSendError(producerId, sequenceId, ServerError.MetadataError,
                            "Messages must be encrypted");
                    cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
                });
                return false;
            }
        }

        startPublishOperation((int) batchSize, headersAndPayload.readableBytes());
        return true;
    }

    private void publishMessageToTopic(ByteBuf headersAndPayload, long sequenceId, int batchSize, boolean isChunked,
                                       boolean isMarker, Position position) {
        MessagePublishContext messagePublishContext =
                MessagePublishContext.get(this, sequenceId, headersAndPayload.readableBytes(),
                        batchSize, isChunked, System.nanoTime(), isMarker, position);
        if (brokerInterceptor != null) {
            brokerInterceptor
                    .onMessagePublish(this, headersAndPayload, messagePublishContext);
        }
        topic.publishMessage(headersAndPayload, messagePublishContext);
    }

    private void publishMessageToTopic(ByteBuf headersAndPayload, long lowestSequenceId, long highestSequenceId,
                                       int batchSize, boolean isChunked, boolean isMarker, Position position) {
        MessagePublishContext messagePublishContext = MessagePublishContext.get(this, lowestSequenceId,
                highestSequenceId, headersAndPayload.readableBytes(), batchSize,
                isChunked, System.nanoTime(), isMarker, position);
        if (brokerInterceptor != null) {
            brokerInterceptor
                    .onMessagePublish(this, headersAndPayload, messagePublishContext);
        }
        topic.publishMessage(headersAndPayload, messagePublishContext);
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

    private void startPublishOperation(int batchSize, long msgSize) {
        // A single thread is incrementing/decrementing this counter, so we can use lazySet which doesn't involve a mem
        // barrier
        pendingPublishAcksUpdater.lazySet(this, pendingPublishAcks + 1);
        // increment publish-count
        this.getTopic().incrementPublishCount(this, batchSize, msgSize);
    }

    private void publishOperationCompleted() {
        long newPendingPublishAcks = this.pendingPublishAcks - 1;
        pendingPublishAcksUpdater.lazySet(this, newPendingPublishAcks);

        // Check the close future to avoid grabbing the mutex every time the pending acks goes down to 0
        if (newPendingPublishAcks == 0 && !closeFuture.isDone()) {
            synchronized (this) {
                if (isClosed && !closeFuture.isDone()) {
                    closeNow(true);
                }
            }
        }
    }

    public void recordMessageDrop(int batchSize) {
        if (stats instanceof NonPersistentPublisherStatsImpl nonPersistentPublisherStats) {
            nonPersistentPublisherStats.recordMsgDrop(batchSize);
        }
    }

    /**
     * Return the sequence id of.
     *
     * @return the sequence id
     */
    public long getLastSequenceId() {
        if (isNonPersistentTopic) {
            return -1;
        } else {
            return ((PersistentTopic) topic).getLastPublishedSequenceId(producerName);
        }
    }

    public TransportCnx getCnx() {
        return this.cnx;
    }

    /**
     * MessagePublishContext implements Position because that ShadowManagedLedger need to know the source position info.
     */
    private static final class MessagePublishContext implements PublishContext, Runnable, Position {
        /*
         * To store context information built by message payload
         * processors (time duration, size etc), if any configured
         */
        Map<String, Object> propertyMap;
        private Producer producer;
        private long sequenceId;
        private long ledgerId;
        private long entryId;
        private int msgSize;
        private int batchSize;
        private boolean chunked;
        private boolean isMarker;

        private long startTimeNs;

        private String originalProducerName;
        private long originalSequenceId;

        private long highestSequenceId;
        private long originalHighestSequenceId;

        private long entryTimestamp;

        @Override
        public long getLedgerId() {
            return ledgerId;
        }

        @Override
        public long getEntryId() {
            return entryId;
        }

        public String getProducerName() {
            return producer.getProducerName();
        }

        public long getSequenceId() {
            return sequenceId;
        }

        @Override
        public boolean isChunked() {
            return chunked;
        }

        @Override
        public long getEntryTimestamp() {
            return entryTimestamp;
        }

        @Override
        public void setEntryTimestamp(long entryTimestamp) {
            this.entryTimestamp = entryTimestamp;
        }
        @Override
        public void setProperty(String propertyName, Object value){
            if (this.propertyMap == null) {
                this.propertyMap = new HashMap<>();
            }
            this.propertyMap.put(propertyName, value);
        }

        @Override
        public Object getProperty(String propertyName){
            if (this.propertyMap != null) {
                return this.propertyMap.get(propertyName);
            } else {
                return null;
            }
        }

        @Override
        public long getHighestSequenceId() {
            return highestSequenceId;
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

        @Override
        public void setOriginalHighestSequenceId(long originalHighestSequenceId) {
            this.originalHighestSequenceId = originalHighestSequenceId;
        }

        @Override
        public long getOriginalHighestSequenceId() {
            return originalHighestSequenceId;
        }

        /**
         * Executed from managed ledger thread when the message is persisted.
         */
        @Override
        public void completed(Exception exception, long ledgerId, long entryId) {
            if (exception != null) {
                final ServerError serverError = getServerError(exception);

                producer.cnx.execute(() -> {
                    // if the topic is transferring, we don't send error code to the clients.
                    if (producer.getTopic().isTransferring()) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Received producer exception: {} while transferring.",
                                    producer.getTopic().getName(), exception.getMessage(), exception);
                        }
                    } else if (!(exception instanceof TopicClosedException)) {
                        // For TopicClosed exception there's no need to send explicit error, since the client was
                        // already notified
                        // For TopicClosingOrDeleting exception, a notification will be sent separately
                        long callBackSequenceId = Math.max(highestSequenceId, sequenceId);
                        producer.cnx.getCommandSender().sendSendError(producer.producerId, callBackSequenceId,
                                serverError, exception.getMessage());
                    }
                    producer.cnx.completedSendOperation(producer.isNonPersistentTopic, msgSize);
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
                producer.cnx.execute(this);
            }
        }

        private ServerError getServerError(Exception exception) {
            ServerError serverError;
            if (exception instanceof TopicTerminatedException) {
                serverError = ServerError.TopicTerminatedError;
            } else if (exception instanceof BrokerServiceException.NotAllowedException) {
                serverError = ServerError.NotAllowedError;
            } else {
                serverError = ServerError.PersistenceError;
            }
            return serverError;
        }

        /**
         * Executed from I/O thread when sending receipt back to client.
         */
        @Override
        public void run() {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] [{}] Persisted message. cnx {}, sequenceId {}", producer.topic,
                        producer.producerName, producer.producerId, producer.cnx, sequenceId);
            }

            // stats
            producer.stats.recordMsgIn(batchSize, msgSize);
            producer.topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.NANOSECONDS);
            producer.cnx.getCommandSender().sendSendReceiptResponse(producer.producerId, sequenceId, highestSequenceId,
                    ledgerId, entryId);
            producer.cnx.completedSendOperation(producer.isNonPersistentTopic, msgSize);
            if (this.chunked) {
                producer.stats.recordChunkedMsgIn();
            }
            producer.publishOperationCompleted();
            if (producer.brokerInterceptor != null) {
                producer.brokerInterceptor.messageProduced(
                        (ServerCnx) producer.cnx, producer, startTimeNs, ledgerId, entryId, this);
            }
            recycle();
        }

        static MessagePublishContext get(Producer producer, long sequenceId, int msgSize, int batchSize,
                                         boolean chunked, long startTimeNs, boolean isMarker, Position position) {
            MessagePublishContext callback = RECYCLER.get();
            callback.producer = producer;
            callback.sequenceId = sequenceId;
            callback.msgSize = msgSize;
            callback.batchSize = batchSize;
            callback.chunked = chunked;
            callback.originalProducerName = null;
            callback.originalSequenceId = -1L;
            callback.startTimeNs = startTimeNs;
            callback.isMarker = isMarker;
            callback.ledgerId = position == null ? -1 : position.getLedgerId();
            callback.entryId = position == null ? -1 : position.getEntryId();
            if (callback.propertyMap != null) {
                callback.propertyMap.clear();
            }
            return callback;
        }

        static MessagePublishContext get(Producer producer, long lowestSequenceId, long highestSequenceId, int msgSize,
                             int batchSize, boolean chunked, long startTimeNs, boolean isMarker, Position position) {
            MessagePublishContext callback = RECYCLER.get();
            callback.producer = producer;
            callback.sequenceId = lowestSequenceId;
            callback.highestSequenceId = highestSequenceId;
            callback.msgSize = msgSize;
            callback.batchSize = batchSize;
            callback.originalProducerName = null;
            callback.originalSequenceId = -1L;
            callback.startTimeNs = startTimeNs;
            callback.chunked = chunked;
            callback.isMarker = isMarker;
            callback.ledgerId = position == null ? -1 : position.getLedgerId();
            callback.entryId = position == null ? -1 : position.getEntryId();
            if (callback.propertyMap != null) {
                callback.propertyMap.clear();
            }
            return callback;
        }

        @Override
        public long getNumberOfMessages() {
            return batchSize;
        }

        @Override
        public long getMsgSize() {
            return  msgSize;
        }

        @Override
        public boolean isMarkerMessage() {
            return isMarker;
        }

        private final Handle<MessagePublishContext> recyclerHandle;

        private MessagePublishContext(Handle<MessagePublishContext> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<MessagePublishContext> RECYCLER = new Recycler<MessagePublishContext>() {
            protected MessagePublishContext newObject(Handle<MessagePublishContext> handle) {
                return new MessagePublishContext(handle);
            }
        };

        public void recycle() {
            producer = null;
            sequenceId = -1L;
            highestSequenceId = -1L;
            originalSequenceId = -1L;
            originalHighestSequenceId = -1L;
            msgSize = 0;
            ledgerId = -1L;
            entryId = -1L;
            batchSize = 0;
            startTimeNs = -1L;
            chunked = false;
            isMarker = false;
            if (propertyMap != null) {
                propertyMap.clear();
            }
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
        return MoreObjects.toStringHelper(this).add("topic", topic).add("client", cnx.toString())
                .add("producerName", producerName).add("producerId", producerId).toString();
    }

    /**
     * Close the producer immediately if: a. the connection is dropped b. it's a graceful close and no pending publish
     * acks are left else wait for pending publish acks
     *
     * @return completable future indicate completion of close
     */
    public synchronized CompletableFuture<Void> close(boolean removeFromTopic) {
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
                closeNow(removeFromTopic);
            }
        }
        return closeFuture;
    }

    public void closeNow(boolean removeFromTopic) {
        if (removeFromTopic) {
            topic.removeProducer(this);
        }
        cnx.removedProducer(this);

        if (log.isDebugEnabled()) {
            log.debug("Removed producer: {}", this);
        }
        closeFuture.complete(null);
        isDisconnecting.set(false);
    }

    public CompletableFuture<Void> disconnect() {
        return disconnect(Optional.empty());
    }

    /**
     * It closes the producer from server-side and sends command to client to disconnect producer from existing
     * connection without closing that connection.
     *
     * @return Completable future indicating completion of producer close
     */
    public CompletableFuture<Void> disconnect(Optional<BrokerLookupData> assignedBrokerLookupData) {
        if (!closeFuture.isDone() && isDisconnecting.compareAndSet(false, true)) {
            log.info("Disconnecting producer: {}, assignedBrokerLookupData: {}", this, assignedBrokerLookupData);
            cnx.execute(() -> {
                cnx.closeProducer(this, assignedBrokerLookupData);
                closeNow(true);
            });
        }
        return closeFuture;
    }

    public void topicMigrated(Optional<ClusterUrl> clusterUrl) {
        if (clusterUrl.isPresent()) {
            ClusterUrl url = clusterUrl.get();
            cnx.getCommandSender().sendTopicMigrated(ResourceType.Producer, producerId, url.getBrokerServiceUrl(),
                    url.getBrokerServiceUrlTls());
            disconnect();
        }
    }

    public void updateRates() {
        stats.calculateRates();
        if (stats.getMsgChunkIn().getCount() > 0 && topic instanceof PersistentTopic persistentTopic) {
            persistentTopic.msgChunkPublished = true;
        }
    }

    public boolean isRemote() {
        return isRemote;
    }

    public String getRemoteCluster() {
        return remoteCluster;
    }

    public PublisherStatsImpl getStats() {
        return stats;
    }

    public boolean isNonPersistentTopic() {
        return isNonPersistentTopic;
    }

    public long getEpoch() {
        return epoch;
    }

    public boolean isUserProvidedProducerName() {
        return userProvidedProducerName;
    }

    @VisibleForTesting
    long getPendingPublishAcks() {
        return pendingPublishAcks;
    }

    public CompletableFuture<Void> checkPermissionsAsync() {
        TopicName topicName = TopicName.get(topic.getName());
        if (cnx.getBrokerService().getAuthorizationService() != null) {
            return cnx.getBrokerService().getAuthorizationService()
                    .allowTopicOperationAsync(topicName, TopicOperation.PRODUCE, appId, cnx.getAuthenticationData())
                    .handle((ok, ex) -> {
                        if (ex != null) {
                            log.warn("[{}] Get unexpected error while autorizing [{}]  {}", appId, topic.getName(),
                                    ex.getMessage(), ex);
                        }

                        if (ok == null || !ok) {
                            log.info("[{}] is not allowed to produce on topic [{}] anymore", appId, topic.getName());
                            disconnect();
                        }

                        return null;
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    public void checkEncryption() {
        if (topic.isEncryptionRequired() && !isEncrypted) {
            log.info("[{}] [{}] Unencrypted producer is not allowed to produce on topic [{}] anymore",
                    producerId, producerName, topic.getName());
            disconnect();
        }
    }

    public void publishTxnMessage(TxnID txnID, long producerId, long sequenceId, long highSequenceId,
                                  ByteBuf headersAndPayload, int batchSize, boolean isChunked, boolean isMarker) {
        if (!checkAndStartPublish(producerId, sequenceId, headersAndPayload, batchSize, null)) {
            return;
        }
        MessagePublishContext messagePublishContext =
                MessagePublishContext.get(this, sequenceId, highSequenceId,
                        headersAndPayload.readableBytes(), batchSize, isChunked, System.nanoTime(), isMarker, null);
        if (brokerInterceptor != null) {
            brokerInterceptor
                    .onMessagePublish(this, headersAndPayload, messagePublishContext);
        }
        topic.publishTxnMessage(txnID, headersAndPayload, messagePublishContext);
    }

    public SchemaVersion getSchemaVersion() {
        return schemaVersion;
    }

    public ProducerAccessMode getAccessMode() {
        return accessMode;
    }

    public Optional<Long> getTopicEpoch() {
        return topicEpoch;
    }

    public String getClientAddress() {
        return clientAddress;
    }

    public boolean isDisconnecting() {
        return isDisconnecting.get();
    }

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    /**
     * This method increments a counter that is used to control the throttling of a connection.
     * The connection's read operations are paused when the counter's value is greater than 0, indicating that
     * throttling is in effect.
     * It's important to note that after calling this method, it is the caller's responsibility to ensure that the
     * counter is decremented by calling the {@link #decrementThrottleCount()} method when throttling is no longer
     * needed on the connection.
     */
    public void incrementThrottleCount() {
        cnx.incrementThrottleCount();
    }

    /**
     * This method decrements a counter that is used to control the throttling of a connection.
     * The connection's read operations are resumed when the counter's value is 0, indicating that
     * throttling is no longer in effect.
     * It's important to note that before calling this method, the caller should have previously
     * incremented the counter by calling the {@link #incrementThrottleCount()} method when throttling
     * was needed on the connection.
     */
    public void decrementThrottleCount() {
        cnx.decrementThrottleCount();
    }

    public Attributes getOpenTelemetryAttributes() {
        if (attributes != null) {
            return attributes;
        }
        return ATTRIBUTES_FIELD_UPDATER.updateAndGet(this, old -> {
            if (old != null) {
                return old;
            }
            var topicName = TopicName.get(topic.getName());
            var builder = Attributes.builder()
                    .put(OpenTelemetryAttributes.PULSAR_PRODUCER_NAME, producerName)
                    .put(OpenTelemetryAttributes.PULSAR_PRODUCER_ID, producerId)
                    .put(OpenTelemetryAttributes.PULSAR_PRODUCER_ACCESS_MODE,
                            CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, accessMode.name()))
                    .put(OpenTelemetryAttributes.PULSAR_DOMAIN, topicName.getDomain().toString())
                    .put(OpenTelemetryAttributes.PULSAR_TENANT, topicName.getTenant())
                    .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, topicName.getNamespace())
                    .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicName.getPartitionedTopicName());
            if (topicName.isPartitioned()) {
                builder.put(OpenTelemetryAttributes.PULSAR_PARTITION_INDEX, topicName.getPartitionIndex());
            }
            return builder.build();
        });
    }
}

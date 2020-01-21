package org.apache.pulsar.broker.service;

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

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NonPersistentPublisherStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.util.DateFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static org.apache.pulsar.broker.service.AbstractReplicator.REPL_PRODUCER_NAME_DELIMITER;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.protocol.Commands.readChecksum;

public abstract class Producer {
    protected final Topic topic;
    protected final ServerCnx cnx;
    protected final String producerName;
    protected final long epoch;
    protected final boolean userProvidedProducerName;
    protected final long producerId;
    protected final String appId;
    protected Rate msgIn;
    // it records msg-drop rate only for non-persistent topic
    protected final Rate msgDrop;
    protected AuthenticationDataSource authenticationData;

    protected volatile long pendingPublishAcks = 0;
    protected static final AtomicLongFieldUpdater<Producer> pendingPublishAcksUpdater = AtomicLongFieldUpdater
            .newUpdater(Producer.class, "pendingPublishAcks");

    protected boolean isClosed = false;
    protected final CompletableFuture<Void> closeFuture;

    protected final PublisherStats stats;
    protected final boolean isRemote;
    protected final String remoteCluster;
    protected final boolean isNonPersistentTopic;
    protected final boolean isEncrypted;

    protected final Map<String, String> metadata;

    protected final SchemaVersion schemaVersion;

    public Producer(Topic topic, ServerCnx cnx, long producerId, String producerName, String appId,
                    boolean isEncrypted, Map<String, String> metadata, SchemaVersion schemaVersion, long epoch,
                    boolean userProvidedProducerName) {
        this.topic = topic;
        this.cnx = cnx;
        this.producerId = producerId;
        this.producerName = checkNotNull(producerName);
        this.userProvidedProducerName = userProvidedProducerName;
        this.epoch = epoch;
        this.closeFuture = new CompletableFuture<>();
        this.appId = appId;
        this.authenticationData = cnx.getAuthenticationData();
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
        beforePublish(producerId, sequenceId, headersAndPayload, batchSize);
        publishMessageToTopic(headersAndPayload, sequenceId, batchSize);
    }

    public void publishMessage(long producerId, long lowestSequenceId, long highestSequenceId,
                               ByteBuf headersAndPayload, long batchSize) {
        if (lowestSequenceId > highestSequenceId) {
            sendError(producerId, highestSequenceId, PulsarApi.ServerError.MetadataError, "Invalid lowest or highest sequence id");
            cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
            return;
        }
        beforePublish(producerId, highestSequenceId, headersAndPayload, batchSize);
        publishMessageToTopic(headersAndPayload, lowestSequenceId, highestSequenceId, batchSize);
    }

    public void beforePublish(long producerId, long sequenceId, ByteBuf headersAndPayload, long batchSize) {
        if (isClosed) {
            sendError(producerId, sequenceId, PulsarApi.ServerError.PersistenceError, "Producer is closed");
            cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
            return;
        }

        if (!verifyChecksum(headersAndPayload)) {
            sendError(producerId, sequenceId, PulsarApi.ServerError.ChecksumError, "Checksum failed on the broker");
            cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
            return;
        }

        if (topic.isEncryptionRequired()) {

            headersAndPayload.markReaderIndex();
            PulsarApi.MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
            headersAndPayload.resetReaderIndex();

            // Check whether the message is encrypted or not
            if (msgMetadata.getEncryptionKeysCount() < 1) {
                log.warn("[{}] Messages must be encrypted", getTopic().getName());
                sendError(producerId, sequenceId, PulsarApi.ServerError.MetadataError, "Messages must be encrypted");
                cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
                return;
            }
        }

        startPublishOperation((int) batchSize, headersAndPayload.readableBytes());
    }

    abstract protected void sendError(long producerId, long sequenceId, PulsarApi.ServerError serverError, String message);

    protected void publishMessageToTopic(ByteBuf headersAndPayload, long sequenceId, long batchSize) {
        topic.publishMessage(headersAndPayload,
                MessagePublishContext.get(this, sequenceId, msgIn, headersAndPayload.readableBytes(), batchSize,
                        System.nanoTime()));
    }

    protected void publishMessageToTopic(ByteBuf headersAndPayload, long lowestSequenceId, long highestSequenceId, long batchSize) {
        topic.publishMessage(headersAndPayload,
                MessagePublishContext.get(this, lowestSequenceId, highestSequenceId, msgIn, headersAndPayload.readableBytes(), batchSize,
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

    abstract protected void execute(Runnable runnable);

    abstract protected void sendReceipt(long sequenceId, long highestSequenceId, long ledgerId, long entryId);

    protected void startPublishOperation(int batchSize, long msgSize) {
        // A single thread is incrementing/decrementing this counter, so we can use lazySet which doesn't involve a mem
        // barrier
        pendingPublishAcksUpdater.lazySet(this, pendingPublishAcks + 1);
        // increment publish-count
        this.getTopic().incrementPublishCount(batchSize, msgSize);
    }

    protected void publishOperationCompleted() {
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

    public ServerCnx getCnx() {
        return this.cnx;
    }

    private static final class MessagePublishContext implements Topic.PublishContext, Runnable {
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

        private long highestSequenceId;
        private long originalHighestSequenceId;

        public String getProducerName() {
            return producer.getProducerName();
        }

        public long getSequenceId() {
            return sequenceId;
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
         * Executed from managed ledger thread when the message is persisted
         */
        @Override
        public void completed(Exception exception, long ledgerId, long entryId) {
            if (exception != null) {
                PulsarApi.ServerError serverError = (exception instanceof BrokerServiceException.TopicTerminatedException)
                        ? PulsarApi.ServerError.TopicTerminatedError : PulsarApi.ServerError.PersistenceError;

                producer.execute(() -> {
                    if (!(exception instanceof BrokerServiceException.TopicClosedException)) {
                        // For TopicClosed exception there's no need to send explicit error, since the client was
                        // already notified
                        long callBackSequenceId = Math.max(highestSequenceId, sequenceId);
                        producer.sendError(producer.producerId, callBackSequenceId,
                                serverError, exception.getMessage());
                    }
                    producer.cnx.completedSendOperation(producer.isNonPersistentTopic, msgSize);
                    producer.publishOperationCompleted();
                });
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] [{}] triggered send callback. cnx {}, sequenceId {}", producer.topic,
                            producer.producerName, producer.producerId, producer.cnx.clientAddress(), sequenceId);
                }

                this.ledgerId = ledgerId;
                this.entryId = entryId;
                producer.execute(this);
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
            producer.topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.NANOSECONDS);
            producer.sendReceipt(sequenceId, highestSequenceId, ledgerId, entryId);
            producer.cnx.completedSendOperation(producer.isNonPersistentTopic, msgSize);
            producer.publishOperationCompleted();
            recycle();
        }

        static Producer.MessagePublishContext get(Producer producer, long sequenceId, Rate rateIn, int msgSize,
                                                  long batchSize, long startTimeNs) {
            Producer.MessagePublishContext callback = RECYCLER.get();
            callback.producer = producer;
            callback.sequenceId = sequenceId;
            callback.rateIn = rateIn;
            callback.msgSize = msgSize;
            callback.batchSize = batchSize;
            callback.originalProducerName = null;
            callback.originalSequenceId = -1L;
            callback.startTimeNs = startTimeNs;
            return callback;
        }

        static Producer.MessagePublishContext get(Producer producer, long lowestSequenceId, long highestSequenceId, Rate rateIn,
                                                  int msgSize, long batchSize, long startTimeNs) {
            Producer.MessagePublishContext callback = RECYCLER.get();
            callback.producer = producer;
            callback.sequenceId = lowestSequenceId;
            callback.highestSequenceId = highestSequenceId;
            callback.rateIn = rateIn;
            callback.msgSize = msgSize;
            callback.batchSize = batchSize;
            callback.originalProducerName = null;
            callback.originalSequenceId = -1L;
            callback.startTimeNs = startTimeNs;
            return callback;
        }

        private final Recycler.Handle<Producer.MessagePublishContext> recyclerHandle;

        private MessagePublishContext(Recycler.Handle<Producer.MessagePublishContext> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<Producer.MessagePublishContext> RECYCLER = new Recycler<Producer.MessagePublishContext>() {
            protected Producer.MessagePublishContext newObject(Recycler.Handle<Producer.MessagePublishContext> handle) {
                return new Producer.MessagePublishContext(handle);
            }
        };

        public void recycle() {
            producer = null;
            sequenceId = -1L;
            highestSequenceId = -1L;
            originalSequenceId = -1L;
            originalHighestSequenceId = -1L;
            rateIn = null;
            msgSize = 0;
            ledgerId = -1L;
            entryId = -1L;
            batchSize = 0L;
            startTimeNs = -1L;
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

    void closeNow(boolean removeFromTopic) {
        if (removeFromTopic) {
            topic.removeProducer(this);
        }
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
            execute(() -> {
                cnx.closeProducer(this);
                closeNow(true);
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

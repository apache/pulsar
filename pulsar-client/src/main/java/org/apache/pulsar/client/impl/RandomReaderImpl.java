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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.MessageInvisibleReason;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RandomReadResult;
import org.apache.pulsar.client.api.RandomReader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.RandomReaderConfigurationData;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.util.TimedCompletableFuture;
import org.apache.pulsar.common.api.proto.CommandRandomReadEntryResult;
import org.apache.pulsar.common.api.proto.CommandRandomReadMessage;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.RandomReadInvisibleReason;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;

public class RandomReaderImpl<T> extends HandlerState implements RandomReader<T> {

    private final RandomReaderConfigurationData<T> conf;
    private final Schema<T> schema;
    private final boolean partitionedTopic;
    private final ConcurrentHashMap<Integer, RandomReaderSession<T>> sessions = new ConcurrentHashMap<>();

    static <T> CompletableFuture<RandomReader<T>> create(PulsarClientImpl client,
                                                         RandomReaderConfigurationData<T> conf,
                                                         Schema<T> schema) {
        return client.getPartitionedTopicMetadata(conf.getTopicName(), false, false)
                .thenApply(metadata -> {
                    RandomReaderImpl<T> reader = new RandomReaderImpl<>(client, conf, schema,
                            metadata.partitions > 0);
                    reader.changeToReadyState();
                    return reader;
                });
    }

    private RandomReaderImpl(PulsarClientImpl client, RandomReaderConfigurationData<T> conf, Schema<T> schema,
                             boolean partitionedTopic) {
        super(client, conf.getTopicName());
        this.conf = conf;
        this.schema = schema;
        this.partitionedTopic = partitionedTopic;
    }

    boolean isOpen() {
        return getState() == State.Ready;
    }

    boolean isClosed() {
        return getState() == State.Closed || getState() == State.Closing || getState() == State.Failed;
    }

    @Override
    public CompletableFuture<List<RandomReadResult<T>>> read(MessageId startPosition, int numberOfBatches) {
        if (!isOpen()) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException(
                    "RandomReader is not ready or already closed"));
        }
        if (numberOfBatches <= 0) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "numberOfBatches must be positive"));
        }
        MessageIdAdv messageId;
        try {
            messageId = unwrapAndValidate(startPosition);
        } catch (RuntimeException e) {
            return FutureUtil.failedFuture(e);
        }
        return getSession(messageId.getPartitionIndex())
                .thenCompose(session -> session.read(messageId, numberOfBatches));
    }

    private MessageIdAdv unwrapAndValidate(MessageId startPosition) {
        if (startPosition == MessageId.earliest || startPosition == MessageId.latest) {
            throw new IllegalArgumentException("RandomReader requires a concrete message id");
        }
        if (!(startPosition instanceof MessageIdAdv messageId)) {
            throw new IllegalArgumentException("RandomReader requires a concrete Pulsar message id");
        }
        if (messageId.getLedgerId() < 0 || messageId.getEntryId() < 0) {
            throw new IllegalArgumentException("RandomReader requires non-negative ledger and entry ids");
        }
        if (messageId instanceof BatchMessageIdImpl && messageId.getBatchIndex() >= 0) {
            throw new IllegalArgumentException("RandomReader does not support batch-index message ids");
        }
        int partition = messageId.getPartitionIndex();
        if (partitionedTopic && partition < 0) {
            throw new IllegalArgumentException("Partitioned topics require a message id with a partition");
        }
        return messageId;
    }

    private CompletableFuture<RandomReaderSession<T>> getSession(int partitionIndex) {
        int concretePartition = partitionedTopic ? partitionIndex : -1;
        return sessions.computeIfAbsent(concretePartition, this::newSession).connect();
    }

    private RandomReaderSession<T> newSession(int concretePartition) {
        String sessionTopic = concretePartition >= 0
                ? TopicName.get(conf.getTopicName()).getPartition(concretePartition).toString()
                : conf.getTopicName();
        return new RandomReaderSession<>(this, sessionTopic, concretePartition);
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        setState(State.Closing);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        sessions.values().forEach(s -> futures.add(s.closeAsync()));
        return FutureUtil.waitForAll(futures).thenRun(() -> setState(State.Closed));
    }

    @Override
    public String getTopic() {
        return conf.getTopicName();
    }

    @Override
    String getHandlerName() {
        return "random-reader";
    }

    Schema<T> schema() {
        return schema;
    }

    RandomReaderConfigurationData<T> conf() {
        return conf;
    }

    // ---- Inner classes ----

    static final class RandomReaderSession<T> extends HandlerState implements ConnectionHandler.Connection {

        @Override
        String getHandlerName() {
            return "random-reader-session";
        }

        private final long randomReaderId;
        private final int partitionIndex;
        private final String sessionTopic;
        private final RandomReaderImpl<T> parent;
        private final ConnectionHandler connectionHandler;
        private final AtomicReference<CompletableFuture<RandomReaderSession<T>>> connectFuture =
                new AtomicReference<>(new CompletableFuture<>());
        private final AtomicReference<PendingRandomRead<T>> pendingRead = new AtomicReference<>();
        private final AtomicInteger previousExceptionCount = new AtomicInteger();
        private volatile long connectDeadline;
        private volatile boolean createdOnce;

        RandomReaderSession(RandomReaderImpl<T> parent, String sessionTopic, int partitionIndex) {
            super(parent.client, sessionTopic);
            this.parent = parent;
            this.randomReaderId = parent.client.newRandomReaderId();
            this.partitionIndex = partitionIndex;
            this.sessionTopic = sessionTopic;
            this.connectionHandler = new ConnectionHandler(this, newBackoff(), this);
        }

        private Backoff newBackoff() {
            return Backoff.builder()
                    .initialDelay(Duration.ofNanos(parent.client.getConfiguration()
                            .getInitialBackoffIntervalNanos()))
                    .maxBackoff(Duration.ofNanos(parent.client.getConfiguration()
                            .getMaxBackoffIntervalNanos()))
                    .build();
        }

        CompletableFuture<RandomReaderSession<T>> connect() {
            if (getState() == State.Ready) {
                return CompletableFuture.completedFuture(this);
            }
            if (getState() == State.Connecting) {
                return connectFuture.get();
            }
            CompletableFuture<RandomReaderSession<T>> nextConnectFuture = newConnectFuture();
            if (!changeToConnecting()) {
                nextConnectFuture.completeExceptionally(
                        new PulsarClientException("RandomReader session is closed"));
                return nextConnectFuture;
            }
            connectionHandler.grabCnx();
            return nextConnectFuture;
        }

        private CompletableFuture<RandomReaderSession<T>> newConnectFuture() {
            CompletableFuture<RandomReaderSession<T>> next = new CompletableFuture<>();
            connectFuture.set(next);
            connectDeadline = System.currentTimeMillis() + parent.client.getConfiguration().getOperationTimeoutMs();
            return next;
        }

        @Override
        public CompletableFuture<Void> connectionOpened(ClientCnx clientCnx) {
            if (clientCnx.getRemoteEndpointProtocolVersion() < ProtocolVersion.v22.getValue()) {
                return FutureUtil.failedFuture(new PulsarClientException.NotSupportedException(
                        "Broker does not support RandomReader protocol"));
            }
            if (!changeToConnecting()) {
                clientCnx.removeRandomReader(randomReaderId);
                return CompletableFuture.completedFuture(null);
            }
            connectionHandler.setClientCnx(clientCnx);
            clientCnx.registerRandomReader(randomReaderId, this);

            SchemaInfo schemaInfo = parent.schema().getSchemaInfo();
            if (schemaInfo != null && (SchemaType.BYTES == schemaInfo.getType()
                    || SchemaType.NONE == schemaInfo.getType())) {
                schemaInfo = null;
            } else if (parent.schema() instanceof AutoConsumeSchema
                    && Commands.peerSupportsCarryAutoConsumeSchemaToBroker(
                            clientCnx.getRemoteEndpointProtocolVersion())) {
                schemaInfo = AutoConsumeSchema.SCHEMA_INFO;
            }

            long requestId = client.newRequestId();
            ByteBuf command = Commands.newRandomReader(sessionTopic, randomReaderId, requestId,
                    parent.conf.getReaderName(), schemaInfo, parent.conf.getProperties(),
                    parent.conf.isReadCommitted());
            return clientCnx.sendRandomReaderCreate(command, requestId).thenAccept(success -> {
                if (changeToReadyState()) {
                    createdOnce = true;
                    connectionHandler.resetBackoff();
                    connectFuture.get().complete(this);
                }
            }).exceptionally(ex -> {
                clientCnx.removeRandomReader(randomReaderId);
                connectionHandler.setClientCnx(null);
                throw FutureUtil.wrapToCompletionException(ex);
            });
        }

        @Override
        public boolean connectionFailed(PulsarClientException exception) {
            boolean nonRetriable = !PulsarClientException.isRetriableError(exception);
            boolean initialCreateTimedOut = !createdOnce && System.currentTimeMillis() > connectDeadline;
            if (nonRetriable || initialCreateTimedOut) {
                exception.setPreviousExceptionCount(previousExceptionCount);
                connectFuture.get().completeExceptionally(exception);
                ClientCnx current = connectionHandler.cnx();
                if (current != null) {
                    current.removeRandomReader(randomReaderId);
                    connectionHandler.setClientCnx(null);
                }
                setState(State.Failed);
                return false;
            }
            previousExceptionCount.incrementAndGet();
            return true;
        }

        void connectionClosed(ClientCnx cnx, Optional<Long> initialDelay, Optional<URI> hostUrl) {
            newConnectFuture();
            PendingRandomRead<T> pending = pendingRead.get();
            if (pending != null) {
                pending.fail(new PulsarClientException("Connection closed"));
            }
            connectionHandler.connectionClosed(cnx, initialDelay, hostUrl);
        }

        CompletableFuture<List<RandomReadResult<T>>> read(MessageIdAdv startPosition, int numberOfBatches) {
            CompletableFuture<List<RandomReadResult<T>>> future = new CompletableFuture<>();
            long deadline = System.currentTimeMillis()
                    + parent.client.getConfiguration().getOperationTimeoutMs();
            doRead(startPosition, numberOfBatches, future, deadline, newBackoff());
            return future;
        }

        private void doRead(MessageIdAdv startPosition, int numberOfBatches,
                            CompletableFuture<List<RandomReadResult<T>>> resultFuture, long deadline,
                            Backoff readBackoff) {
            if (isTerminalState()) {
                resultFuture.completeExceptionally(
                        new PulsarClientException.AlreadyClosedException("RandomReader session is closed"));
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                resultFuture.completeExceptionally(
                        new PulsarClientException("RandomReader read operation timed out"));
                return;
            }
            ClientCnx current = connectionHandler.cnx();
            if (current == null || getState() != State.Ready) {
                client.timer().newTimeout(
                        __ -> doRead(startPosition, numberOfBatches, resultFuture, deadline, readBackoff),
                        100, TimeUnit.MILLISECONDS);
                return;
            }
            PendingRandomRead<T> pending = new PendingRandomRead<>(startPosition, numberOfBatches, this);
            if (!pendingRead.compareAndSet(null, pending)) {
                resultFuture.completeExceptionally(new PulsarClientException.TooManyRequestsException(
                        "RandomReader already has a read in flight"));
                return;
            }
            long requestId = client.newRequestId();
            pending.requestId = requestId;
            ByteBuf command = Commands.newRandomRead(randomReaderId, requestId,
                    pending.startPosition.getLedgerId(), pending.startPosition.getEntryId(),
                    partitionIndex, pending.numberOfBatches);
            current.registerPendingReadTimeout(requestId, pending.getFuture());
            current.ctx().writeAndFlush(command).addListener(writeFuture -> {
                if (!writeFuture.isSuccess()) {
                    failPendingRead(requestId, writeFuture.cause());
                }
            });
            pending.future.whenComplete((result, ex) -> {
                current.removePendingReadTimeout(requestId, pending.getFuture());
                if (ex == null) {
                    readBackoff.reset();
                    resultFuture.complete(result);
                    return;
                }
                pending.releaseVisibleMessages();
                if (!isRetriableForRead(ex) || System.currentTimeMillis() > deadline) {
                    resultFuture.completeExceptionally(ex);
                    return;
                }
                client.timer().newTimeout(
                        __ -> doRead(startPosition, numberOfBatches, resultFuture, deadline, readBackoff),
                        readBackoff.next().toMillis(), TimeUnit.MILLISECONDS);
            });
        }

        private boolean isTerminalState() {
            return getState() == State.Closed || getState() == State.Closing || getState() == State.Failed;
        }

        private static boolean isRetriableForRead(Throwable ex) {
            return PulsarClientException.isRetriableError(ex)
                    && !(ex instanceof PulsarClientException.BrokerMetadataException)
                    && !(ex instanceof PulsarClientException.AlreadyClosedException);
        }

        void messageReceived(CommandRandomReadMessage command, ByteBuf headersAndPayload, ClientCnx cnx) {
            PendingRandomRead<T> pending = pendingRead.get();
            if (pending != null && pending.requestId == command.getRequestId()) {
                pending.messageReceived(command, headersAndPayload, cnx);
            }
        }

        void entryResultReceived(CommandRandomReadEntryResult command) {
            PendingRandomRead<T> pending = pendingRead.get();
            if (pending != null && pending.requestId == command.getRequestId()) {
                pending.entryResultReceived(command);
            }
        }

        boolean hasPendingRead(long requestId) {
            PendingRandomRead<T> pending = pendingRead.get();
            return pending != null && pending.requestId == requestId;
        }

        void completePendingRead(long requestId, int expectedResultCount) {
            PendingRandomRead<T> pending = pendingRead.get();
            if (pending != null && pending.requestId == requestId) {
                pending.complete(expectedResultCount);
            }
        }

        void failPendingRead(long requestId, Throwable cause) {
            PendingRandomRead<T> pending = pendingRead.get();
            if (pending != null && pending.requestId == requestId) {
                pending.fail(cause);
            }
        }

        void failPendingRead(Throwable cause) {
            PendingRandomRead<T> pending = pendingRead.get();
            if (pending != null) {
                pending.fail(cause);
            }
        }

        CompletableFuture<Void> closeAsync() {
            setState(State.Closing);
            PendingRandomRead<T> pending = pendingRead.get();
            if (pending != null) {
                pending.fail(new PulsarClientException.AlreadyClosedException(
                        "RandomReader session is closed"));
            }
            ClientCnx current = connectionHandler.cnx();
            if (current != null) {
                long requestId = client.newRequestId();
                ByteBuf cmd = Commands.newCloseRandomReader(randomReaderId, requestId);
                current.removeRandomReader(randomReaderId);
                connectionHandler.setClientCnx(null);
                current.ctx().writeAndFlush(cmd);
            }
            setState(State.Closed);
            return CompletableFuture.completedFuture(null);
        }
    }

    static final class PendingRandomRead<T> {
        private final MessageIdAdv startPosition;
        private final int numberOfBatches;
        final RandomReaderSession<T> session;
        private final TimedCompletableFuture<List<RandomReadResult<T>>> future = new TimedCompletableFuture<>();
        private final List<RandomReadResult<T>> results = new ArrayList<>();
        volatile long requestId;

        PendingRandomRead(MessageIdAdv startPosition, int numberOfBatches,
                          RandomReaderSession<T> session) {
            this.startPosition = startPosition;
            this.numberOfBatches = numberOfBatches;
            this.session = session;
            this.future.whenComplete((result, ex) -> {
                session.pendingRead.compareAndSet(this, null);
            });
        }

        TimedCompletableFuture<List<RandomReadResult<T>>> getFuture() {
            return future;
        }

        void complete(int expectedResultCount) {
            if (results.size() != expectedResultCount) {
                future.completeExceptionally(new PulsarClientException.InvalidMessageException(
                        "RandomReader response result count mismatch for request " + requestId
                                + ": expected " + expectedResultCount + " entries but received "
                                + results.size()));
                return;
            }
            future.complete(List.copyOf(results));
        }

        void fail(Throwable cause) {
            future.completeExceptionally(cause);
        }

        void releaseVisibleMessages() {
            for (RandomReadResult<T> result : results) {
                if (!result.isVisible()) {
                    continue;
                }
                try {
                    result.getMessages().forEach(Message::release);
                } catch (PulsarClientException.MessageInvisibleException ignored) {
                    // Guarded by isVisible().
                }
            }
            results.clear();
        }

        void messageReceived(CommandRandomReadMessage command, ByteBuf headersAndPayload, ClientCnx cnx) {
            try {
                List<Message<T>> decoded = decodeEntry(command, headersAndPayload, session.parent.schema(),
                        session.parent.conf(), session.sessionTopic, session.partitionIndex);
                results.add(RandomReadResultImpl.visible(toMessageId(command.getMessageId(),
                        session.partitionIndex), decoded));
            } catch (Exception e) {
                session.failPendingRead(command.getRequestId(), toInvalidMessageException(e));
            }
        }

        private static PulsarClientException toInvalidMessageException(Exception e) {
            if (e instanceof PulsarClientException.InvalidMessageException invalidMessageException) {
                return invalidMessageException;
            }
            String message = e.getMessage();
            return new PulsarClientException.InvalidMessageException(
                    "Failed to decode random read entry" + (message == null ? "" : ": " + message));
        }

        void entryResultReceived(CommandRandomReadEntryResult command) {
            results.add(RandomReadResultImpl.invisible(toMessageId(command.getMessageId(), session.partitionIndex),
                    toClientReason(command.getInvisibleReason())));
        }

        private static MessageIdImpl toMessageId(org.apache.pulsar.common.api.proto.MessageIdData messageId,
                                                 int fallbackPartition) {
            int partition = messageId.hasPartition() ? messageId.getPartition() : fallbackPartition;
            return new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), partition);
        }

        private static MessageInvisibleReason toClientReason(RandomReadInvisibleReason reason) {
            if (reason == null) {
                return MessageInvisibleReason.UNKNOWN;
            }
            switch (reason) {
                case SERVER_ONLY_MARKER:
                    return MessageInvisibleReason.SERVER_ONLY_MARKER;
                case TRANSACTION_MARKER:
                    return MessageInvisibleReason.TRANSACTION_MARKER;
                case ABORTED_TRANSACTION:
                    return MessageInvisibleReason.ABORTED_TRANSACTION;
                case DELAYED_DELIVERY:
                    return MessageInvisibleReason.DELAYED_DELIVERY;
                case EXCEEDED_MAX_VISIBLE_POSITION:
                    return MessageInvisibleReason.EXCEEDED_MAX_VISIBLE_POSITION;
                case UNKNOWN:
                default:
                    return MessageInvisibleReason.UNKNOWN;
            }
        }

        private static <T> List<Message<T>> decodeEntry(CommandRandomReadMessage command, ByteBuf headersAndPayload,
                                                         Schema<T> schema, RandomReaderConfigurationData<T> conf,
                                                         String topicName, int partitionIndex)
                throws PulsarClientException {
            headersAndPayload.markReaderIndex();
            Commands.skipChecksumIfPresent(headersAndPayload);
            MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);

            MessageIdImpl entryMsgId = new MessageIdImpl(command.getMessageId().getLedgerId(),
                    command.getMessageId().getEntryId(), partitionIndex);

            int numMessages = msgMetadata.getNumMessagesInBatch();
            List<Message<T>> result = new ArrayList<>(Math.max(numMessages, 1));

            try {
                if (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch()) {
                    MessageImpl<T> message = MessageImpl.create(topicName, entryMsgId,
                            msgMetadata, headersAndPayload, Optional.empty(), null, schema,
                            0, conf.isPoolMessages(), Commands.DEFAULT_CONSUMER_EPOCH);
                    result.add(message);
                } else {
                    for (int i = 0; i < numMessages; i++) {
                        SingleMessageMetadata singleMetadata = new SingleMessageMetadata();
                        ByteBuf singlePayload;
                        try {
                            singlePayload = Commands.deSerializeSingleMessageInBatch(
                                    headersAndPayload, singleMetadata, i, numMessages);
                        } catch (IOException e) {
                            throw new PulsarClientException(e);
                        }
                        try {
                            BatchMessageIdImpl batchMsgId = new BatchMessageIdImpl(
                                    command.getMessageId().getLedgerId(),
                                    command.getMessageId().getEntryId(),
                                    partitionIndex, i, numMessages, null);
                            MessageImpl<T> message = MessageImpl.create(topicName, batchMsgId,
                                    msgMetadata, singleMetadata, singlePayload,
                                    Optional.empty(), null, schema, 0,
                                    conf.isPoolMessages(), Commands.DEFAULT_CONSUMER_EPOCH);
                            result.add(message);
                        } finally {
                            singlePayload.release();
                        }
                    }
                }
            } catch (PulsarClientException | RuntimeException | Error e) {
                result.forEach(Message::release);
                throw e;
            }
            return result;
        }
    }
}

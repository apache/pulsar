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

import com.google.re2j.Pattern;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandWatchTopicUpdate;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.BackoffBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicListWatcher extends HandlerState implements ConnectionHandler.Connection {

    private static final Logger log = LoggerFactory.getLogger(TopicListWatcher.class);

    private static final AtomicLongFieldUpdater<TopicListWatcher> CREATE_WATCHER_DEADLINE_UPDATER =
            AtomicLongFieldUpdater
                    .newUpdater(TopicListWatcher.class, "createWatcherDeadline");

    private final PatternConsumerUpdateQueue patternConsumerUpdateQueue;
    private final String name;
    private final ConnectionHandler connectionHandler;
    private final Pattern topicsPattern;
    private final long watcherId;
    private volatile long createWatcherDeadline = 0;
    private final NamespaceName namespace;
    // TODO maintain the value based on updates from broker and warn the user if inconsistent with hash from polling
    private String topicsHash;
    private final CompletableFuture<TopicListWatcher> watcherFuture;

    private final AtomicInteger previousExceptionCount = new AtomicInteger();
    private final AtomicReference<ClientCnx> clientCnxUsedForWatcherRegistration = new AtomicReference<>();

    private final Runnable recheckTopicsChangeAfterReconnect;


    /***
     * @param topicsPattern The regexp for the topic name(not contains partition suffix).
     */
    public TopicListWatcher(PatternConsumerUpdateQueue patternConsumerUpdateQueue,
                            PulsarClientImpl client, Pattern topicsPattern, long watcherId,
                            NamespaceName namespace, String topicsHash,
                            CompletableFuture<TopicListWatcher> watcherFuture,
                            Runnable recheckTopicsChangeAfterReconnect) {
        super(client, topicsPattern.pattern());
        this.patternConsumerUpdateQueue = patternConsumerUpdateQueue;
        this.name = "Watcher(" + topicsPattern + ")";
        this.connectionHandler = new ConnectionHandler(this,
                new BackoffBuilder()
                        .setInitialTime(client.getConfiguration().getInitialBackoffIntervalNanos(),
                                TimeUnit.NANOSECONDS)
                        .setMax(client.getConfiguration().getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                        .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                        .create(),
                this);
        this.topicsPattern = topicsPattern;
        this.watcherId = watcherId;
        this.namespace = namespace;
        this.topicsHash = topicsHash;
        this.watcherFuture = watcherFuture;
        this.recheckTopicsChangeAfterReconnect = recheckTopicsChangeAfterReconnect;

        connectionHandler.grabCnx();
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        boolean nonRetriableError = !PulsarClientException.isRetriableError(exception);
        if (nonRetriableError) {
            exception.setPreviousExceptionCount(previousExceptionCount);
            if (watcherFuture.completeExceptionally(exception)) {
                setState(State.Failed);
                log.info("[{}] Watcher creation failed for {} with non-retriable error {}",
                        topic, name, exception.getMessage());
                deregisterFromClientCnx();
            }
        } else {
            previousExceptionCount.incrementAndGet();
        }
    }

    @Override
    public CompletableFuture<Void> connectionOpened(ClientCnx cnx) {
        previousExceptionCount.set(0);

        State state = getState();
        if (state == State.Closing || state == State.Closed) {
            setState(State.Closed);
            deregisterFromClientCnx();
            return CompletableFuture.completedFuture(null);
        }

        log.info("[{}][{}] Creating topic list watcher on cnx {}, watcherId {}",
                topic, getHandlerName(), cnx.ctx().channel(), watcherId);

        long requestId = client.newRequestId();

        CREATE_WATCHER_DEADLINE_UPDATER
                .compareAndSet(this, 0L, System.currentTimeMillis()
                        + client.getConfiguration().getOperationTimeoutMs());

        final CompletableFuture<Void> future = new CompletableFuture<>();
        // synchronized this, because redeliverUnAckMessage eliminate the epoch inconsistency between them
        synchronized (this) {
            setClientCnx(cnx);
            BaseCommand watchRequest = Commands.newWatchTopicList(requestId, watcherId, namespace.toString(),
                            topicsPattern.pattern(), topicsHash);

            cnx.newWatchTopicList(watchRequest, requestId)

                    .thenAccept(response -> {
                        synchronized (TopicListWatcher.this) {
                            if (!changeToReadyState()) {
                                // Watcher was closed while reconnecting, close the connection to make sure the broker
                                // drops the watcher on its side
                                setState(State.Closed);
                                deregisterFromClientCnx();
                                cnx.channel().close();
                                future.complete(null);
                                return;
                            }
                        }
                        this.connectionHandler.resetBackoff();

                        recheckTopicsChangeAfterReconnect.run();
                        watcherFuture.complete(this);
                        future.complete(null);
                    }).exceptionally((e) -> {
                        deregisterFromClientCnx();
                        if (getState() == State.Closing || getState() == State.Closed) {
                            // Watcher was closed while reconnecting, close the connection to make sure the broker
                            // drops the watcher on its side
                            cnx.channel().close();
                            future.complete(null);
                            return null;
                        }
                        log.warn("[{}][{}] Failed to create topic list watcher on {}",
                                topic, getHandlerName(), cnx.channel().remoteAddress());

                        if (e.getCause() instanceof PulsarClientException
                                && PulsarClientException.isRetriableError(e.getCause())
                                && System.currentTimeMillis()
                                    < CREATE_WATCHER_DEADLINE_UPDATER.get(TopicListWatcher.this)) {
                            future.completeExceptionally(e.getCause());
                        } else if (!watcherFuture.isDone()) {
                            // unable to create new watcher, fail operation
                            setState(State.Failed);
                            watcherFuture.completeExceptionally(
                                    PulsarClientException.wrap(e, String.format("Failed to create topic list watcher %s"
                                                    + "when connecting to the broker", getHandlerName())));
                        } else {
                            // watcher was subscribed and connected, but we got some error, keep trying
                            future.completeExceptionally(e.getCause());
                        }
                        if (!future.isDone()) {
                            future.complete(null);
                        }
                        return null;
                    });
        }
        return future;
    }

    @Override
    String getHandlerName() {
        return name;
    }

    public boolean isConnected() {
        return getClientCnx() != null && (getState() == State.Ready);
    }

    public ClientCnx getClientCnx() {
        return this.connectionHandler.cnx();
    }

    public CompletableFuture<Void> closeAsync() {

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        if (getState() == State.Closing || getState() == State.Closed) {
            closeFuture.complete(null);
            return closeFuture;
        }

        if (!isConnected()) {
            log.info("[{}] [{}] Closed watcher (not connected)", topic, getHandlerName());
            setState(State.Closed);
            deregisterFromClientCnx();
            closeFuture.complete(null);
            return closeFuture;
        }

        setState(State.Closing);


        long requestId = client.newRequestId();

        ClientCnx cnx = cnx();
        if (null == cnx) {
            cleanupAtClose(closeFuture, null);
        } else {
            BaseCommand cmd = Commands.newWatchTopicListClose(watcherId, requestId);
            cnx.newWatchTopicListClose(cmd, requestId).handle((v, exception) -> {
                final ChannelHandlerContext ctx = cnx.ctx();
                boolean ignoreException = ctx == null || !ctx.channel().isActive();
                if (ignoreException && exception != null) {
                    log.debug("Exception ignored in closing watcher", exception);
                }
                cleanupAtClose(closeFuture, ignoreException ? null : exception);
                return null;
            });
        }

        return closeFuture;
    }

    // wrapper for connection methods
    ClientCnx cnx() {
        return this.connectionHandler.cnx();
    }

    public void connectionClosed(ClientCnx clientCnx) {
        this.connectionHandler.connectionClosed(clientCnx);
    }

    void setClientCnx(ClientCnx clientCnx) {
        if (clientCnx != null) {
            this.connectionHandler.setClientCnx(clientCnx);
            clientCnx.registerTopicListWatcher(watcherId, this);
        }
        ClientCnx previousClientCnx = clientCnxUsedForWatcherRegistration.getAndSet(clientCnx);
        if (previousClientCnx != null && previousClientCnx != clientCnx) {
            previousClientCnx.removeTopicListWatcher(watcherId);
        }
    }

    void deregisterFromClientCnx() {
        setClientCnx(null);
    }

    private void cleanupAtClose(CompletableFuture<Void> closeFuture, Throwable exception) {
        log.info("[{}] Closed topic list watcher", getHandlerName());
        setState(State.Closed);
        deregisterFromClientCnx();
        if (exception != null) {
            closeFuture.completeExceptionally(exception);
        } else {
            closeFuture.complete(null);
        }
    }

    public void handleCommandWatchTopicUpdate(CommandWatchTopicUpdate update) {
        patternConsumerUpdateQueue.appendTopicsRemovedOp(update.getDeletedTopicsList());
        patternConsumerUpdateQueue.appendTopicsAddedOp(update.getNewTopicsList());
    }
}

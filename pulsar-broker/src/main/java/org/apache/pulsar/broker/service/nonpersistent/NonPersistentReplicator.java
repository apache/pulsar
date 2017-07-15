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
package org.apache.pulsar.broker.service.nonpersistent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.util.Rate;
import org.apache.pulsar.broker.service.Replicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.SendCallback;
import org.apache.pulsar.common.policies.data.ReplicatorStats;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

public class NonPersistentReplicator implements Replicator {

    private final BrokerService brokerService;
    private final NonPersistentTopic topic;
    private final String topicName;
    private final String localCluster;
    private final String remoteCluster;
    private final PulsarClientImpl client;

    private volatile ProducerImpl producer;

    private final Rate msgOut = new Rate();
    private final Rate msgExpired = new Rate();

    private static final ProducerConfiguration producerConfiguration = new ProducerConfiguration().setSendTimeout(0,
            TimeUnit.SECONDS);

    private final Backoff backOff = new Backoff(100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES);

    private final ReplicatorStats stats = new ReplicatorStats();

    public NonPersistentReplicator(NonPersistentTopic topic, String localCluster, String remoteCluster,
            BrokerService brokerService) {
        this.brokerService = brokerService;
        this.topic = topic;
        this.topicName = topic.getName();
        this.localCluster = localCluster;
        this.remoteCluster = remoteCluster;
        this.client = (PulsarClientImpl) brokerService.getReplicationClient(remoteCluster);
        this.producer = null;
        STATE_UPDATER.set(this, State.Stopped);

        producerConfiguration
                .setMaxPendingMessages(brokerService.pulsar().getConfiguration().getReplicationProducerQueueSize());
        producerConfiguration.setBlockIfQueueFull(false);

        startProducer();
    }

    public String getRemoteCluster() {
        return remoteCluster;
    }

    enum State {
        Stopped, Starting, Started, Stopping
    }

    private static final AtomicReferenceFieldUpdater<NonPersistentReplicator, State> STATE_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(NonPersistentReplicator.class, State.class, "state");
    private volatile State state = State.Stopped;

    // This method needs to be synchronized with disconnects else if there is a disconnect followed by startProducer
    // the end result can be disconnect.
    public synchronized void startProducer() {
        if (STATE_UPDATER.get(this) == State.Stopping) {
            long waitTimeMs = backOff.next();
            if (log.isDebugEnabled()) {
                log.debug(
                        "[{}][{} -> {}] waiting for producer to close before attempting to reconnect, retrying in {} s",
                        topicName, localCluster, remoteCluster, waitTimeMs / 1000.0);
            }
            // BackOff before retrying
            brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
            return;
        }
        State state = STATE_UPDATER.get(this);
        if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
            if (state == State.Started) {
                // Already running
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Replicator was already running", topicName, localCluster, remoteCluster);
                }
            } else {
                log.info("[{}][{} -> {}] Replicator already being started. Replicator state: {}", topicName,
                        localCluster, remoteCluster, state);
            }

            return;
        }

        log.info("[{}][{} -> {}] Starting replicator", topicName, localCluster, remoteCluster);
        client.createProducerAsync(topicName, producerConfiguration,
                getReplicatorName(topic.replicatorPrefix, localCluster)).thenAccept(producer -> {

                    this.producer = (ProducerImpl) producer;

                    if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Started)) {
                        log.info("[{}][{} -> {}] Created replicator producer", topicName, localCluster, remoteCluster);
                        backOff.reset();
                    } else {
                        log.info(
                                "[{}][{} -> {}] Replicator was stopped while creating the producer. Closing it. Replicator state: {}",
                                topicName, localCluster, remoteCluster, STATE_UPDATER.get(this));
                        STATE_UPDATER.set(this, State.Stopping);
                        closeProducerAsync();
                        return;
                    }
                }).exceptionally(ex -> {
                    if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopped)) {
                        long waitTimeMs = backOff.next();
                        log.warn("[{}][{} -> {}] Failed to create remote producer ({}), retrying in {} s", topicName,
                                localCluster, remoteCluster, ex.getMessage(), waitTimeMs / 1000.0);

                        // BackOff before retrying
                        brokerService.executor().schedule(this::startProducer, waitTimeMs, TimeUnit.MILLISECONDS);
                    } else {
                        log.warn("[{}][{} -> {}] Failed to create remote producer. Replicator state: {}", topicName,
                                localCluster, remoteCluster, STATE_UPDATER.get(this), ex);
                    }
                    return null;
                });

    }

    public void sendMessage(Entry entry) {
        if ((STATE_UPDATER.get(this) == State.Started) && isWritable()) {

            int length = entry.getLength();
            ByteBuf headersAndPayload = entry.getDataBuffer();
            MessageImpl msg;
            try {
                msg = MessageImpl.deserialize(headersAndPayload);
            } catch (Throwable t) {
                log.error("[{}][{} -> {}] Failed to deserialize message at {} (buffer size: {}): {}", topicName,
                        localCluster, remoteCluster, entry.getPosition(), length, t.getMessage(), t);
                entry.release();
                return;
            }

            if (msg.isReplicated()) {
                // Discard messages that were already replicated into this region
                entry.release();
                msg.recycle();
                return;
            }

            if (msg.hasReplicateTo() && !msg.getReplicateTo().contains(remoteCluster)) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Skipping message at {} / msg-id: {}: replicateTo {}", topicName,
                            localCluster, remoteCluster, entry.getPosition(), msg.getMessageId(), msg.getReplicateTo());
                }
                entry.release();
                msg.recycle();
                return;
            }

            msgOut.recordEvent(headersAndPayload.readableBytes());

            msg.setReplicatedFrom(localCluster);

            headersAndPayload.retain();

            producer.sendAsync(msg, ProducerSendCallback.create(this, entry, msg));

        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{} -> {}] dropping message because replicator producer is not started/writable",
                        topicName, localCluster, remoteCluster);
            }
            entry.release();
        }
    }

    private synchronized CompletableFuture<Void> closeProducerAsync() {
        if (producer == null) {
            STATE_UPDATER.set(this, State.Stopped);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = producer.closeAsync();
        future.thenRun(() -> {
            STATE_UPDATER.set(this, State.Stopped);
            this.producer = null;
        }).exceptionally(ex -> {
            long waitTimeMs = backOff.next();
            log.warn(
                    "[{}][{} -> {}] Exception: '{}' occured while trying to close the producer. retrying again in {} s",
                    topicName, localCluster, remoteCluster, ex.getMessage(), waitTimeMs / 1000.0);
            // BackOff before retrying
            brokerService.executor().schedule(this::closeProducerAsync, waitTimeMs, TimeUnit.MILLISECONDS);
            return null;
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        return disconnect(false);
    }

    @Override
    public CompletableFuture<Void> disconnect(boolean failIfHasBacklog) {

        if (STATE_UPDATER.get(this) == State.Stopping) {
            // Do nothing since the all "STATE_UPDATER.set(this, Stopping)" instructions are followed by
            // closeProducerAsync()
            // which will at some point change the state to stopped
            return CompletableFuture.completedFuture(null);
        }

        if (producer != null && (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopping)
                || STATE_UPDATER.compareAndSet(this, State.Started, State.Stopping))) {
            log.info("[{}][{} -> {}] Disconnect replicator", topicName, localCluster, remoteCluster);
            return closeProducerAsync();
        }

        STATE_UPDATER.set(this, State.Stopped);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void updateRates() {
        msgOut.calculateRate();
        msgExpired.calculateRate();
        stats.msgRateOut = msgOut.getRate();
        stats.msgThroughputOut = msgOut.getValueRate();
    }

    @Override
    public ReplicatorStats getStats() {
        stats.connected = producer != null && producer.isConnected();
        stats.replicationDelayInSeconds = getReplicationDelayInSeconds();

        ProducerImpl producer = this.producer;
        if (producer != null) {
            stats.outboundConnection = producer.getConnectionId();
            stats.outboundConnectedSince = producer.getConnectedSince();
        } else {
            stats.outboundConnection = null;
            stats.outboundConnectedSince = null;
        }

        return stats;
    }

    private long getReplicationDelayInSeconds() {
        if (producer != null) {
            return TimeUnit.MILLISECONDS.toSeconds(producer.getDelayInMillis());
        }
        return 0L;
    }

    private boolean isWritable() {
        return this.producer != null && this.producer.isWritable();
    }

    public static void setReplicatorQueueSize(int queueSize) {
        producerConfiguration.setMaxPendingMessages(queueSize);
    }

    public static String getRemoteCluster(String remoteCursor) {
        String[] split = remoteCursor.split("\\.");
        return split[split.length - 1];
    }

    public static String getReplicatorName(String replicatorPrefix, String cluster) {
        return String.format("%s.%s", replicatorPrefix, cluster);
    }

    private static final class ProducerSendCallback implements SendCallback {
        private NonPersistentReplicator replicator;
        private Entry entry;
        private MessageImpl msg;

        @Override
        public void sendComplete(Exception exception) {
            if (exception != null) {
                log.error("[{}][{} -> {}] Error producing on remote broker", replicator.topicName,
                        replicator.localCluster, replicator.remoteCluster, exception);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{} -> {}] Message persisted on remote broker", replicator.topicName,
                            replicator.localCluster, replicator.remoteCluster);
                }
            }
            entry.release();

            recycle();
        }

        private final Handle recyclerHandle;

        private ProducerSendCallback(Handle recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static ProducerSendCallback create(NonPersistentReplicator replicator, Entry entry, MessageImpl msg) {
            ProducerSendCallback sendCallback = RECYCLER.get();
            sendCallback.replicator = replicator;
            sendCallback.entry = entry;
            sendCallback.msg = msg;
            return sendCallback;
        }

        private void recycle() {
            replicator = null;
            entry = null; // already released and recycled on sendComplete
            if (msg != null) {
                msg.recycle();
                msg = null;
            }
            RECYCLER.recycle(this, recyclerHandle);
        }

        private static final Recycler<ProducerSendCallback> RECYCLER = new Recycler<ProducerSendCallback>() {
            @Override
            protected ProducerSendCallback newObject(Handle handle) {
                return new ProducerSendCallback(handle);
            }

        };

        @Override
        public void addCallback(SendCallback scb) {
            // noop
        }

        @Override
        public SendCallback getNextSendCallback() {
            return null;
        }

        @Override
        public CompletableFuture<MessageId> getFuture() {
            return null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentReplicator.class);
}

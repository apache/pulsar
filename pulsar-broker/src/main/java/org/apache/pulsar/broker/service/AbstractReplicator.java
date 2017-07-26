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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicBusyException;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractReplicator {

    protected final BrokerService brokerService;
    protected final String topicName;
    protected final String localCluster;
    protected final String remoteCluster;
    protected final PulsarClientImpl client;

    protected volatile ProducerImpl producer;

    protected static final ProducerConfiguration producerConfiguration = new ProducerConfiguration()
            .setSendTimeout(0, TimeUnit.SECONDS).setBlockIfQueueFull(true);

    protected final Backoff backOff = new Backoff(100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES);

    protected final String replicatorPrefix;
    
    protected static final AtomicReferenceFieldUpdater<AbstractReplicator, State> STATE_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(AbstractReplicator.class, State.class, "state");
    private volatile State state = State.Stopped;
    
    protected enum State {
        Stopped, Starting, Started, Stopping
    }

    public AbstractReplicator(String topicName, String replicatorPrefix, String localCluster,
            String remoteCluster, BrokerService brokerService) {
        this.brokerService = brokerService;
        this.topicName = topicName;
        this.replicatorPrefix = replicatorPrefix;
        this.localCluster = localCluster;
        this.remoteCluster = remoteCluster;
        this.client = (PulsarClientImpl) brokerService.getReplicationClient(remoteCluster);
        this.producer = null;
        STATE_UPDATER.set(this, State.Stopped);
    }

    protected abstract void readEntries(org.apache.pulsar.client.api.Producer producer);

    protected abstract Position getReplicatorReadPosition();

    protected abstract long getNumberOfEntriesInBacklog();
    
    protected abstract void disableReplicatorRead();
    
    public String getRemoteCluster() {
        return remoteCluster;
    }

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
        client.createProducerAsync(topicName, producerConfiguration, getReplicatorName(replicatorPrefix, localCluster))
                .thenAccept(producer -> {
                    readEntries(producer);
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

    protected synchronized CompletableFuture<Void> closeProducerAsync() {
        if (producer == null) {
            STATE_UPDATER.set(this, State.Stopped);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = producer.closeAsync();
        future.thenRun(() -> {
            STATE_UPDATER.set(this, State.Stopped);
            this.producer = null;
            // deactivate further read
            disableReplicatorRead();
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


    public CompletableFuture<Void> disconnect() {
        return disconnect(false);
    }

    public synchronized CompletableFuture<Void> disconnect(boolean failIfHasBacklog) {
        if (failIfHasBacklog && getNumberOfEntriesInBacklog() > 0) {
            CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();
            disconnectFuture.completeExceptionally(new TopicBusyException("Cannot close a replicator with backlog"));
            log.debug("[{}][{} -> {}] Replicator disconnect failed since topic has backlog", topicName, localCluster,
                    remoteCluster);
            return disconnectFuture;
        }

        if (STATE_UPDATER.get(this) == State.Stopping) {
            // Do nothing since the all "STATE_UPDATER.set(this, Stopping)" instructions are followed by
            // closeProducerAsync()
            // which will at some point change the state to stopped
            return CompletableFuture.completedFuture(null);
        }

        if (producer != null && (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopping)
                || STATE_UPDATER.compareAndSet(this, State.Started, State.Stopping))) {
            log.info("[{}][{} -> {}] Disconnect replicator at position {} with backlog {}", topicName, localCluster,
                    remoteCluster, getReplicatorReadPosition(), getNumberOfEntriesInBacklog());
            return closeProducerAsync();
        }

        STATE_UPDATER.set(this, State.Stopped);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> remove() {
        // No-op
        return null;
    }

    protected boolean isWritable() {
        ProducerImpl producer = this.producer;
        return producer != null && producer.isWritable();
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

    private static final Logger log = LoggerFactory.getLogger(AbstractReplicator.class);
}

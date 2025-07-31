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

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.event.data.ReplicatorStopEventData;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicBusyException;
import org.apache.pulsar.broker.service.TopicEventsListener.TopicEvent;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractReplicator implements Replicator {

    protected final BrokerService brokerService;
    protected final String localTopicName;
    protected final String localCluster;
    protected final String remoteTopicName;
    protected final String remoteCluster;
    protected final PulsarClientImpl replicationClient;
    protected final PulsarClientImpl client;
    protected final PulsarAdmin replicationAdmin;
    protected String replicatorId;
    protected final Topic localTopic;
    @VisibleForTesting
    @Getter
    protected volatile ProducerImpl producer;
    public static final String REPL_PRODUCER_NAME_DELIMITER = "-->";

    protected final int producerQueueSize;
    protected final ProducerBuilder<byte[]> producerBuilder;

    protected final Backoff backOff = new Backoff(100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES, 0,
            TimeUnit.MILLISECONDS);

    protected final String replicatorPrefix;

    protected static final AtomicReferenceFieldUpdater<AbstractReplicator, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(AbstractReplicator.class, State.class, "state");
    @VisibleForTesting
    @Getter
    protected volatile State state = State.Disconnected;

    public enum State {
        /**
         * This enum has two mean meanings：
         *   Init: replicator is just created, has not been started now.
         *   Disconnected: the producer was closed after {@link PersistentTopic#checkGC} called {@link #disconnect}.
         */
        // The internal producer is disconnected.
        Disconnected,
        // Trying to create a new internal producer.
        Starting,
        // The internal producer has started, and tries copy data.
        Started,
        /**
         * The producer is closing after {@link PersistentTopic#checkGC} called {@link #disconnect}.
         */
        // The internal producer is trying to disconnect.
        Disconnecting,
        // The replicator is in terminating.
        Terminating,
        // The replicator is never used again. Pulsar will create a new Replicator when enable replication again.
        Terminated;
    }

    public AbstractReplicator(String localCluster, Topic localTopic, String remoteCluster, String remoteTopicName,
                              String replicatorPrefix, BrokerService brokerService, PulsarClientImpl replicationClient,
                              PulsarAdmin replicationAdmin)
            throws PulsarServerException {
        this.brokerService = brokerService;
        this.localTopic = localTopic;
        this.localTopicName = localTopic.getName();
        this.replicatorPrefix = replicatorPrefix;
        this.localCluster = localCluster.intern();
        this.remoteTopicName = remoteTopicName;
        this.remoteCluster = remoteCluster.intern();
        this.replicationClient = replicationClient;
        this.replicationAdmin = replicationAdmin;
        this.client = (PulsarClientImpl) brokerService.pulsar().getClient();
        this.producer = null;
        this.producerQueueSize = brokerService.pulsar().getConfiguration().getReplicationProducerQueueSize();
        this.replicatorId = String.format("%s | %s",
                StringUtils.equals(localTopicName, remoteTopicName) ? localTopicName :
                        localTopicName + "-->" + remoteTopicName,
                StringUtils.equals(localCluster, remoteCluster) ? localCluster : localCluster + "-->" + remoteCluster
        );
        this.producerBuilder = replicationClient.newProducer(Schema.AUTO_PRODUCE_BYTES()) //
                .topic(remoteTopicName)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .enableBatching(false)
                .sendTimeout(0, TimeUnit.SECONDS) //
                .maxPendingMessages(producerQueueSize) //
                .producerName(getProducerName());
        STATE_UPDATER.set(this, State.Disconnected);
    }

    protected abstract String getProducerName();

    protected abstract void setProducerAndTriggerReadEntries(org.apache.pulsar.client.api.Producer<byte[]> producer);

    protected abstract Position getReplicatorReadPosition();

    public abstract long getNumberOfEntriesInBacklog();

    protected abstract void disableReplicatorRead();

    public String getRemoteCluster() {
        return remoteCluster;
    }

    protected CompletableFuture<Void> prepareCreateProducer() {
        return CompletableFuture.completedFuture(null);
    }

    public void startProducer() {
        // Guarantee only one task call "producerBuilder.createAsync()".
        Pair<Boolean, State> setStartingRes = compareSetAndGetState(State.Disconnected, State.Starting);
        if (!setStartingRes.getLeft()) {
            if (setStartingRes.getRight() == State.Starting) {
                log.info("[{}] Skip the producer creation since other thread is doing starting, state : {}",
                        replicatorId, state);
            } else if (setStartingRes.getRight() == State.Started) {
                // Since the method "startProducer" will be called even if it is started, only print debug-level log.
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Replicator was already running. state: {}", replicatorId, state);
                }
            } else if (setStartingRes.getRight() == State.Disconnecting) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Rep.producer is closing, delay to retry(wait the producer close success)."
                            + " state: {}", replicatorId, state);
                }
                delayStartProducerAfterDisconnected();
            } else {
                /** {@link State.Terminating}, {@link State.Terminated}. **/
                log.info("[{}] Skip the producer creation since the replicator state is : {}", replicatorId, state);
            }
            return;
        }

        log.info("[{}] Starting replicator", replicatorId);

        // Force only replicate messages to a non-partitioned topic, to avoid auto-create a partitioned topic on
        // the remote cluster.
        prepareCreateProducer().thenCompose(ignore -> {
            ProducerBuilderImpl builderImpl = (ProducerBuilderImpl) producerBuilder;
            builderImpl.getConf().setNonPartitionedTopicExpected(true);
            return producerBuilder.createAsync().thenAccept(producer -> {
                setProducerAndTriggerReadEntries(producer);
            });
        }).exceptionally(ex -> {
            Pair<Boolean, State> setDisconnectedRes = compareSetAndGetState(State.Starting, State.Disconnected);
            if (setDisconnectedRes.getLeft()) {
                long waitTimeMs = backOff.next();
                log.warn("[{}] Failed to create remote producer, retrying in {} s",
                        replicatorId, waitTimeMs / 1000.0, ex);
                // BackOff before retrying
                scheduleCheckTopicActiveAndStartProducer(waitTimeMs);
            } else {
                if (setDisconnectedRes.getRight() == State.Terminating
                        || setDisconnectedRes.getRight() == State.Terminated) {
                    log.info("[{}] Skip to create producer, because it has been terminated, state is : {}",
                            replicatorId, state);
                } else {
                    /** {@link  State.Disconnected}, {@link  State.Starting}, {@link  State.Started} **/
                    // Since only one task can call "producerBuilder.createAsync()", this scenario is not expected.
                    // So print a warn log.
                    log.warn("[{}] Other thread will try to create the producer again. so skipped current one task."
                                    + " State is : {}",
                            replicatorId, state);
                }
            }
            return null;
        });

    }

    /***
     * The producer is disconnecting, delay to start the producer.
     * If we start a producer immediately, we will get a conflict producer(same name producer) registered error.
     */
    protected void delayStartProducerAfterDisconnected() {
        long waitTimeMs = backOff.next();
        if (log.isDebugEnabled()) {
            log.debug(
                    "[{}] waiting for producer to close before attempting to reconnect, retrying in {} s",
                    replicatorId, waitTimeMs / 1000.0);
        }
        scheduleCheckTopicActiveAndStartProducer(waitTimeMs);
    }

    protected void scheduleCheckTopicActiveAndStartProducer(final long waitTimeMs) {
        brokerService.executor().schedule(() -> {
            if (state == State.Terminating || state == State.Terminated) {
                log.info("[{}] Skip scheduled to start the producer since the replicator state is : {}",
                        replicatorId, state);
                return;
            }
            CompletableFuture<Optional<Topic>> topicFuture = brokerService.getTopics().get(localTopicName);
            if (topicFuture == null) {
                // Topic closed.
                log.info("[{}] Skip scheduled to start the producer since the topic was closed successfully."
                        + " And trigger a terminate.", replicatorId);
                terminate();
                return;
            }
            topicFuture.thenAccept(optional -> {
                if (optional.isEmpty()) {
                    // Topic closed.
                    log.info("[{}] Skip scheduled to start the producer since the topic was closed. And trigger a"
                            + " terminate.", replicatorId);
                    terminate();
                    return;
                }
                if (optional.get() != localTopic) {
                    // Topic closed and created a new one, current replicator is outdated.
                    log.info("[{}] Skip scheduled to start the producer since the topic was closed. And trigger a"
                            + " terminate.", replicatorId);
                    terminate();
                    return;
                }
                Replicator replicator = localTopic.getReplicators().get(remoteCluster);
                if (replicator != AbstractReplicator.this) {
                    // Current replicator has been closed, and created a new one.
                    log.info("[{}] Skip scheduled to start the producer since a new replicator has instead current"
                            + " one. And trigger a terminate.", replicatorId);
                    terminate();
                    return;
                }
                startProducer();
            }).exceptionally(ex -> {
                log.error("[{}] [{}] Stop retry to create producer due to unknown error(topic create failed), and"
                                + " trigger a terminate. Replicator state: {}",
                        localTopicName, replicatorId, STATE_UPDATER.get(this), ex);
                terminate();
                return null;
            });
        }, waitTimeMs, TimeUnit.MILLISECONDS);
    }

    protected CompletableFuture<Boolean> isLocalTopicActive() {
        CompletableFuture<Optional<Topic>> topicFuture = brokerService.getTopics().get(localTopicName);
        if (topicFuture == null){
            return CompletableFuture.completedFuture(false);
        }
        return topicFuture.thenApplyAsync(optional -> {
            if (optional.isEmpty()) {
                return false;
            }
            return optional.get() == localTopic;
        }, brokerService.executor());
    }

    /**
     * This method only be used by {@link PersistentTopic#checkGC} now.
     */
    public CompletableFuture<Void> disconnect(boolean failIfHasBacklog, boolean closeTheStartingProducer) {
        long backlog = getNumberOfEntriesInBacklog();
        if (failIfHasBacklog && backlog > 0) {
            CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();
            disconnectFuture.completeExceptionally(new TopicBusyException("Cannot close a replicator with backlog"));
            if (log.isDebugEnabled()) {
                log.debug("[{}] Replicator disconnect failed since topic has backlog", replicatorId);
            }
            return disconnectFuture;
        }
        log.info("[{}] Disconnect replicator at position {} with backlog {}", replicatorId,
                getReplicatorReadPosition(), backlog);
        return closeProducerAsync(closeTheStartingProducer);
    }

    /**
     * This method only be used by {@link PersistentTopic#checkGC} now.
     */
    protected CompletableFuture<Void> closeProducerAsync(boolean closeTheStartingProducer) {
        Pair<Boolean, State> setDisconnectingRes = compareSetAndGetState(State.Started, State.Disconnecting);
        if (!setDisconnectingRes.getLeft()) {
            if (setDisconnectingRes.getRight() == State.Starting) {
                if (closeTheStartingProducer) {
                    /**
                     * Delay retry(wait for the start producer task is finish).
                     * Note: If the producer always start fail, the start producer task will always retry until the
                     *   state changed to {@link State.Terminated}.
                     *   Nit: The better solution is creating a {@link CompletableFuture} to trace the in-progress
                     *     creation and call "inProgressCreationFuture.thenApply(closeProducer())".
                     */
                    long waitTimeMs = backOff.next();
                    brokerService.executor().schedule(() -> closeProducerAsync(true),
                            waitTimeMs, TimeUnit.MILLISECONDS);
                } else {
                    log.info("[{}] Skip current producer closing since the previous producer has been closed,"
                                    + " and trying start a new one, state : {}",
                            replicatorId, setDisconnectingRes.getRight());
                }
            } else if (setDisconnectingRes.getRight() == State.Disconnected
                    || setDisconnectingRes.getRight() == State.Disconnecting) {
                log.info("[{}] Skip current producer closing since other thread did closing, state : {}",
                        replicatorId, setDisconnectingRes.getRight());
            } else if (setDisconnectingRes.getRight() == State.Terminating
                    || setDisconnectingRes.getRight() == State.Terminated) {
                log.info("[{}] Skip current producer closing since other thread is doing termination, state : {}",
                        replicatorId, state);
            }
            log.info("[{}] Skip current termination since other thread is doing close producer or termination,"
                            + " state : {}", replicatorId, state);
            return CompletableFuture.completedFuture(null);
        }

        // Close producer and update state.
        return doCloseProducerAsync(producer, () -> {
            Pair<Boolean, State> setDisconnectedRes = compareSetAndGetState(State.Disconnecting, State.Disconnected);
            if (setDisconnectedRes.getLeft()) {
                this.producer = null;
                // deactivate further read
                disableReplicatorRead();
                brokerService.getTopicEventsDispatcher()
                        .newEvent(localTopicName, TopicEvent.REPLICATOR_STOP)
                        .data(ReplicatorStopEventData.builder()
                                .replicatorId(replicatorId)
                                .localCluster(localCluster)
                                .remoteCluster(remoteCluster)
                                .build())
                        .dispatch();
                return;
            }
            if (setDisconnectedRes.getRight() == State.Terminating
                    || setDisconnectingRes.getRight() == State.Terminated) {
                log.info("[{}] Skip setting state to terminated because it was terminated, state : {}",
                        replicatorId, state);
            } else {
                // Since only one task can call "doCloseProducerAsync(producer, action)", this scenario is not expected.
                // So print a warn log.
                log.warn("[{}] Other task has change the state to terminated. so skipped current one task."
                                + " State is : {}",
                        replicatorId, state);
            }
        });
    }

    protected CompletableFuture<Void> doCloseProducerAsync(Producer<byte[]> producer, Runnable actionAfterClosed) {
        CompletableFuture<Void> future =
                producer == null ? CompletableFuture.completedFuture(null) : producer.closeAsync();
        return future.thenRun(() -> {
            actionAfterClosed.run();
        }).exceptionally(ex -> {
            long waitTimeMs = backOff.next();
            log.warn(
                    "[{}] Exception: '{}' occurred while trying to close the producer. Replicator state: {}."
                            + " Retrying again in {} s.",
                    replicatorId, ex.getMessage(), state, waitTimeMs / 1000.0);
            // BackOff before retrying
            brokerService.executor().schedule(() -> doCloseProducerAsync(producer, actionAfterClosed),
                    waitTimeMs, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    public CompletableFuture<Void> terminate() {
        if (!tryChangeStatusToTerminating()) {
            log.info("[{}] Skip current termination since other thread is doing termination, state : {}", replicatorId,
                    state);
            return CompletableFuture.completedFuture(null);
        }
        return doCloseProducerAsync(producer, () -> {
            STATE_UPDATER.set(this, State.Terminated);
            this.producer = null;
            // set the cursor as inactive.
            disableReplicatorRead();
            // release resources.
            doReleaseResources();
        });
    }

    protected void doReleaseResources() {}

    protected boolean tryChangeStatusToTerminating() {
        if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Terminating)){
            return true;
        }
        if (STATE_UPDATER.compareAndSet(this, State.Started, State.Terminating)){
            return true;
        }
        if (STATE_UPDATER.compareAndSet(this, State.Disconnecting, State.Terminating)){
            return true;
        }
        if (STATE_UPDATER.compareAndSet(this, State.Disconnected, State.Terminating)) {
            return true;
        }
        return false;
    }

    public CompletableFuture<Void> remove() {
        // No-op
        return CompletableFuture.completedFuture(null);
    }

    protected boolean isWritable() {
        ProducerImpl producer = this.producer;
        return producer != null && producer.isWritable();
    }

    public static String getRemoteCluster(String remoteCursor) {
        String[] split = remoteCursor.split("\\.");
        return split[split.length - 1];
    }

    public static String getReplicatorName(String replicatorPrefix, String cluster) {
        return (replicatorPrefix + "." + cluster).intern();
    }

    /**
     * Replication can't be started on root-partitioned-topic to avoid producer startup conflict.
     *
     * <pre>
     * eg:
     * if topic : persistent://prop/cluster/ns/my-topic is a partitioned topic with 2 partitions then
     * broker explicitly creates replicator producer for: "my-topic-partition-1" and "my-topic-partition-2".
     *
     * However, if broker tries to start producer with root topic "my-topic" then client-lib internally
     * creates individual producers for "my-topic-partition-1" and "my-topic-partition-2" which creates
     * conflict with existing
     * replicator producers.
     * </pre>
     *
     * Therefore, replicator can't be started on root-partition topic which can internally create multiple partitioned
     * producers.
     *
     * @param topic
     * @param brokerService
     */
    public static CompletableFuture<Void> validatePartitionedTopicAsync(String topic, BrokerService brokerService) {
        TopicName topicName = TopicName.get(topic);
        return brokerService.pulsar().getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
            .partitionedTopicExistsAsync(topicName).thenCompose(isPartitionedTopic -> {
                if (isPartitionedTopic) {
                    String s = topicName
                            + " is a partitioned-topic and replication can't be started for partitioned-producer ";
                    log.error(s);
                    return FutureUtil.failedFuture(new NamingException(s));
                }
                return CompletableFuture.completedFuture(null);
            });
    }

    private static final Logger log = LoggerFactory.getLogger(AbstractReplicator.class);

    public State getState() {
        return state;
    }

    protected ImmutablePair<Boolean, State> compareSetAndGetState(State expect, State update) {
        State original1 = state;
        if (STATE_UPDATER.compareAndSet(this, expect, update)) {
            return ImmutablePair.of(true, expect);
        }
        State original2 = state;
        // Maybe the value changed more than once even if "original1 == original2", but the probability is very small,
        // so let's ignore this case for prevent using a lock.
        if (original1 == original2) {
            return ImmutablePair.of(false, original1);
        }
        return compareSetAndGetState(expect, update);
    }

    public boolean isTerminated() {
        return state == State.Terminating || state == State.Terminated;
    }
}

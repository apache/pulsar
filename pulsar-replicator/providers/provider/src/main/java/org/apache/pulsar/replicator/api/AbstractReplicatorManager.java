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
package org.apache.pulsar.replicator.api;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import org.apache.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * Base class for ReplicatorManager : manages pulsar consumer to consume pulsar messages and push to appropriate
 * replicator producer to publish them to targeted system.
 * 
 *
 */
public abstract class AbstractReplicatorManager
        implements ReplicatorManager, Function<Throwable, Void>, java.util.function.Consumer<Message> {

    private static final Map<String, PulsarClientImpl> pulsarClients = Maps.newConcurrentMap();
    protected PulsarClientImpl pulsarClient;
    protected ReplicatorProducer producer;
    protected String topicName;
    protected String regionName;
    protected ReplicatorConfig config;

    protected Consumer inputConsumer;
    protected static final AtomicReferenceFieldUpdater<AbstractReplicatorManager, State> STATE_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(AbstractReplicatorManager.class, State.class, "state");
    private volatile State state = State.Stopped;

    public static final String replPrefix = "pulsar";
    protected static final long READ_DELAY_BACKOFF_MS = 100;
    private static final int ZK_SESSION_TIME_OUT_MS = 30_000;
    private static final int MAX_ACK_RETRY = 50;

    private enum State {
        Stopped, Starting, Started;
    }

    /**
     * Initialize replicator producer resource and start message replication
     * 
     * @param topicName
     *            Pulsar topic-name from which it reads message and sends to targeted system
     * @param replicatorPolicies
     *            Replicator policies to initialize replicator produecr
     * @throws Exception
     */
    protected abstract CompletableFuture<ReplicatorProducer> startProducer(String topicName,
            ReplicatorPolicies replicatorPolicies);

    @Override
    public void start(ReplicatorConfig config) throws Exception {

        if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
            log.info("[{}] Replicator-manager {} topic {} is already started ", topicName, getType(), state);
            return;
        }

        try {
            this.config = config;
            this.topicName = checkNotNull(config.getTopicName(),
                    "topic-name must be present to start replicator on it");
            this.regionName = checkNotNull(config.getRegionName(), "region-name must be present");
            final String zkPolicyPath = path(POLICIES, TopicName.get(this.topicName).getNamespaceObject().toString());

            pulsarClient = pulsarClients.computeIfAbsent(config.getBrokerServiceUrl(), (url) -> {
                try {
                    return ((PulsarClientImpl) PulsarClient.builder().serviceUrl(config.getBrokerServiceUrl())
                            .statsInterval(0, TimeUnit.SECONDS).build());
                } catch (PulsarClientException e) {
                    log.error("Failed to create pulsar-client for url {}", url, e);
                    return null;
                }
            });

            inputConsumer = pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(formSubscriptionName(getType(), this.regionName)).receiverQueueSize(10)
                    .subscribe();

            startProducer(getReplicatorPolicies(zkPolicyPath, this.regionName));

        } catch (Exception e) {
            // cleanup resources
            stop();
            throw e;
        }
    }

    protected void startProducer(ReplicatorPolicies policies) {

        startProducer(this.topicName, policies).thenAccept(producer -> {
            this.producer = producer;
            STATE_UPDATER.set(this, State.Started);
            // producer started successfully.. trigger reading message
            readMessage();
        }).exceptionally(ex -> {
            pulsarClient.timer().newTimeout(timeout -> {
                startProducer(policies);
            }, READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
            return null;
        });

    }

    /**
     * Stops replicator producer
     * 
     * @throws Exception
     */
    protected void stopProducer() throws Exception {
        if (this.producer != null) {
            this.producer.close();
        }
    }

    @Override
    public void stop() throws Exception {
        if (this.inputConsumer != null) {
            this.inputConsumer.close();
        }
        if (this.producer != null) {
            this.producer.close();
        }
        STATE_UPDATER.set(this, State.Stopped);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void accept(Message message) {
        if (state == State.Stopped) {
            log.info("[{}} replicator {} is already stopped so, can't send messages", this.topicName, getType());
            return;
        }
        producer.send(message).thenAccept((res) -> {
            if (log.isDebugEnabled()) {
                log.debug("Successfully published message for replicator of {} ", this.topicName);
            }
            acknowledgeMessageWithRetry(message, 0);
            readMessage();
        }).exceptionally(ex -> {
            log.error("Failed to publish on replicator of {} retry after {}", this.topicName, READ_DELAY_BACKOFF_MS);
            pulsarClient.timer().newTimeout(timeout -> {
                accept(message);
            }, READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    @SuppressWarnings("unchecked")
    protected void readMessage() {
        if (state == State.Stopped) {
            log.info("[{}} replicator {} is already stopped so, can't read messages", this.topicName, getType());
            return;
        }
        inputConsumer.receiveAsync().thenAccept(this).exceptionally(this);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void acknowledgeMessageWithRetry(Message message, int retry) {
        if (state == State.Stopped) {
            if (log.isDebugEnabled()) {
                log.debug("[{}} replicator {} is already stopped so, can't ack", this.topicName, getType());
            }
            return;
        }
        if (retry > MAX_ACK_RETRY || state == State.Stopped) {
            log.error("[{}] Failed to ack for {} after {} retry, state {} ", this.topicName, message.getMessageId(),
                    MAX_ACK_RETRY, state);
            return;
        }
        inputConsumer.acknowledgeAsync(message).exceptionally(ex -> {
            log.error("[{}] Failed to ack for {}, will retry after ms", this.topicName, message.getMessageId(),
                    READ_DELAY_BACKOFF_MS);
            pulsarClient.timer().newTimeout(timeout -> {
                acknowledgeMessageWithRetry(message, retry + 1);
            }, READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    @Override
    public Void apply(Throwable t) {
        if (state == State.Stopped) {
            log.info("[{}} replicator {} is already stopped so, can't retry", this.topicName, getType());
            return null;
        }
        log.error("[{}} Failed to read message, will retry after {} ms, {}", this.topicName, READ_DELAY_BACKOFF_MS,
                t.getMessage());
        pulsarClient.timer().newTimeout(timeout -> readMessage(), READ_DELAY_BACKOFF_MS, TimeUnit.MILLISECONDS);
        return null;
    }

    private ReplicatorPolicies getReplicatorPolicies(String zkPolicyPath, String regionName) throws Exception {
        OrderedScheduler orderedExecutor = null;
        ZooKeeper zk = null;
        ReplicatorPolicies replicatorPolicies = null;
        try {
            orderedExecutor = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("pulsar-replicator-ordered")
                    .build();
            zk = getZooKeeperClient(config.getZkServiceUrl(), orderedExecutor);
            replicatorPolicies = getReplicatorPolicies(zk, zkPolicyPath, regionName);
        } finally {
            if (orderedExecutor != null) {
                orderedExecutor.shutdown();
            }
            if (zk != null) {
                zk.close();
            }
        }
        return replicatorPolicies;
    }

    private ReplicatorPolicies getReplicatorPolicies(ZooKeeper zk, String zkPolicyPath, String regionName)
            throws Exception {
        try {
            byte[] data = zk.getData(zkPolicyPath, false, null);
            Policies policies = ObjectMapperFactory.getThreadLocal().readValue(data, Policies.class);
            if (policies != null && policies.replicatorPolicies.containsKey(getType())
                    && policies.replicatorPolicies.get(getType()).containsKey(regionName)) {
                return policies.replicatorPolicies.get(getType()).get(regionName);
            } else {
                log.error("[{}] couldn't find replicator policies for {}", this.topicName, getType());
                throw new IllegalStateException(
                        "couldn't find replicator policies for " + this.topicName + ", " + getType());
            }
        } catch (NoNodeException e) {
            log.error("[{}] couldn't find policies for {}", this.topicName, getType());
            throw e;
        } catch (Exception e) {
            log.error("[{}] Failed to find policies for {}", this.topicName, getType());
            throw e;
        }
    }

    public static String path(String... parts) {
        StringBuilder sb = new StringBuilder();
        sb.append("/admin/");
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }

    public static String formSubscriptionName(ReplicatorType type, String clusterName) {
        return String.format("%s.%s.%s", replPrefix, clusterName, type.toString());
    }

    private ZooKeeper getZooKeeperClient(String zkServiceUrl, OrderedExecutor executor) throws Exception {
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create(zkServiceUrl, SessionType.AllowReadOnly,
                (int) ZK_SESSION_TIME_OUT_MS);
        try {
            return zkFuture.get(ZK_SESSION_TIME_OUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("[{}] Failed to start zk on {} ", this.topicName, zkServiceUrl, e);
            throw e;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(AbstractReplicatorManager.class);
}

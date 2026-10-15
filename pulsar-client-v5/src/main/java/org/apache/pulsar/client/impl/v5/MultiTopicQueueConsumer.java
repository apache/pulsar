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
package org.apache.pulsar.client.impl.v5;

import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.netty.util.Timeout;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncQueueConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Multi-topic {@link QueueConsumer} that subscribes to every scalable topic in a
 * namespace matching a (possibly empty) set of property filters. The matching set
 * follows live: when topics enter or leave the filter, the consumer attaches /
 * detaches automatically via a long-lived {@link ScalableTopicsWatcher} session
 * to the broker.
 *
 * <p>Internals:
 * <ul>
 *   <li>One {@link ScalableQueueConsumer} per matched topic.</li>
 *   <li>Each per-topic consumer forwards directly into the shared multiplexed queue
 *       via an injected message sink — no pump thread — tagging each message with the
 *       parent topic so the subsequent ack can be routed back.</li>
 *   <li>The watcher's {@code Snapshot} replaces the active set; {@code Diff}
 *       applies removals before additions to handle a rapid remove-then-add of the
 *       same topic name.</li>
 *   <li>Per-topic add failures retry with exponential backoff (100 ms initial,
 *       30 min cap) until the topic attaches or leaves the matching set.</li>
 * </ul>
 */
final class MultiTopicQueueConsumer<T> implements QueueConsumerImpl<T> {

    private static final Logger LOG = Logger.get(MultiTopicQueueConsumer.class);
    /**
     * Cap for per-topic subscribe retries. Matches v4 consumer reconnect semantics —
     * the consumer never gives up; the user just sees no messages from the topic
     * until the broker / topic recovers.
     */
    private static final Duration RETRY_MAX = Duration.ofMinutes(30);
    private final Logger log;

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private final ConsumerConfigurationData<T> consumerConf;
    private final NamespaceName namespace;
    private final Map<String, String> propertyFilters;
    private final String subscriptionName;

    private final ScalableTopicsWatcher watcher;
    /**
     * Every topic of the matching set, from when the consumer starts attaching it until it leaves the set:
     * while it is being attached, once attached, and while a failed attach waits to be retried.
     */
    private final ConcurrentHashMap<String, PerTopicState<T>> perTopic = new ConcurrentHashMap<>();
    private final V5ReceiveQueue<T> mux;

    private volatile boolean closed = false;
    private final AsyncQueueConsumerV5<T> asyncView;

    private MultiTopicQueueConsumer(PulsarClientV5 client,
                                    Schema<T> v5Schema,
                                    ConsumerConfigurationData<T> consumerConf,
                                    NamespaceName namespace,
                                    Map<String, String> propertyFilters,
                                    ScalableTopicsWatcher watcher) {
        this.client = client;
        this.v5Schema = v5Schema;
        this.consumerConf = consumerConf;
        this.namespace = namespace;
        this.propertyFilters = propertyFilters;
        this.subscriptionName = consumerConf.getSubscriptionName();
        this.watcher = watcher;
        this.mux = new V5ReceiveQueue<>(
                client.v4Client().externalExecutorProvider().getExecutor(), client.v4Client().timer(),
                consumerConf.getReceiverQueueSize());
        this.log = LOG.with()
                .attr("namespace", namespace)
                .attr("subscription", subscriptionName)
                .attr("filters", propertyFilters)
                .build();
        this.asyncView = new AsyncQueueConsumerV5<>(this);
    }

    static <T> CompletableFuture<QueueConsumer<T>> createAsync(PulsarClientV5 client,
                                                                Schema<T> v5Schema,
                                                                ConsumerConfigurationData<T> consumerConf,
                                                                NamespaceName namespace,
                                                                Map<String, String> propertyFilters) {
        ScalableTopicsWatcher watcher = new ScalableTopicsWatcher(
                client.v4Client(), namespace, propertyFilters);
        MultiTopicQueueConsumer<T> consumer = new MultiTopicQueueConsumer<>(
                client, v5Schema, consumerConf, namespace, propertyFilters, watcher);
        return watcher.start()
                .thenCompose(initial -> consumer.openInitial(initial))
                .thenApply(__ -> {
                    watcher.setListener(consumer.new WatcherListener());
                    return (QueueConsumer<T>) consumer;
                })
                .exceptionallyCompose(ex -> consumer.closeAsync().handle((__, ___) -> {
                    throw ex instanceof CompletionException ce ? ce : new CompletionException(ex);
                }));
    }

    /**
     * Open one per-topic consumer per topic in the initial snapshot. Wait for the first
     * attempt on every topic so {@code subscribeAsync} only resolves once the consumer is
     * fully attached, unless an attempt failed: that topic is then retried in the
     * background, like a topic that joins the matching set later.
     */
    private CompletableFuture<Void> openInitial(List<String> topics) {
        if (topics.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<?>> opens = new ArrayList<>(topics.size());
        for (String t : topics) {
            opens.add(openTopic(t));
        }
        return CompletableFuture.allOf(opens.toArray(CompletableFuture[]::new));
    }

    /**
     * Subscribe to one topic. Failures schedule a background retry with exponential
     * backoff until an attempt succeeds or the topic leaves the matching set; the
     * returned future completes as soon as the first attempt finishes (success or
     * failure) so we don't hold up Snapshot / Diff processing.
     */
    private CompletableFuture<Void> openTopic(String topicName) {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        PerTopicState<T> state = new PerTopicState<>();
        if (perTopic.putIfAbsent(topicName, state) != null) {
            // Already attached, being attached, or waiting to retry.
            return CompletableFuture.completedFuture(null);
        }
        return attachTopic(topicName, state);
    }

    /** One attempt at subscribing to a topic; a failure schedules the next one. */
    private CompletableFuture<Void> attachTopic(String topicName, PerTopicState<T> state) {
        TopicName topic = V5Utils.parseScalableTopicInput(topicName);
        // A namespace consumer only attaches to topics the watcher reports as existing; it must
        // never auto-create one (so a deleted topic can't be resurrected by a reconnecting watch).
        DagWatchClient dagWatch = new DagWatchClient(client.v4Client(), topic, /* createIfMissing= */ false);
        // Per-topic message sink: tag each delivered message with the parent scalable
        // topic for ack routing + display, and forward into the shared mux. No pump
        // thread; per-segment v4 receive loops fire this sink directly.
        MessageSink<T> sink = msg -> {
            if (closed) {
                return CompletableFuture.completedFuture(null);
            }
            return mux.offer(msg.withTopicOverride(topicName));
        };
        return dagWatch.start()
                .thenCompose(layout -> ScalableQueueConsumer.createAsyncImpl(
                        client, v5Schema, perTopicConf(topicName), dagWatch, layout, sink, null))
                .thenAccept(qc -> {
                    // closeTopic cancels the state when the topic leaves the matching set
                    // while this attempt is in flight: close what it opened.
                    if (closed || !state.consumer.complete(qc)) {
                        qc.closeAsync();
                        return;
                    }
                    log.info().attr("topic", topicName).log("Per-topic consumer attached");
                })
                .exceptionally(ex -> {
                    Throwable cause = ex instanceof CompletionException ce && ce.getCause() != null
                            ? ce.getCause() : ex;
                    // No retry once the topic has left the matching set: it may be gone for good.
                    if (!closed && !state.consumer.isDone()) {
                        scheduleRetry(topicName, state);
                    }
                    log.warn().attr("topic", topicName).exceptionMessage(cause)
                            .log("Per-topic subscribe failed");
                    return null;
                });
    }

    private void scheduleRetry(String topicName, PerTopicState<T> state) {
        long delayMs = nextBackoff(topicName);
        log.info().attr("topic", topicName).attr("delayMs", delayMs)
                .log("Retrying per-topic subscribe after backoff");
        state.retry = client.v4Client().timer().newTimeout(t -> {
            if (!closed && !state.consumer.isDone()) {
                attachTopic(topicName, state);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private final ConcurrentHashMap<String, AtomicLong> retryDelays = new ConcurrentHashMap<>();

    /** Returns the next exponential-backoff delay (ms) for a topic and updates the state. */
    private long nextBackoff(String topicName) {
        AtomicLong al = retryDelays.computeIfAbsent(topicName, t -> new AtomicLong(100));
        long current = al.get();
        long next = Math.min(current * 2, RETRY_MAX.toMillis());
        al.set(next);
        return current;
    }

    private void resetBackoff(String topicName) {
        retryDelays.remove(topicName);
    }

    /**
     * Clone the user's consumer config for a per-topic consumer. Each per-topic
     * consumer needs a unique {@code consumerName} so the broker can disambiguate
     * them on the same subscription; we suffix with the topic name.
     */
    private ConsumerConfigurationData<T> perTopicConf(String topicName) {
        var conf = consumerConf.clone();
        if (consumerConf.getConsumerName() != null) {
            // Disambiguate across topics — the broker side cares about uniqueness on
            // the same Shared subscription per segment.
            String localName = TopicName.get(topicName).getLocalName();
            conf.setConsumerName(consumerConf.getConsumerName() + "-" + localName);
        }
        return conf;
    }

    /**
     * Close per-topic consumer for a topic that has dropped out of the matching set.
     * No explicit ack flush — Shared subscription acks are independent per message
     * and the per-topic consumer's existing close already flushes pending acks via
     * its v4 segment consumers.
     */
    private CompletableFuture<Void> closeTopic(String topicName) {
        retryDelays.remove(topicName);
        PerTopicState<T> state = perTopic.remove(topicName);
        if (state == null) {
            return CompletableFuture.completedFuture(null);
        }
        // If it isn't attached yet, stop attaching it: an attempt in flight closes the consumer
        // it opens, and a pending backoff retry must not re-subscribe a topic that just left
        // the match set.
        boolean attaching = state.consumer.cancel(false);
        Timeout retry = state.retry;
        if (retry != null) {
            retry.cancel();
        }
        if (attaching) {
            return CompletableFuture.completedFuture(null);
        }
        return state.consumer.thenCompose(ScalableQueueConsumer::closeAsync)
                .thenRun(() -> log.info().attr("topic", topicName)
                        .log("Per-topic consumer detached"));
    }

    /**
     * The scalable topics this namespace consumer currently has an attached per-topic consumer for.
     * Lets tests assert that topics are attached/detached as the namespace's matching set changes
     * (e.g. that a deleted topic's per-topic consumer is stopped).
     */
    @VisibleForTesting
    Set<String> attachedTopicsForTesting() {
        Set<String> attached = new HashSet<>();
        perTopic.forEach((topic, state) -> {
            if (state.attachedConsumer() != null) {
                attached.add(topic);
            }
        });
        return attached;
    }

    // --- QueueConsumer ---

    @Override
    public String topic() {
        return "namespace://" + namespace;
    }

    @Override
    public String subscription() {
        return subscriptionName;
    }

    @Override
    public String consumerName() {
        return consumerConf.getConsumerName();
    }

    @Override
    public Message<T> receive() throws PulsarClientException {
        return mux.take();
    }

    @Override
    public Message<T> receive(Duration timeout) throws PulsarClientException {
        return mux.poll(timeout);
    }

    @Override
    public void acknowledge(MessageId messageId) {
        routeAck(messageId, ptc -> ptc.acknowledge(messageId));
    }

    @Override
    public void acknowledge(MessageId messageId, Transaction txn) {
        routeAck(messageId, ptc -> ptc.acknowledge(messageId, txn));
    }

    @Override
    public void negativeAcknowledge(MessageId messageId) {
        routeAck(messageId, ptc -> ptc.negativeAcknowledge(messageId));
    }

    /** Look up the per-topic consumer via the parent topic tag and delegate. */
    private void routeAck(MessageId messageId, java.util.function.Consumer<QueueConsumer<T>> action) {
        if (!(messageId instanceof MessageIdV5 id)) {
            throw new IllegalArgumentException("Expected MessageIdV5, got: " + messageId.getClass());
        }
        String parent = id.parentTopic();
        if (parent == null) {
            throw new IllegalStateException("MessageIdV5 missing parent topic — was the message"
                    + " delivered through a multi-topic consumer?");
        }
        PerTopicState<T> state = perTopic.get(parent);
        ScalableQueueConsumer<T> consumer = state != null ? state.attachedConsumer() : null;
        if (consumer == null) {
            // Topic was removed between deliver and ack. Fine — broker has dropped the
            // session for that topic. Drop the ack silently.
            log.debug().attr("topic", parent)
                    .log("Ack for removed topic; dropping");
            return;
        }
        action.accept(consumer);
    }

    @Override
    public AsyncQueueConsumer<T> async() {
        return asyncView;
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Close interrupted", e);
        } catch (ExecutionException e) {
            throw new PulsarClientException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Message<T>> receiveAsync() {
        return mux.receiveAsync();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        closed = true;
        watcher.close();
        mux.close();
        List<CompletableFuture<Void>> closes = new ArrayList<>();
        for (var topic : new HashSet<>(perTopic.keySet())) {
            closes.add(closeTopic(topic));
        }
        return CompletableFuture.allOf(closes.toArray(CompletableFuture[]::new));
    }

    // --- Watcher listener ---

    private final class WatcherListener implements ScalableTopicsWatcher.Listener {
        @Override
        public void onSnapshot(List<String> topics) {
            // Reconcile: open anything new, close anything missing. Same as Diff but
            // computed from the full snapshot — used on reconnect when broker hash
            // differs from ours.
            Set<String> target = new HashSet<>(topics);
            Set<String> current = new HashSet<>(perTopic.keySet());
            // Remove first so a rapid remove-then-add of same name closes-then-reopens.
            for (String t : current) {
                if (!target.contains(t)) {
                    closeTopic(t);
                }
            }
            for (String t : target) {
                if (!current.contains(t)) {
                    openTopic(t);
                    resetBackoff(t);
                }
            }
        }

        @Override
        public void onDiff(List<String> added, List<String> removed) {
            // Apply removed before added — covers rapid remove-then-add of same name.
            for (String t : removed) {
                closeTopic(t);
            }
            for (String t : added) {
                openTopic(t);
                resetBackoff(t);
            }
        }
    }

    // --- Per-topic state ---

    /**
     * Per-topic bookkeeping, from when the topic joins the matching set until it leaves it.
     * Messages flow directly into the shared mux via the sink the wrapper installed on the
     * per-topic consumer at create-time, so there's no pump thread to start/stop here — just
     * hold the underlying consumer, once attached, for ack routing and clean shutdown, and the
     * pending retry while attaching it fails.
     */
    private static final class PerTopicState<T> {
        /**
         * Completes with the per-topic consumer once an attempt attaches it. Cancelled by
         * {@code closeTopic} if the topic leaves the matching set first, which stops the attempts.
         */
        private final CompletableFuture<ScalableQueueConsumer<T>> consumer = new CompletableFuture<>();
        private volatile Timeout retry;

        /** The per-topic consumer, or null while the topic isn't attached. */
        ScalableQueueConsumer<T> attachedConsumer() {
            return consumer.isDone() && !consumer.isCompletedExceptionally() ? consumer.getNow(null) : null;
        }
    }
}

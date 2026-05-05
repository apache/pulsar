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

import io.github.merlimat.slog.Logger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Messages;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncStreamConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Multi-topic {@link StreamConsumer} over the union of scalable topics in a
 * namespace matching a (possibly empty) set of property filters.
 *
 * <p>Cumulative ack across topics works via a per-message position-vector
 * snapshot: each message that enters the multiplexed queue carries
 * {@code Map<TopicName, Map<SegmentId, MessageId>>} captured at enqueue time.
 * On {@code acknowledgeCumulative(msg)}, the wrapper fans out to every
 * per-topic consumer with the right segment vector — same semantics as the
 * single-topic case, just lifted one level.
 *
 * <p>For Removed-mid-stream topics we flush acks up to {@code latestDelivered}
 * for that topic before closing the per-topic consumer, so the user's
 * processing-acked invariant is preserved if the topic is later re-added.
 */
final class MultiTopicStreamConsumer<T> implements StreamConsumer<T> {

    private static final Logger LOG = Logger.get(MultiTopicStreamConsumer.class);
    /**
     * Cap for per-topic subscribe retries. Matches v4 consumer reconnect semantics.
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
    private final ConcurrentHashMap<String, PerTopic<T>> perTopic = new ConcurrentHashMap<>();
    private final LinkedTransferQueue<MessageV5<T>> mux = new LinkedTransferQueue<>();

    /**
     * Tracks the latest delivered message id per (parent topic, segment id) across
     * every per-topic consumer. Snapshotted at enqueue time for each delivered
     * message so cumulative ack covers everything visible up to that message.
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<Long, org.apache.pulsar.client.api.MessageId>>
            latestDeliveredPerTopicSegment = new ConcurrentHashMap<>();

    private volatile boolean closed = false;
    private final AsyncStreamConsumerV5Multi asyncView;

    private MultiTopicStreamConsumer(PulsarClientV5 client,
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
        this.log = LOG.with()
                .attr("namespace", namespace)
                .attr("subscription", subscriptionName)
                .attr("filters", propertyFilters)
                .build();
        this.asyncView = new AsyncStreamConsumerV5Multi();
    }

    static <T> CompletableFuture<StreamConsumer<T>> createAsync(PulsarClientV5 client,
                                                                Schema<T> v5Schema,
                                                                ConsumerConfigurationData<T> consumerConf,
                                                                NamespaceName namespace,
                                                                Map<String, String> propertyFilters) {
        ScalableTopicsWatcher watcher = new ScalableTopicsWatcher(
                client.v4Client(), namespace, propertyFilters);
        MultiTopicStreamConsumer<T> consumer = new MultiTopicStreamConsumer<>(
                client, v5Schema, consumerConf, namespace, propertyFilters, watcher);
        return watcher.start()
                .thenCompose(initial -> consumer.openInitial(initial))
                .thenApply(__ -> {
                    watcher.setListener(consumer.new WatcherListener());
                    return (StreamConsumer<T>) consumer;
                })
                .exceptionallyCompose(ex -> consumer.closeAsync().handle((__, ___) -> {
                    throw ex instanceof CompletionException ce ? ce : new CompletionException(ex);
                }));
    }

    private CompletableFuture<Void> openInitial(List<String> topics) {
        if (topics.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<?>> opens = new ArrayList<>(topics.size());
        for (String t : topics) {
            opens.add(openTopic(t, /* retry= */ false));
        }
        return CompletableFuture.allOf(opens.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> openTopic(String topicName, boolean retry) {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        if (perTopic.containsKey(topicName)) {
            return CompletableFuture.completedFuture(null);
        }
        TopicName topic = V5Utils.asScalableTopicName(topicName);
        // One ScalableConsumerClient session per topic, same as the single-topic builder.
        ScalableConsumerClient session = new ScalableConsumerClient(
                client.v4Client(), topic,
                consumerConf.getSubscriptionName(),
                perTopicConsumerName(topicName),
                ScalableConsumerType.STREAM);

        // Per-topic message sink: each delivered message arrives with its
        // single-topic positionVector (computed by ScalableStreamConsumer). Update
        // our cross-topic latestDelivered map, snapshot the full cross-topic vector,
        // and forward to the shared mux. No pump thread.
        java.util.function.Consumer<MessageV5<T>> sink = msg ->
                onPerTopicMessage(topicName, msg);

        return session.start()
                .thenCompose(initialAssignment -> ScalableStreamConsumer.createAsyncImpl(
                        client, v5Schema, perTopicConf(topicName), session,
                        topicName, initialAssignment, sink))
                .thenAccept(sc -> {
                    if (closed) {
                        sc.closeAsync();
                        return;
                    }
                    PerTopic<T> state = new PerTopic<>(topicName, sc);
                    PerTopic<T> existing = perTopic.putIfAbsent(topicName, state);
                    if (existing != null) {
                        sc.closeAsync();
                        return;
                    }
                    log.info().attr("topic", topicName).log("Per-topic stream consumer attached");
                })
                .exceptionally(ex -> {
                    Throwable cause = ex instanceof CompletionException ce && ce.getCause() != null
                            ? ce.getCause() : ex;
                    if (retry && !closed) {
                        scheduleRetry(topicName);
                    }
                    log.warn().attr("topic", topicName).exceptionMessage(cause)
                            .log("Per-topic stream subscribe failed");
                    return null;
                });
    }

    private void scheduleRetry(String topicName) {
        long delayMs = nextBackoff(topicName);
        log.info().attr("topic", topicName).attr("delayMs", delayMs)
                .log("Retrying per-topic stream subscribe");
        client.v4Client().timer().newTimeout(timeout -> openTopic(topicName, /* retry= */ true),
                delayMs, TimeUnit.MILLISECONDS);
    }

    private final ConcurrentHashMap<String, AtomicLong> retryDelays = new ConcurrentHashMap<>();

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
     * Per-topic consumer name. Each topic gets a distinct name so the broker's per-topic
     * coordinator can register them as separate consumers (same Exclusive-per-segment
     * semantics, no cross-topic identity coupling).
     */
    private String perTopicConsumerName(String topicName) {
        String localName = TopicName.get(topicName).getLocalName();
        if (consumerConf.getConsumerName() != null) {
            return consumerConf.getConsumerName() + "-" + localName;
        }
        return "v5-stream-" + V5RandomIds.randomAlphanumeric(8) + "-" + localName;
    }

    private ConsumerConfigurationData<T> perTopicConf(String topicName) {
        var conf = consumerConf.clone();
        conf.setConsumerName(perTopicConsumerName(topicName));
        return conf;
    }

    /**
     * Close per-topic consumer, flushing pending cumulative acks up to whatever was
     * last delivered for that topic. If the topic later re-appears (re-Added), a
     * fresh consumer subscribes and resumes from the broker-side cursor — already
     * advanced past the messages we've delivered to the user.
     */
    private CompletableFuture<Void> closeTopic(String topicName) {
        retryDelays.remove(topicName);
        PerTopic<T> state = perTopic.remove(topicName);
        if (state == null) {
            return CompletableFuture.completedFuture(null);
        }
        // Flush: ack everything we delivered for this topic.
        ConcurrentHashMap<Long, org.apache.pulsar.client.api.MessageId> latest =
                latestDeliveredPerTopicSegment.remove(topicName);
        if (latest != null && !latest.isEmpty()) {
            state.consumer.ackUpToVector(new HashMap<>(latest));
        }
        return state.consumer.closeAsync()
                .thenRun(() -> log.info().attr("topic", topicName)
                        .log("Per-topic stream consumer detached"));
    }

    // --- StreamConsumer ---

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
        try {
            return mux.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        }
    }

    @Override
    public Message<T> receive(Duration timeout) throws PulsarClientException {
        try {
            return mux.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        }
    }

    @Override
    public Messages<T> receiveMulti(int maxNumMessages, Duration timeout) throws PulsarClientException {
        // Block for up to `timeout` waiting for the first message, then drain whatever
        // else is immediately available up to maxNumMessages. Same shape as the single
        // topic StreamConsumer.
        long deadline = System.nanoTime() + timeout.toNanos();
        List<Message<T>> batch = new ArrayList<>();
        try {
            long remaining = deadline - System.nanoTime();
            while (batch.size() < maxNumMessages && remaining > 0) {
                MessageV5<T> msg = mux.poll(remaining, TimeUnit.NANOSECONDS);
                if (msg == null) {
                    break;
                }
                batch.add(msg);
                remaining = deadline - System.nanoTime();
            }
            // Opportunistic drain of anything else already queued.
            List<MessageV5<T>> tail = new ArrayList<>();
            mux.drainTo(tail, maxNumMessages - batch.size());
            batch.addAll(tail);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        }
        return new MessagesV5<>(batch);
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId) {
        fanOutCumulativeAck(messageId, (sc, vector) -> sc.ackUpToVector(vector));
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId, Transaction txn) {
        // Transactions on multi-topic are best-effort across per-topic consumers — each
        // per-topic ack is independently transactional. See note in the design doc.
        fanOutCumulativeAck(messageId, (sc, vector) -> sc.ackUpToVector(vector));
    }

    /**
     * For a cumulative ack on a multi-topic message, look up its multi-topic vector
     * and invoke the per-topic ack on every parent topic.
     */
    private void fanOutCumulativeAck(MessageId messageId,
                                     java.util.function.BiConsumer<ScalableStreamConsumer<T>,
                                             Map<Long, org.apache.pulsar.client.api.MessageId>> action) {
        if (!(messageId instanceof MessageIdV5 id)) {
            throw new IllegalArgumentException("Expected MessageIdV5, got: " + messageId.getClass());
        }
        Map<String, Map<Long, org.apache.pulsar.client.api.MessageId>> vector = id.multiTopicVector();
        if (vector == null) {
            throw new IllegalStateException("MessageIdV5 missing multi-topic vector — was the"
                    + " message delivered through a multi-topic stream consumer?");
        }
        for (var entry : vector.entrySet()) {
            PerTopic<T> state = perTopic.get(entry.getKey());
            if (state == null) {
                // Topic was Removed since enqueue; closeTopic already flushed.
                continue;
            }
            action.accept(state.consumer, entry.getValue());
        }
    }

    @Override
    public AsyncStreamConsumer<T> async() {
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

    CompletableFuture<Void> closeAsync() {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        closed = true;
        watcher.close();
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
            Set<String> target = new HashSet<>(topics);
            Set<String> current = new HashSet<>(perTopic.keySet());
            for (String t : current) {
                if (!target.contains(t)) {
                    closeTopic(t);
                }
            }
            for (String t : target) {
                if (!current.contains(t)) {
                    openTopic(t, /* retry= */ true);
                    resetBackoff(t);
                }
            }
        }

        @Override
        public void onDiff(List<String> added, List<String> removed) {
            for (String t : removed) {
                closeTopic(t);
            }
            for (String t : added) {
                openTopic(t, /* retry= */ true);
                resetBackoff(t);
            }
        }
    }

    /**
     * Per-topic message handler installed as the sink on each per-topic
     * {@link ScalableStreamConsumer}. The single-topic consumer has already
     * computed its per-segment position vector and stored it on the inbound
     * message id; we adopt that as the per-topic slice of our cross-topic
     * vector, snapshot the full map, and forward into the shared mux.
     *
     * <p>Runs on the netty IO thread that delivered the per-segment message —
     * the only contention is the synchronized snapshot block which guards
     * against torn cross-topic views during concurrent deliveries.
     */
    private void onPerTopicMessage(String parentTopic, MessageV5<T> msg) {
        if (closed) {
            return;
        }
        MessageIdV5 origId = (MessageIdV5) msg.id();

        // Adopt the message's own positionVector as our per-topic latest-delivered
        // slice. ScalableStreamConsumer maintained the increasing invariant on
        // each segment id; merging via putAll keeps the property cross-topic.
        ConcurrentHashMap<Long, org.apache.pulsar.client.api.MessageId> ours =
                latestDeliveredPerTopicSegment.computeIfAbsent(parentTopic,
                        k -> new ConcurrentHashMap<>());
        ours.putAll(origId.positionVector());

        // Snapshot the cross-topic vector under lock so concurrent deliveries
        // can't observe a torn view.
        Map<String, Map<Long, org.apache.pulsar.client.api.MessageId>> snapshot;
        synchronized (latestDeliveredPerTopicSegment) {
            snapshot = new HashMap<>(latestDeliveredPerTopicSegment.size());
            for (var e : latestDeliveredPerTopicSegment.entrySet()) {
                snapshot.put(e.getKey(), new HashMap<>(e.getValue()));
            }
        }

        MessageIdV5 newId = new MessageIdV5(
                origId.v4MessageId(), origId.segmentId(),
                origId.positionVector(), parentTopic, snapshot);
        mux.add(new MessageV5<>(msg.v4Message(), newId, parentTopic));
    }

    // --- Per-topic state ---

    /**
     * Per-topic bookkeeping. Messages flow into the shared mux directly via the
     * sink installed on the per-topic consumer at create-time, so there's no
     * pump thread to manage — this is just a holder for ack routing and clean
     * shutdown.
     */
    private static final class PerTopic<T> {
        private final String parentTopic;
        private final ScalableStreamConsumer<T> consumer;

        PerTopic(String parentTopic, ScalableStreamConsumer<T> consumer) {
            this.parentTopic = parentTopic;
            this.consumer = consumer;
        }
    }

    // --- Async view ---

    private final class AsyncStreamConsumerV5Multi implements AsyncStreamConsumer<T> {
        @Override
        public CompletableFuture<Message<T>> receive() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return MultiTopicStreamConsumer.this.receive();
                } catch (PulsarClientException e) {
                    throw new CompletionException(e);
                }
            });
        }

        @Override
        public CompletableFuture<Message<T>> receive(Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return MultiTopicStreamConsumer.this.receive(timeout);
                } catch (PulsarClientException e) {
                    throw new CompletionException(e);
                }
            });
        }

        @Override
        public CompletableFuture<List<Message<T>>> receiveMulti(int maxNumMessages, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Messages<T> ms = MultiTopicStreamConsumer.this.receiveMulti(maxNumMessages, timeout);
                    List<Message<T>> out = new ArrayList<>();
                    for (Message<T> m : ms) {
                        out.add(m);
                    }
                    return out;
                } catch (PulsarClientException e) {
                    throw new CompletionException(e);
                }
            });
        }

        @Override
        public void acknowledgeCumulative(MessageId messageId) {
            MultiTopicStreamConsumer.this.acknowledgeCumulative(messageId);
        }

        @Override
        public void acknowledgeCumulative(MessageId messageId, Transaction txn) {
            MultiTopicStreamConsumer.this.acknowledgeCumulative(messageId, txn);
        }

        @Override
        public CompletableFuture<Void> close() {
            return MultiTopicStreamConsumer.this.closeAsync();
        }
    }
}

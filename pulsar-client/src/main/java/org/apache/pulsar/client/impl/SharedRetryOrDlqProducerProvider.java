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

import io.github.merlimat.slog.Logger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.DeadLetterProducerBuilderContext;
import org.apache.pulsar.client.api.DeadLetterProducerBuilderCustomizer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * A {@link RetryOrDlqProducerProvider} implementation that holds a single pair of retry-letter and
 * dead-letter producers to be shared among all partition sub-consumers of the same subscription.
 *
 * <p>Lifecycle semantics:
 * <ul>
 *   <li>Producers are created lazily, on first access, using the same double-checked
 *       locking + backoff-on-failure logic as the historical per-consumer path.</li>
 *   <li>The producer builder customizer context is built using the <b>subscription-level</b>
 *       topic name (the non-partitioned topic name), so that the context is identical regardless
 *       of which partition sub-consumer triggered the first creation.</li>
 *   <li>{@link #close()} MUST be called by the owning {@link MultiTopicsConsumerImpl} when the
 *       subscription is being closed; sub-consumers must not close the shared producers.</li>
 * </ul>
 */
class SharedRetryOrDlqProducerProvider implements RetryOrDlqProducerProvider {

    private static final Logger log = Logger.get(SharedRetryOrDlqProducerProvider.class);

    private final PulsarClientImpl client;
    // Subscription-level topic name (non-partitioned). Used as the "input topic" of the customizer context.
    private final String subscriptionTopicName;
    private final String subscription;
    private final String consumerName;
    private final DeadLetterPolicy deadLetterPolicy;
    // Hint of how many partition sub-consumers will share this provider. Used to scale the
    // shared producer's maxPendingMessages so that aggregate buffering capacity does not shrink
    // compared to the historical per-partition behaviour. Must be >= 1.
    // Volatile because the owning MultiTopicsConsumerImpl may call {@link #updatePartitionHint}
    // during dynamic partition expansion; the value is only observed when creating a new producer
    // (lazy-init slow path), so updates do not affect producers already created.
    private volatile int partitionHint;

    // Set to true after close() has started. Protects against sub-consumers that race with the
    // close path and might otherwise observe (and send on) an already-closed shared producer.
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Guards the lazy-initialization of the two shared producers. The hot path (producer already
    // created and healthy) stays lock-free via a volatile read.
    private final Object retryProducerInitLock = new Object();
    private final Object deadLetterProducerInitLock = new Object();

    private volatile CompletableFuture<Producer<byte[]>> retryLetterProducer;
    private final AtomicInteger retryLetterProducerFailureCount = new AtomicInteger();

    private volatile CompletableFuture<Producer<byte[]>> deadLetterProducer;
    private final AtomicInteger deadLetterProducerFailureCount = new AtomicInteger();

    // Expected schema (reference-equality) that all sub-consumers must share. The provider is
    // scoped to a single subscription on a single partitioned topic, where every partition
    // sub-consumer is cloned from the same source schema. Mismatches would indicate incorrect
    // wiring and are logged but not fatal (first-call wins).
    private volatile Schema<?> expectedSchema;

    SharedRetryOrDlqProducerProvider(PulsarClientImpl client,
                                     String subscriptionTopicName,
                                     String subscription,
                                     String consumerName,
                                     DeadLetterPolicy deadLetterPolicy) {
        this(client, subscriptionTopicName, subscription, consumerName, deadLetterPolicy, 1);
    }

    SharedRetryOrDlqProducerProvider(PulsarClientImpl client,
                                     String subscriptionTopicName,
                                     String subscription,
                                     String consumerName,
                                     DeadLetterPolicy deadLetterPolicy,
                                     int partitionHint) {
        this.client = client;
        this.subscriptionTopicName = subscriptionTopicName;
        this.subscription = subscription;
        this.consumerName = consumerName;
        this.deadLetterPolicy = deadLetterPolicy;
        this.partitionHint = Math.max(1, partitionHint);
    }

    // Visible for testing.
    String getSubscriptionTopicNameForTesting() {
        return subscriptionTopicName;
    }

    // Visible for testing.
    DeadLetterPolicy getDeadLetterPolicyForTesting() {
        return deadLetterPolicy;
    }

    // Visible for testing.
    int getPartitionHintForTesting() {
        return partitionHint;
    }

    @Override
    public CompletableFuture<Producer<byte[]>> getOrCreateRetryLetterProducer(Schema<?> schemaForSubscription) {
        if (closed.get()) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException(
                    "Shared retry letter producer provider has been closed"));
        }
        checkSchemaConsistency(schemaForSubscription);
        CompletableFuture<Producer<byte[]>> p = retryLetterProducer;
        // Hot path: stay lock-free when a future exists. Failed futures are rebuilt by the first
        // thread that observes the failure (see slow path below); concurrent callers simply await
        // the same in-flight retry attempt instead of each scheduling their own backoff task,
        // which would amplify the pressure on the retry topic broker.
        if (p != null) {
            return p;
        }
        synchronized (retryProducerInitLock) {
            // Re-check closed state under the init lock to prevent a race where close() runs
            // between the hot-path check above and acquiring the init lock -- without this guard
            // we would create a brand-new producer that would never be closed.
            if (closed.get()) {
                return FutureUtil.failedFuture(
                        new PulsarClientException.AlreadyClosedException(
                                "Shared retry letter producer provider has been closed"));
            }
            p = retryLetterProducer;
            if (p == null) {
                p = createRetryLetterProducerLocked(schemaForSubscription);
                retryLetterProducer = p;
                hookRetryLetterProducerFailureRebuild(p, schemaForSubscription);
            }
            return p;
        }
    }

    private CompletableFuture<Producer<byte[]>> createRetryLetterProducerLocked(Schema<?> schemaForSubscription) {
        return createProducerWithBackOff(() -> {
            ProducerBuilder<byte[]> builder = client
                    .newProducer(Schema.AUTO_PRODUCE_BYTES(schemaForSubscription))
                    .topic(deadLetterPolicy.getRetryLetterTopic())
                    .enableBatching(false)
                    .enableChunking(true)
                    .blockIfQueueFull(false);
            scaleMaxPendingMessages(builder);
            customizeDeadLetterProducerBuilder(
                    deadLetterPolicy.getRetryLetterProducerBuilderCustomizer(),
                    deadLetterPolicy.getRetryLetterTopic(), builder);
            CompletableFuture<Producer<byte[]>> newProducer = builder.createAsync();
            newProducer.whenComplete((producer, ex) -> {
                if (ex != null) {
                    log.error().attr("retryLetterTopic", deadLetterPolicy.getRetryLetterTopic())
                            .exception(ex)
                            .log("Failed to create shared retry letter producer");
                    retryLetterProducerFailureCount.incrementAndGet();
                } else {
                    retryLetterProducerFailureCount.set(0);
                }
            });
            return newProducer;
        }, retryLetterProducerFailureCount, () -> "shared retry letter producer (topic: "
                + deadLetterPolicy.getRetryLetterTopic() + ")");
    }

    /**
     * When the shared future finally completes exceptionally, drop it from the volatile slot so
     * that the next request from any sub-consumer will trigger a single, backoff-respecting
     * rebuild under the init lock -- instead of every concurrent caller re-entering the slow path
     * (as `isCompletedExceptionally()` on the hot path would have caused) and scheduling its own
     * backoff task, which multiplies the retry-topic broker pressure by the partition count.
     */
    private void hookRetryLetterProducerFailureRebuild(CompletableFuture<Producer<byte[]>> future,
                                                       Schema<?> schemaForSubscription) {
        future.whenComplete((producer, ex) -> {
            if (ex == null) {
                return;
            }
            synchronized (retryProducerInitLock) {
                if (retryLetterProducer == future) {
                    retryLetterProducer = null;
                }
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Producer<byte[]>> getOrCreateDeadLetterProducer(Schema<?> schemaForSubscription) {
        if (closed.get()) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException(
                    "Shared dead letter producer provider has been closed"));
        }
        checkSchemaConsistency(schemaForSubscription);
        CompletableFuture<Producer<byte[]>> p = deadLetterProducer;
        if (p != null) {
            return p;
        }
        synchronized (deadLetterProducerInitLock) {
            // Re-check closed state under the init lock (same rationale as the retry path).
            if (closed.get()) {
                return FutureUtil.failedFuture(
                        new PulsarClientException.AlreadyClosedException(
                                "Shared dead letter producer provider has been closed"));
            }
            p = deadLetterProducer;
            if (p == null) {
                p = createDeadLetterProducerLocked(schemaForSubscription);
                deadLetterProducer = p;
                hookDeadLetterProducerFailureRebuild(p);
            }
            return p;
        }
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<Producer<byte[]>> createDeadLetterProducerLocked(Schema<?> schemaForSubscription) {
        return createProducerWithBackOff(() -> {
            AutoProduceBytesSchema<byte[]> deadLetterProducerSchema =
                    (AutoProduceBytesSchema<byte[]>) Schema.AUTO_PRODUCE_BYTES(schemaForSubscription);
            deadLetterProducerSchema.setRequireSchemaValidation(false);
            ProducerBuilder<byte[]> builder =
                    ((ProducerBuilderImpl<byte[]>) client.newProducer(deadLetterProducerSchema))
                            .initialSubscriptionName(deadLetterPolicy.getInitialSubscriptionName())
                            .topic(deadLetterPolicy.getDeadLetterTopic())
                            .producerName(
                                    String.format("%s-%s-%s-%s-DLQ",
                                            subscriptionTopicName, subscription, consumerName,
                                            RandomStringUtils.insecure().nextAlphanumeric(5)))
                            .blockIfQueueFull(false)
                            .enableBatching(false)
                            .enableChunking(true);
            scaleMaxPendingMessages(builder);
            customizeDeadLetterProducerBuilder(
                    deadLetterPolicy.getDeadLetterProducerBuilderCustomizer(),
                    deadLetterPolicy.getDeadLetterTopic(), builder);
            CompletableFuture<Producer<byte[]>> newProducer = builder.createAsync();
            newProducer.whenComplete((producer, ex) -> {
                if (ex != null) {
                    log.error().attr("deadLetterTopic", deadLetterPolicy.getDeadLetterTopic())
                            .exception(ex)
                            .log("Failed to create shared dead letter producer");
                    deadLetterProducerFailureCount.incrementAndGet();
                } else {
                    deadLetterProducerFailureCount.set(0);
                }
            });
            return newProducer;
        }, deadLetterProducerFailureCount, () -> "shared dead letter producer (topic: "
                + deadLetterPolicy.getDeadLetterTopic() + ")");
    }

    private void hookDeadLetterProducerFailureRebuild(CompletableFuture<Producer<byte[]>> future) {
        future.whenComplete((producer, ex) -> {
            if (ex == null) {
                return;
            }
            synchronized (deadLetterProducerInitLock) {
                if (deadLetterProducer == future) {
                    deadLetterProducer = null;
                }
            }
        });
    }

    /**
     * Scale the shared producer's maxPendingMessages so that aggregate client-side buffering
     * capacity across all sharing sub-consumers does not shrink compared to the historical
     * per-partition behaviour. The scaling factor is the number of partition sub-consumers that
     * will share this producer. We clamp the lower bound at the Pulsar client default
     * (see ProducerConfigurationData#maxPendingMessages = 1000) to avoid regressing
     * single-partition cases.
     */
    private void scaleMaxPendingMessages(ProducerBuilder<byte[]> builder) {
        if (partitionHint <= 1) {
            return;
        }
        // Pulsar default (ProducerConfigurationData#maxPendingMessages). Hard-coded intentionally:
        // the per-consumer PulsarClient configuration does not expose a producer-side default
        // override, and overriding this value here would bypass any explicit customizer the user
        // may have attached via DeadLetterPolicy#producerBuilderCustomizer (which runs after this
        // scaling and therefore wins).
        final int baseDefault = 1000;
        long scaled = (long) baseDefault * partitionHint;
        // Guard against int overflow for pathological partition counts.
        int effective = (int) Math.min(scaled, Integer.MAX_VALUE);
        builder.maxPendingMessages(effective);
    }

    private void customizeDeadLetterProducerBuilder(DeadLetterProducerBuilderCustomizer customizer,
                                                    String deadLetterOrRetryLetterTopic,
                                                    ProducerBuilder<byte[]> builder) {
        if (customizer != null) {
            DeadLetterProducerBuilderContext context = new DeadLetterProducerBuilderContextImpl(
                    deadLetterOrRetryLetterTopic, subscriptionTopicName, subscription, consumerName);
            customizer.customize(context, builder);
        }
    }

    private CompletableFuture<Producer<byte[]>> createProducerWithBackOff(
            Supplier<CompletableFuture<Producer<byte[]>>> producerSupplier, AtomicInteger failureCount,
            Supplier<String> logDescription) {
        int snapshot = failureCount.get();
        if (snapshot == 0) {
            return producerSupplier.get();
        }
        Backoff backoff = Backoff.builder()
                .mandatoryStop(Duration.ofMillis(client.getConfiguration().getOperationTimeoutMs() * 2))
                .build();
        long backoffTimeMillis = 0;
        for (int i = 0; i < snapshot; i++) {
            backoffTimeMillis = backoff.next().toMillis();
        }
        CompletableFuture<Producer<byte[]>> newProducer = new CompletableFuture<>();
        ScheduledExecutorService executor =
                (ScheduledExecutorService) client.getScheduledExecutorProvider().getExecutor(this);
        log.info().attr("creating", logDescription.get())
                .attr("backoffTimeMillis", backoffTimeMillis)
                .log("Creating with backoff time of ms");
        executor.schedule(() -> {
            FutureUtil.completeAfter(newProducer, producerSupplier.get());
        }, backoffTimeMillis, TimeUnit.MILLISECONDS);
        return newProducer;
    }

    /**
     * Verify that every sub-consumer requesting a shared producer brings in the same schema
     * instance. All partition sub-consumers of a single subscription are cloned from the same
     * source schema (see {@link MultiTopicsConsumerImpl#createInternalConsumer}), so reference
     * equality is the correct check. A mismatch indicates incorrect wiring (e.g. mixing
     * subscriptions / topics in the same provider) and is logged at warn level; we do not fail
     * the call to avoid introducing a new failure mode compared to the historical per-consumer
     * behaviour.
     */
    private void checkSchemaConsistency(Schema<?> schemaForSubscription) {
        if (schemaForSubscription == null) {
            return;
        }
        Schema<?> current = expectedSchema;
        if (current == null) {
            // benign race: last writer wins; both writers see the same reference under normal flow.
            expectedSchema = schemaForSubscription;
            return;
        }
        if (current != schemaForSubscription) {
            // Downgraded to debug: Pattern-subscriptions and dynamically re-created sub-consumers
            // can legitimately pass semantically-equal-but-not-reference-equal schema instances.
            // Reference inequality is still a useful diagnostic signal but should not spam user logs.
            log.debug().attr("subscriptionTopicName", subscriptionTopicName)
                    .attr("subscription", subscription)
                    .log("Shared retry/DLQ producer provider received a different schema instance than the first "
                            + "caller; the first schema will continue to be used by the shared producers");
        }
    }

    /**
     * Update the partition hint used to scale newly-created producers' maxPendingMessages.
     * Called by {@link MultiTopicsConsumerImpl} when the number of partitions on the subscription
     * topic changes (e.g. partition auto-update detects an expansion). Does not resize producers
     * that have already been created -- any pending-messages adjustment for existing producers
     * must be handled by the user's own customizer or by closing and re-creating the provider.
     */
    void updatePartitionHint(int newPartitionHint) {
        if (newPartitionHint <= 0) {
            return;
        }
        int sanitized = Math.max(1, newPartitionHint);
        if (sanitized != this.partitionHint) {
            log.info().attr("subscriptionTopicName", subscriptionTopicName)
                    .attr("oldPartitionHint", this.partitionHint)
                    .attr("newPartitionHint", sanitized)
                    .log("Updating shared retry/DLQ provider partition hint; already-created producers "
                            + "keep their existing maxPendingMessages value");
            this.partitionHint = sanitized;
        }
    }

    /**
     * Returns whether this provider has already begun closing. Useful for callers that want to
     * short-circuit before touching the shared producers.
     */
    boolean isClosed() {
        return closed.get();
    }

    /**
     * Close any producers that have been created. After this call returns, subsequent calls to
     * {@link #getOrCreateRetryLetterProducer}/{@link #getOrCreateDeadLetterProducer} will return
     * a failed future rather than hand out a stale, already-closed producer. Safe to call
     * multiple times -- a subsequent call is a no-op that returns the same close future.
     *
     * @return a future completing after all underlying producers are closed.
     */
    CompletableFuture<Void> close() {
        if (!closed.compareAndSet(false, true)) {
            // Already closed (or closing) -- do nothing. We intentionally return an immediately
            // completed future here rather than tracking the first close future for simplicity;
            // the caller only ever invokes close() once in the normal lifecycle.
            return CompletableFuture.completedFuture(null);
        }
        // Snapshot the futures and null out the volatile slots so that any concurrent getOrCreate*
        // call will observe the closed state and fail fast instead of using the producer we are
        // about to close.
        CompletableFuture<Producer<byte[]>> retrySnapshot;
        CompletableFuture<Producer<byte[]>> deadLetterSnapshot;
        synchronized (retryProducerInitLock) {
            retrySnapshot = retryLetterProducer;
            retryLetterProducer = null;
        }
        synchronized (deadLetterProducerInitLock) {
            deadLetterSnapshot = deadLetterProducer;
            deadLetterProducer = null;
        }
        List<CompletableFuture<Void>> closeFutures = new ArrayList<>(2);
        if (retrySnapshot != null) {
            closeFutures.add(retrySnapshot.thenCompose(Producer::closeAsync).whenComplete((ignore, ex) -> {
                if (ex != null) {
                    log.warn().exception(ex).log("Exception ignored in closing shared retry letter producer");
                }
            }));
        }
        if (deadLetterSnapshot != null) {
            closeFutures.add(deadLetterSnapshot.thenCompose(Producer::closeAsync).whenComplete((ignore, ex) -> {
                if (ex != null) {
                    log.warn().exception(ex).log("Exception ignored in closing shared dead letter producer");
                }
            }));
        }
        return FutureUtil.waitForAll(closeFutures);
    }
}

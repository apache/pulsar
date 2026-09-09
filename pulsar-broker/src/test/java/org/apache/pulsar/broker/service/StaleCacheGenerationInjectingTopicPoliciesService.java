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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

/**
 * A {@link SystemTopicBasedTopicPoliciesService} that, while armed, replays a namespace-bundle bounce before it
 * completes the {@code prepareInitPoliciesCacheAsync(namespace)} future that
 * {@link SystemTopicBasedTopicPoliciesService#getTopicPoliciesAsync} awaits. The continuation that re-derives that
 * awaited guarantee from {@code policyCacheInitMap} after its thread hop is therefore guaranteed -- not merely
 * likely -- to resume onto a replaced, still-loading generation, which is why the reproduction needs no sleep and no
 * timing tuning. The bounce is two production calls and nothing else:
 * {@code cleanPoliciesCacheInitMap(namespace)}, which drops the namespace's cached policies together with the future
 * tracking their load, then {@code prepareInitPoliciesCacheAsync(namespace)}, which installs a new and still-loading
 * one. No cache is edited by hand, nothing is stubbed and no failure is injected; only the moment at which those two
 * production methods run is chosen.
 *
 * <p>A bounce fires only once the generation owning the namespace has completed successfully -- the production
 * precondition being modelled -- one at a time, and at most as often as the budget passed to
 * {@link #arm(NamespaceName, int)} allows, so a broker that retries its way out of the stale read converges instead
 * of being perturbed for ever.
 */
class StaleCacheGenerationInjectingTopicPoliciesService extends SystemTopicBasedTopicPoliciesService {

    private static final long GENERATION_INSTALL_TIMEOUT_MILLIS = 10_000;
    private static final long GENERATION_INSTALL_POLL_MILLIS = 5;

    private final PulsarService pulsar;
    private final Object lock = new Object();
    private final AtomicInteger staleInjections = new AtomicInteger();
    private NamespaceName armedNamespace;
    private int remainingBudget;
    private boolean bounceInFlight;

    StaleCacheGenerationInjectingTopicPoliciesService(PulsarService pulsar) {
        super(pulsar);
        this.pulsar = pulsar;
    }

    /**
     * Starts interleaving a bundle bounce into the policy reads of {@code namespace}, at most {@code budget} times.
     */
    void arm(NamespaceName namespace, int budget) {
        synchronized (lock) {
            armedNamespace = namespace;
            remainingBudget = budget;
        }
    }

    /** Stops interleaving bounces; the service then behaves exactly like its superclass. */
    void disarm() {
        synchronized (lock) {
            armedNamespace = null;
            remainingBudget = 0;
        }
    }

    /**
     * How many bounces were handed back to the caller in the state under test: replacement generation installed and
     * still loading. A bounce that misses that state leaves the resumed read correct even on unpatched code, so a
     * test asserting on this counter cannot pass vacuously.
     */
    int staleInjectionCount() {
        return staleInjections.get();
    }

    /**
     * Waits until the namespace's first generation has finished loading {@code topicName}'s policy, which is the
     * precondition every bounce needs. Until that policy is in the loaded cache, a read that resumes on a wiped cache
     * is not distinguishable from a legitimate empty read, so arming earlier would race the policy write instead of
     * testing the window.
     */
    void awaitGenerationLoaded(NamespaceName namespace, TopicName topicName) {
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            Assertions.assertThat(getPoliciesCacheInit(namespace))
                    .describedAs("the namespace's policy-cache generation has not loaded successfully yet")
                    .isCompleted();
            Assertions.assertThat(TopicPolicyTestUtils.getLocalTopicPolicies(this, topicName))
                    .describedAs("the topic's policy is not in the loaded cache yet")
                    .isNotNull();
        });
    }

    @Override
    CompletableFuture<Boolean> prepareInitPoliciesCacheAsync(NamespaceName namespace) {
        final CompletableFuture<Boolean> prepared = super.prepareInitPoliciesCacheAsync(namespace);
        synchronized (lock) {
            if (!namespace.equals(armedNamespace)) {
                return prepared;
            }
        }
        // Bounce only after the caller's guarantee has genuinely been satisfied, and hand back the value the
        // production method computed: the caller still believes it awaited the generation it prepared, which is
        // exactly the state this defect acts on.
        return prepared.thenCompose(inserted -> bounceNamespaceBundle(namespace).thenApply(__ -> inserted));
    }

    /**
     * Replays the two production calls of a bounce, then waits until a generation owns the namespace again so the
     * caller never resumes into the (harmless) missing-generation case. One bounce at a time: a second bounce landing
     * while a replacement is still loading would drop and fail that replacement, aborting an unrelated in-flight read.
     * Both production calls run outside the monitor, since completing an init future runs the topic loads awaiting it.
     */
    private CompletableFuture<Void> bounceNamespaceBundle(NamespaceName namespace) {
        final boolean bounce;
        synchronized (lock) {
            final CompletableFuture<Void> generation = getPoliciesCacheInit(namespace);
            final boolean generationLoaded =
                    generation != null && generation.isDone() && !generation.isCompletedExceptionally();
            bounce = !bounceInFlight && namespace.equals(armedNamespace) && remainingBudget > 0 && generationLoaded;
            if (bounce) {
                bounceInFlight = true;
                remainingBudget--;
            }
        }
        if (!bounce) {
            return CompletableFuture.completedFuture(null);
        }
        cleanPoliciesCacheInitMap(namespace);
        // Fire and forget, exactly like the bundle-load path: the caller is not awaiting this generation.
        super.prepareInitPoliciesCacheAsync(namespace).exceptionally(ignored -> null);
        return awaitGenerationInstalled(namespace,
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(GENERATION_INSTALL_TIMEOUT_MILLIS))
                .whenComplete((__, ignored) -> {
                    final CompletableFuture<Void> replacement = getPoliciesCacheInit(namespace);
                    if (replacement != null && !replacement.isDone()) {
                        staleInjections.incrementAndGet();
                    }
                    synchronized (lock) {
                        bounceInFlight = false;
                    }
                });
    }

    /** Never blocks the broker thread the caller runs on, and never fails: on timeout it lets the caller proceed. */
    private CompletableFuture<Void> awaitGenerationInstalled(NamespaceName namespace, long deadlineNanos) {
        final CompletableFuture<Void> installed = new CompletableFuture<>();
        pollGenerationInstalled(namespace, deadlineNanos, installed);
        return installed;
    }

    /** One poll, rescheduled onto the broker executor so neither the stack nor the pending futures grow with time. */
    private void pollGenerationInstalled(NamespaceName namespace, long deadlineNanos,
                                         CompletableFuture<Void> installed) {
        if (getPoliciesCacheInit(namespace) != null || System.nanoTime() - deadlineNanos >= 0) {
            installed.complete(null);
            return;
        }
        try {
            pulsar.getExecutor().schedule(() -> pollGenerationInstalled(namespace, deadlineNanos, installed),
                    GENERATION_INSTALL_POLL_MILLIS, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // The broker is shutting down: let the caller proceed instead of leaving its read pending for ever.
            installed.complete(null);
        }
    }
}

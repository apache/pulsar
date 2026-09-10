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

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

/**
 * A {@link SystemTopicBasedTopicPoliciesService} that opens a controlled <em>stale-read window</em> around the thread
 * hop in {@link SystemTopicBasedTopicPoliciesService#getTopicPoliciesAsync}: a read is held until a namespace-bundle
 * bounce has installed a replacement policy-cache generation <em>whose reader is gated</em>, and that gate is
 * released only once the read has been observed to reach its decision. Both halves are production behaviour; only
 * the moment at which they run is chosen. No cache is edited by hand, nothing is stubbed, no failure is injected.
 *
 * <p>A window opens when a read resumes from a generation it awaited successfully, which is the precondition the
 * defect needs. Opening it replays the two calls a bundle bounce performs and nothing else:
 * {@code cleanPoliciesCacheInitMap(namespace)} drops the namespace's cached policies together with the future
 * tracking their load, then {@code prepareInitPoliciesCacheAsync(namespace)} installs a new, still-loading one. The
 * reader that replacement creates is wrapped so that it reads nothing until the gate is released, which means the
 * caches hold nothing for the namespace for as long as the window is open. The read is let go only once that
 * replacement generation is installed and its reader creation has been gated -- the reader is wrapped on arrival,
 * whether or not it has connected yet -- so the continuation that re-derives the read's guarantee from
 * {@code policyCacheInitMap} is guaranteed -- not merely likely -- to evaluate its predicate against an installed,
 * incomplete generation over an empty cache. A read resuming from the same generation completion as the one that
 * opened the window -- the sibling of the two concurrent policy fetches a topic load issues -- joins that window if
 * it gets there before the window closes, and otherwise re-awaits the replacement generation and then opens or joins
 * the next one. Every read of an armed namespace therefore reaches its decision inside a held window, unless the
 * budget is spent or the namespace has no generation it could be held against, in which case it is passed through
 * untouched to the production code: a loaded generation answers it from the cache, a missing, still-loading or failed
 * one takes the production retry branch.
 *
 * <p>The gate is released once every read in the window has been observed to decide. A decision is either the
 * production retry -- a re-entrant {@code getTopicPoliciesAsync} call with the same topic and
 * {@link TopicPoliciesService.GetType}, which is exactly what the retry branch performs -- or the completion of the
 * read's own future, which means the read was answered from the wiped cache instead. Nothing here depends on how
 * fast the replacement reader is or on how the common pool schedules the continuation, and
 * {@link #staleWindowCount()}, {@link #retriesInsideStaleWindow()} and {@link #readsServedInsideStaleWindow()} count
 * those decisions rather than sampling state a faster reader could already have changed.
 *
 * <p>Windows open one at a time per namespace, only while armed, and at most as often as the budget passed to
 * {@link #arm(NamespaceName, int)} allows, so a broker that retries its way out of the stale read converges instead
 * of being perturbed for ever. A gate is held only until the reads its window holds have decided, one decision per
 * read -- orders of magnitude below {@code topicPoliciesCacheInitTimeoutSeconds} (60 s by default), which is what
 * bounds a generation that never finishes loading -- and both {@link #disarm()} and {@link #close()} release every
 * window still open, so cleanup can never hang on a held gate.
 */
@Slf4j
class StaleCacheGenerationInjectingTopicPoliciesService extends SystemTopicBasedTopicPoliciesService {

    private final PulsarService pulsar;

    /**
     * Guards {@link #armedNamespace}, {@link #remainingBudget}, {@link #openWindows} and the fields of an open window.
     * No call that can run callbacks or take another lock, and no future completion, happens while it is held, so it
     * can never take part in a lock cycle with the maps the service itself locks.
     */
    private final Object lock = new Object();

    private NamespaceName armedNamespace;
    private int remainingBudget;
    private final Map<NamespaceName, StaleWindow> openWindows = new HashMap<>();

    private final AtomicInteger staleWindowsOpened = new AtomicInteger();
    private final AtomicInteger retriesInsideWindow = new AtomicInteger();
    private final AtomicInteger readsServedInsideWindow = new AtomicInteger();
    private final AtomicInteger readsReawaited = new AtomicInteger();

    /**
     * Links a {@link #getTopicPoliciesAsync} call to the {@code prepareInitPoliciesCacheAsync} call it makes: the
     * production method makes that call synchronously, on the caller's thread, before any thread hop. It is the only
     * way that override can know which read it is serving; the calls made when a bundle is loaded see null and are
     * left alone, which is correct since they never reach the continuation under test.
     */
    private final ThreadLocal<ReadContext> currentRead = new ThreadLocal<>();

    StaleCacheGenerationInjectingTopicPoliciesService(PulsarService pulsar) {
        super(pulsar);
        this.pulsar = pulsar;
    }

    /** Starts opening stale-read windows in the policy reads of {@code namespace}, at most {@code budget} of them. */
    void arm(NamespaceName namespace, int budget) {
        synchronized (lock) {
            armedNamespace = namespace;
            remainingBudget = budget;
        }
    }

    /**
     * Stops opening windows and releases any window still open, so no reader stays gated and no read stays held once
     * the scenario under test is over. The service then opens no window and gates no reader.
     */
    void disarm() {
        final List<StaleWindow> abandoned;
        synchronized (lock) {
            armedNamespace = null;
            remainingBudget = 0;
            abandoned = List.copyOf(openWindows.values());
            openWindows.clear();
            // Their reads are no longer observed, so their late decisions must not be counted either.
            abandoned.forEach(window -> window.undecidedReads.clear());
        }
        abandoned.forEach(this::abandon);
    }

    /**
     * How many stale-read windows were opened: how many times a read was held while a replacement generation was
     * installed with its reader gated. Zero means the interleaving under test never happened, so a test asserting on
     * the outcome of such a read would hold vacuously.
     */
    int staleWindowCount() {
        return staleWindowsOpened.get();
    }

    /**
     * How many reads decided, inside a window, to retry: the production retry re-enters
     * {@code getTopicPoliciesAsync} with the same topic and type after refusing to read the caches of the installed
     * but still-loading generation. The number is exact rather than sampled, because the gate keeps that generation
     * unable to make progress until the decision has been observed.
     */
    int retriesInsideStaleWindow() {
        return retriesInsideWindow.get();
    }

    /**
     * How many reads were, inside a window, completed instead of retried: answered from the wiped cache of the
     * installed but still-loading generation (an exceptional completion counts here too, since it also ends the
     * read). Exact for the same reason as {@link #retriesInsideStaleWindow()}, and the defect this fixture
     * reproduces is precisely a non-zero value here.
     */
    int readsServedInsideStaleWindow() {
        return readsServedInsideWindow.get();
    }

    /**
     * Waits until the namespace's first generation has finished loading {@code topicName}'s policy, which is the
     * precondition every window needs. Until that policy is in the loaded cache, a read that resumes on a wiped cache
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
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type) {
        final ReadKey key = new ReadKey(topicName, type);
        // Before anything else: a call carrying a key that is still undecided in an open window is the retry branch
        // re-entering this method, which is one of the two decisions this fixture waits for (see
        // observeRetryDecision for the one assumption that inference makes about the caller).
        observeRetryDecision(key);
        final ReadContext context = new ReadContext(key);
        final CompletableFuture<Optional<TopicPolicies>> read;
        currentRead.set(context);
        try {
            read = super.getTopicPoliciesAsync(topicName, type);
        } finally {
            currentRead.remove();
        }
        // The other decision: the read was answered rather than retried.
        return read.whenComplete((policies, failure) -> observeServedDecision(context, failure));
    }

    @Override
    CompletableFuture<Boolean> prepareInitPoliciesCacheAsync(NamespaceName namespace) {
        // Consumed here: only the call getTopicPoliciesAsync makes on this thread belongs to that read, so a later
        // or nested call on the same thread must not inherit it.
        final ReadContext context = currentRead.get();
        if (context != null) {
            currentRead.remove();
        }
        final CompletableFuture<Boolean> prepared = super.prepareInitPoliciesCacheAsync(namespace);
        if (context == null) {
            return prepared;
        }
        // A false value means the service is closed or the namespace is being deleted: the production method awaited
        // no generation then, so there is no window to open and nothing to observe.
        return prepared.thenCompose(inserted -> inserted
                ? resumeIntoStaleWindow(context).thenApply(__ -> inserted)
                : CompletableFuture.completedFuture(inserted));
    }

    @Override
    protected CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> createSystemTopicClient(
            NamespaceName namespace) {
        final StaleWindow window;
        synchronized (lock) {
            final StaleWindow open = openWindows.get(namespace);
            window = open != null && !open.readerGated ? open : null;
            if (window != null) {
                window.readerGated = true;
            }
        }
        final CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerFuture =
                super.createSystemTopicClient(namespace);
        if (window == null) {
            return readerFuture;
        }
        // The replacement generation is in policyCacheInitMap by now -- it is put there before its reader is created
        // -- and that reader is gated below, so the reads this window holds can be let go. This method runs inside
        // readerCaches.computeIfAbsent, i.e. under a ConcurrentHashMap bin lock, and the dependents of `installed`
        // are those reads: hand the completion to the broker executor so that none of them runs under that lock.
        try {
            window.installed.completeAsync(() -> null, pulsar.getExecutor());
        } catch (RejectedExecutionException e) {
            // The broker is shutting down; complete inline rather than wait for the failed replacement to release
            // the read.
            window.installed.complete(null);
        }
        return readerFuture.thenApply(reader -> new GatedReader(reader, window.gate));
    }

    @Override
    public void close() throws Exception {
        disarm();
        super.close();
    }

    /**
     * Opens, joins, or waits for the window a read resumes into. Called once the generation the read awaited has
     * completed, which is the state the defect acts on. The returned future completes when the replacement
     * generation is installed and its reader gated (or, if that replacement never reaches its reader, when its own
     * future settles), so the read's continuation can only see an incomplete -- or, on those short-circuits, a
     * missing or failed -- generation, never a loaded one; a pass-through returns an already completed future and
     * the read proceeds untouched.
     */
    private CompletableFuture<Void> resumeIntoStaleWindow(ReadContext context) {
        final NamespaceName namespace = context.key.namespace();
        final CompletableFuture<Void> currentGeneration;
        final StaleWindow window;
        final boolean openedHere;
        final boolean awaitReplacement;
        synchronized (lock) {
            // Read under the monitor, together with the decision below, so that a sibling resuming on another thread
            // can never decide on a generation the opener has meanwhile replaced. This is a lock-free map read, not a
            // call that can run callbacks or take another lock, so holding the monitor across it cannot deadlock. In
            // the scenarios this fixture serves it is the generation the read has just awaited; the loaded check is
            // what makes sure a window only ever opens over a loaded generation, which is the defect's precondition.
            currentGeneration = getPoliciesCacheInit(namespace);
            final boolean generationLoaded = currentGeneration != null && currentGeneration.isDone()
                    && !currentGeneration.isCompletedExceptionally();
            final StaleWindow open = openWindows.get(namespace);
            final boolean windowAffordable = namespace.equals(armedNamespace) && remainingBudget > 0;
            if (open != null) {
                open.undecidedReads.add(context);
                context.window = open;
                window = open;
                openedHere = false;
                awaitReplacement = false;
            } else if (windowAffordable && generationLoaded) {
                remainingBudget--;
                staleWindowsOpened.incrementAndGet();
                window = new StaleWindow(namespace);
                window.undecidedReads.add(context);
                context.window = window;
                openWindows.put(namespace, window);
                openedHere = true;
                awaitReplacement = false;
            } else if (windowAffordable && currentGeneration != null && !currentGeneration.isDone()) {
                // The generation this read resumed from has already been replaced by a window that opened AND closed
                // while this continuation was still queued, which is what the second of two concurrent sibling reads
                // sees whenever the first one decides before the sibling gets here. Letting it through would leave
                // that read unheld and uncounted, so wait for the replacement instead: the retry below turns it back
                // into a read resuming from a loaded generation, and it then opens its own window exactly as the
                // sequential case does. Bounded, since every window costs a budget unit and a settled generation
                // never comes back here.
                readsReawaited.incrementAndGet();
                window = null;
                openedHere = false;
                awaitReplacement = true;
            } else {
                // No generation at all, a failed one, or nothing left to spend: pass the read through and let the
                // production predicate decide.
                return CompletableFuture.completedFuture(null);
            }
        }
        if (awaitReplacement) {
            log.info("stale-window re-awaited: this read resumed from a generation a closed window replaced "
                    + "namespace={} topic={} type={} readsReawaited={}",
                    namespace, context.key.topicName(), context.key.type(), readsReawaited.get());
            // Outside the monitor. handle() rather than exceptionally(): a generation that failed must release this
            // read just as a loaded one does, and the recursion below decides what to do with it.
            return currentGeneration.handle((__, ignored) -> null)
                    .thenCompose(__ -> resumeIntoStaleWindow(context));
        }
        if (!openedHere) {
            log.info("stale-window joined by a concurrent read resuming from the same generation "
                    + "namespace={} topic={} type={}",
                    namespace, context.key.topicName(), context.key.type());
            return window.installed;
        }
        log.info("stale-window opening: replaying a namespace-bundle bounce and gating the replacement reader "
                + "namespace={} topic={} type={} windowsOpened={}",
                namespace, context.key.topicName(), context.key.type(), staleWindowsOpened.get());
        cleanPoliciesCacheInitMap(namespace);
        super.prepareInitPoliciesCacheAsync(namespace)
                // Fire and forget, exactly like the bundle-load path: nobody awaits this generation. Completing
                // `installed` from here as well bounds the wait, so a replacement that never reaches its reader
                // (namespace deleted, service closed, reader creation failed) releases the reads instead of
                // stranding them on a window whose reader never came.
                .whenComplete((__, ignored) -> window.installed.complete(null))
                .exceptionally(ignored -> null);
        return window.installed;
    }

    /**
     * The production retry is a re-entrant call of {@code getTopicPoliciesAsync} with the same topic and type, made
     * right after the predicate refused the still-loading generation. The original call registered its key before it
     * was allowed to resume, so a call carrying a key that is undecided in the open window cannot be that original
     * call: it is its retry. That inference relies on no other caller reading the same topic and type while a window
     * holds one undecided, which is what the scenarios this fixture serves do -- a single direct read, and a topic
     * load whose stages are sequential and whose concurrent pairs differ by {@link TopicPoliciesService.GetType}.
     */
    private void observeRetryDecision(ReadKey key) {
        final CompletableFuture<Void> gate;
        final boolean windowClosed;
        synchronized (lock) {
            final StaleWindow window = openWindows.get(key.namespace());
            if (window == null || !window.removeUndecided(key)) {
                return;
            }
            retriesInsideWindow.incrementAndGet();
            gate = closeIfAllDecided(window);
            windowClosed = gate != null;
        }
        log.info("stale-window decision: the read refused the still-loading generation and retried "
                + "namespace={} topic={} type={} windowClosed={} retries={}",
                key.namespace(), key.topicName(), key.type(), windowClosed, retriesInsideWindow.get());
        releaseGate(gate);
    }

    /**
     * The other decision: the read completed instead of retrying, so it was answered from the wiped cache of the
     * still-loading generation. A read that retried already had its context removed, so its later completion is
     * ignored; an exceptional completion still counts, because a gate must never outlive the read holding it.
     */
    private void observeServedDecision(ReadContext context, Throwable failure) {
        final CompletableFuture<Void> gate;
        final boolean windowClosed;
        synchronized (lock) {
            final StaleWindow window = context.window;
            if (window == null || !window.undecidedReads.remove(context)) {
                return;
            }
            readsServedInsideWindow.incrementAndGet();
            gate = closeIfAllDecided(window);
            windowClosed = gate != null;
        }
        log.info("stale-window decision: the read was answered from the cache of the still-loading generation "
                + "namespace={} topic={} type={} failed={} windowClosed={} readsServed={}",
                context.key.namespace(), context.key.topicName(), context.key.type(), failure != null,
                windowClosed, readsServedInsideWindow.get());
        releaseGate(gate);
    }

    /**
     * Must be called while holding {@link #lock}. Returns the gate of a window whose reads have all decided, for the
     * caller to release once outside the monitor, or null while reads are still undecided.
     */
    private CompletableFuture<Void> closeIfAllDecided(StaleWindow window) {
        if (!window.undecidedReads.isEmpty()) {
            return null;
        }
        openWindows.remove(window.namespace, window);
        return window.gate;
    }

    /** Releases the gated reader, which then drains the namespace's events and completes its generation. */
    private void releaseGate(CompletableFuture<Void> gate) {
        if (gate != null) {
            gate.complete(null);
        }
    }

    /** Lets go of a window that is being torn down: nothing may stay blocked on a gate nobody will release. */
    private void abandon(StaleWindow window) {
        log.info("stale-window abandoned: releasing its gate because the fixture is disarmed or closed "
                + "namespace={}",
                window.namespace);
        window.installed.complete(null);
        window.gate.complete(null);
    }

    /** What the production retry re-enters {@code getTopicPoliciesAsync} with, and the only link back to a read. */
    private record ReadKey(TopicName topicName, GetType type) {

        NamespaceName namespace() {
            return topicName.getNamespaceObject();
        }
    }

    /** One {@code getTopicPoliciesAsync} call, and the window it resumed into. */
    private static final class ReadContext {

        private final ReadKey key;
        /** Written and read while holding the fixture's monitor only. */
        private StaleWindow window;

        private ReadContext(ReadKey key) {
            this.key = key;
        }
    }

    /**
     * One open window: a replacement generation installed with its reader gated, and the reads held for it.
     * {@code readerGated} and {@code undecidedReads} are accessed only while holding the fixture's monitor; the two
     * futures and the namespace are final, and the futures are completed only outside it.
     */
    private static final class StaleWindow {

        private final NamespaceName namespace;
        /** Released once every read in the window has decided; until then the replacement reader reads nothing. */
        private final CompletableFuture<Void> gate = new CompletableFuture<>();
        /** Completed once the replacement generation is installed and gated; until then the reads stay held. */
        private final CompletableFuture<Void> installed = new CompletableFuture<>();
        private final Set<ReadContext> undecidedReads = new LinkedHashSet<>();
        private boolean readerGated;

        private StaleWindow(NamespaceName namespace) {
            this.namespace = namespace;
        }

        /**
         * Removes one read that this window holds for {@code key}. Reads sharing a key are interchangeable here:
         * each of them decides exactly once, so the totals stay exact whichever one a retry is attributed to.
         */
        private boolean removeUndecided(ReadKey key) {
            final Iterator<ReadContext> undecided = undecidedReads.iterator();
            while (undecided.hasNext()) {
                if (undecided.next().key.equals(key)) {
                    undecided.remove();
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * A reader that reads nothing until its gate is released. This is what makes the window deterministic:
     * {@code initPolicesCache} calls {@link #hasMoreEventsAsync()} first, so a gated reader has not touched a single
     * event and the caches hold nothing for its namespace while the reads the window holds reach their decision.
     * Closing is never gated, so tearing the service down cannot block on a gate.
     */
    private static final class GatedReader implements SystemTopicClient.Reader<PulsarEvent> {

        private final SystemTopicClient.Reader<PulsarEvent> delegate;
        private final CompletableFuture<Void> gate;

        private GatedReader(SystemTopicClient.Reader<PulsarEvent> delegate, CompletableFuture<Void> gate) {
            this.delegate = delegate;
            this.gate = gate;
        }

        /**
         * Never called: the service drives its readers asynchronously throughout, and gating a blocking call would
         * park whichever broker thread made it until the window closes. Fail loudly instead.
         */
        @Override
        public Message<PulsarEvent> readNext() {
            throw new UnsupportedOperationException("the stale-read window gates the asynchronous reader only; a"
                    + " blocking readNext() would park a broker thread until the window closes");
        }

        @Override
        public CompletableFuture<Message<PulsarEvent>> readNextAsync() {
            return gate.thenCompose(__ -> delegate.readNextAsync());
        }

        /**
         * Never called: the service drives its readers asynchronously throughout, and gating a blocking call would
         * park whichever broker thread made it until the window closes. Fail loudly instead.
         */
        @Override
        public boolean hasMoreEvents() {
            throw new UnsupportedOperationException("the stale-read window gates the asynchronous reader only; a"
                    + " blocking hasMoreEvents() would park a broker thread until the window closes");
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            return gate.thenCompose(__ -> delegate.hasMoreEventsAsync());
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return delegate.closeAsync();
        }

        @Override
        public SystemTopicClient<PulsarEvent> getSystemTopic() {
            return delegate.getSystemTopic();
        }
    }
}

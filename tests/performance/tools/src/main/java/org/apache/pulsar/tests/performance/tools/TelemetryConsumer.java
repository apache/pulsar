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
package org.apache.pulsar.tests.performance.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.util.RetryMessageUtil;
import picocli.CommandLine.Command;

/**
 * Runs every application of the scenario in this JVM, as the gateways run in one. An application is a subscription
 * of its own on every topic, of the type {@code applications.subscriptionType}, consumed through its pods, each a
 * Pulsar client with a consumer of the subscription; the applications differ only in their subscription. Every pod's
 * client shares one set of client resources, the event loop and the thread pools, sized by
 * {@code applications.client}.
 *
 * <p>Each application checks its own delivery and, unless its subscription is Shared, ordering, and writes its outputs
 * into a directory named after its subscription in {@code --output}, as the run report names it. The progress stream
 * sums the applications' counts.
 *
 * <p>An application with {@code applications.joinSeconds} after the measurement's start creates its subscription at
 * startup, so that the backlog builds up from the start, and opens its pods when it joins. It records when it joined
 * and when it caught up: when it first received a measured message within {@code applications.caughtUpLatencyMillis}
 * of its publishing. When the launcher prepares the join, by moving the late applications' subscriptions forward with
 * {@code applications.skipMessages} or unloading the topics with {@code applications.unloadBeforeJoin}, the late
 * applications wait for it at their join time; one that skips messages expects every message but those, each
 * device's from the first one it receives.
 */
@Command(name = "iot-consume", description = "Run the IoT applications, each consuming and validating its own "
        + "subscription")
final class TelemetryConsumer extends PerformanceTool.ScenarioCommand {
    // The most applications that open their pods at the same time; each opens its pods one after another
    private static final int MAX_PARALLEL_STARTS = 32;
    // Starts a line of the workload's startup progress, which the launcher shows on its console
    static final String PROGRESS_PREFIX = "PROGRESS ";
    private static final long STARTUP_PROGRESS_INTERVAL_SECONDS = 5;

    @Override
    public Integer call() throws Exception {
        IotScenario scenario = scenario();
        List<Application> applications = new ArrayList<>(scenario.applicationCount());
        AtomicReference<String> phase = new AtomicReference<>("connecting");
        ProgressStream progress = null;
        MeasurementControl control = null;
        PulsarClientSharedResources sharedResources = null;
        try {
            for (int index = 0; index < scenario.applicationCount(); index++) {
                Path applicationOutput = output.resolve(scenario.subscriptionName(index));
                Files.createDirectories(applicationOutput);
                applications.add(new Application(scenario, index, applicationOutput, coordinationDirectory(), runId));
            }
            progress = new ProgressStream(applications.stream().map(Application::receiveLatency).toList(),
                    line -> status(line, scenario, applications, phase.get()));
            if (controlPort != null) {
                control = MeasurementControl.start(controlPort);
                control.serveProgress(progress);
            }
            sharedResources = SharedClientResources.create(scenario.applications().client().ioThreads(),
                    scenario.applications().client().listenerThreads());
            openPods(applications, sharedResources);
            for (Application application : applications) {
                if (application.joinsLater()) {
                    // it doesn't take part in the warmup, which the gateways would otherwise wait for
                    application.skipWarmup(coordinationDirectory(), runId);
                }
            }
            phase.set("receiving");
            // The clients open now; the late applications' pods open when they join
            System.out.println("READY applications=" + applications.size() + " clients="
                    + applications.stream().filter(application -> !application.joinsLater()).count()
                    * scenario.podsPerApplication());
            long deadlineNanos = System.nanoTime() + Duration.ofSeconds(scenario.timeoutSeconds()).toNanos();
            for (Application application : applications) {
                if (application.joinsLater()) {
                    application.scheduleJoin(sharedResources, coordinationDirectory(), runId, deadlineNanos);
                } else {
                    application.startRestarts(sharedResources);
                }
            }
            boolean succeeded = receive(scenario, applications);
            phase.set("finished");
            return succeeded ? 0 : 1;
        } finally {
            for (Application application : applications) {
                application.close();
            }
            if (sharedResources != null) {
                sharedResources.close();
            }
            if (!"finished".equals(phase.get())) {
                phase.set("failed");
            }
            if (progress != null) {
                progress.finish();
            }
            if (control != null) {
                control.close();
            }
        }
    }

    /**
     * Receives until every application has received every message, or until the workload's timeout, and finishes each
     * application when it has. Returns whether every application received every message, valid and in order.
     */
    private boolean receive(IotScenario scenario, List<Application> applications) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(scenario.timeoutSeconds()).toNanos();
        boolean succeeded = true;
        List<Application> receiving = new ArrayList<>(applications);
        while (!receiving.isEmpty() && System.nanoTime() < deadline) {
            for (Iterator<Application> iterator = receiving.iterator(); iterator.hasNext(); ) {
                Application application = iterator.next();
                application.checkRestarts();
                application.checkJoin();
                application.markWarmupRounds(coordinationDirectory(), runId);
                if (application.receivedEveryMessage()) {
                    succeeded &= application.finish();
                    iterator.remove();
                }
            }
            if (!receiving.isEmpty()) {
                Thread.sleep(100);
            }
        }
        // The applications that timed out write what they received too
        for (Application application : receiving) {
            application.checkRestarts();
            application.checkJoin();
            succeeded &= application.finish();
        }
        return succeeded;
    }

    /**
     * Opens every application's pods, up to {@link #MAX_PARALLEL_STARTS} applications at a time, each application's
     * pods one after another. Every {@link #STARTUP_PROGRESS_INTERVAL_SECONDS} s, it prints how many are open as a
     * {@link #PROGRESS_PREFIX} line, which the launcher shows while it waits for the applications to start.
     */
    private static void openPods(List<Application> applications, PulsarClientSharedResources sharedResources)
            throws Exception {
        long pods = applications.stream().filter(application -> !application.joinsLater()).count()
                * applications.get(0).scenario.podsPerApplication();
        AtomicLong opened = new AtomicLong();
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(applications.size(), MAX_PARALLEL_STARTS),
                runnable -> {
                    Thread thread = new Thread(runnable, "iot-application-start");
                    thread.setDaemon(true);
                    return thread;
                });
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "iot-application-start-progress");
            thread.setDaemon(true);
            return thread;
        });
        reporter.scheduleAtFixedRate(() -> System.out.println(String.format(Locale.ROOT,
                        PROGRESS_PREFIX + "The applications have opened %,d of %,d pods", opened.get(), pods)),
                STARTUP_PROGRESS_INTERVAL_SECONDS, STARTUP_PROGRESS_INTERVAL_SECONDS, TimeUnit.SECONDS);
        try {
            List<Future<?>> starts = new ArrayList<>(applications.size());
            for (Application application : applications) {
                starts.add(executor.submit(() -> {
                    if (application.joinsLater()) {
                        application.createSubscription(sharedResources);
                    } else {
                        application.openPods(sharedResources, opened);
                    }
                    return null;
                }));
            }
            for (Future<?> start : starts) {
                try {
                    start.get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof Exception cause) {
                        throw cause;
                    }
                    throw e;
                }
            }
        } finally {
            reporter.shutdownNow();
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    // The container's progress: the applications' counts summed, as the launcher sums the counts of its sources
    private static void status(ObjectNode line, IotScenario scenario, List<Application> applications, String phase) {
        long received = 0;
        long duplicates = 0;
        long orderingViolations = 0;
        long invalidMessages = 0;
        int finished = 0;
        for (Application application : applications) {
            DeviceSequenceTracker.Summary summary = application.summarySoFar();
            received += summary.uniqueMessages();
            duplicates += summary.duplicates();
            orderingViolations += summary.orderingViolations();
            invalidMessages += summary.invalidMessages();
            if (application.finished) {
                finished++;
            }
        }
        line.put("role", "consumer");
        line.put("phase", phase);
        line.put("applications", applications.size());
        line.put("finishedApplications", finished);
        line.put("received", received);
        line.put("duplicates", duplicates);
        line.put("orderingViolations", orderingViolations);
        line.put("invalidMessages", invalidMessages);
        // without the messages that the applications skip, which they don't receive
        line.put("messageCount", applications.stream()
                .mapToLong(Application::expectedMessagesSoFar).sum());
    }

    /** An application: its subscription, the pods that consume it, and the checks of what it received. */
    private static final class Application {
        private final IotScenario scenario;
        private final int index;
        private final Path output;
        private final DeviceSequenceTracker tracker;
        // The messages that the application receives; -1 until the launcher tells them, for an application that
        // resets its subscription
        private volatile long expectedMessages;
        private final HdrLatencyRecorder receiveLatency;
        private final AtomicLong firstMeasurementReceiptEpochMs = new AtomicLong();
        private final AtomicLong lastMeasurementReceiptEpochMs = new AtomicLong();
        // For an application that joins later: when it joined and when it caught up
        private final CatchUpTracker catchUp;
        private final AtomicReference<Throwable> joinFailure = new AtomicReference<>();
        private final AtomicInteger listenersInFlight = new AtomicInteger();
        private volatile Thread joiner;
        // Guarded by itself, as the restarts replace pods
        private final List<ClientAndConsumer> pods;
        private final AtomicBoolean stopping = new AtomicBoolean();
        private final AtomicReference<Throwable> restartFailure = new AtomicReference<>();
        private volatile Thread restarter;
        private volatile Thread pauser;
        // Read by the restarter
        private volatile int nextWarmupRound = 1;
        private volatile boolean finished;
        // For pods that read compacted: what the messages did to the pods' device states, over every pod, each
        // restarted pod's start-up time, until it receives the first message published after its start, and each
        // device's next sequence at the gateways once they have finished
        private final Path coordinationDirectory;
        private final String runId;
        private final AtomicLong stateUpdates = new AtomicLong();
        private final AtomicLong stateRepeats = new AtomicLong();
        private final AtomicLong stateRegressions = new AtomicLong();
        private final AtomicLong stateInvalid = new AtomicLong();
        private final List<Long> podStartUpMillis = Collections.synchronizedList(new ArrayList<>());
        private volatile long[] finalSequences;
        // With mode latestState: each device's latest sequence that the application's pods received, and when the
        // last message arrived. A device's last message may have expired or been evicted, so the application has
        // received everything when it has each device's last message, or when no message has arrived for a while
        // once the gateways have finished
        private final LatestValueTracker applicationState;
        private volatile long lastReceiptNanos = System.nanoTime();
        private static final long QUIET_NANOS = TimeUnit.SECONDS.toNanos(10);
        // Whether the application completed, with each device's last state or by the quiet time, rather than timed out
        private volatile boolean completed;
        // Set once the gateways have finished: the restarter finishes the restart that it's doing, if any, and
        // doesn't start another, since interrupting a restart could leave a closed pod
        private volatile boolean restartsFinished;
        // Checking every device of every pod takes time under the pods' lock, so the receive loop checks once a second
        private long nextStaleCheckNanos;
        // With processing failures: the failed attempts, and the messages that reached the dead letter topic, which
        // the application also consumes, of which those that weren't to fail at every attempt are unexpected
        private final AtomicLong failedAttempts = new AtomicLong();
        private final AtomicLong deadLetters = new AtomicLong();
        // The messages that reached the dead letter topic, by device and sequence: the client can dead-letter a
        // message again when a reconnection loses the acknowledgment of its first dead-lettering, which the tracker
        // counts as a duplicate
        private final Set<Long> deadLettered = ConcurrentHashMap.newKeySet();
        private final AtomicLong unexpectedDeadLetters = new AtomicLong();
        // The retries that the retry topic couldn't take, which the client redelivers instead
        private final AtomicLong retryPublishFailures = new AtomicLong();
        // With scheduled delivery: the messages that arrived earlier than their delivery time allows, and the most
        // that any arrived before it
        private final AtomicLong earlyDeliveries = new AtomicLong();
        private final AtomicLong maxEarlyMillis = new AtomicLong();
        // Guarded by pods
        private ClientAndConsumer deadLetterPod;

        Application(IotScenario scenario, int index, Path output, Path coordinationDirectory, String runId)
                throws IOException {
            this.scenario = scenario;
            this.index = index;
            this.output = output;
            this.coordinationDirectory = coordinationDirectory;
            this.runId = runId;
            applicationState = IotScenario.Applications.LATEST_STATE_MODE.equals(scenario.applications().mode())
                    ? new LatestValueTracker(scenario.deviceCount()) : null;
            tracker = new DeviceSequenceTracker(scenario.deviceCount(), scenario.startsAtFirstSequence(index),
                    scenario.keepsDeviceOrder());
            // Unknown until the launcher has reset the subscription, which it tells with the prepared join
            expectedMessages = scenario.resetToMillis(index) >= 0 ? -1 : scenario.expectedMessageCount(index);
            catchUp = new CatchUpTracker(scenario.topicCount(), scenario.applications().caughtUpLatencyMillis());
            pods = new ArrayList<>(scenario.podsPerApplication());
            receiveLatency = new HdrLatencyRecorder(output.resolve("application-latency.hdr"),
                    PerformanceTool.MAX_LATENCY_MICROS);
        }

        /** The messages that the application expects, or all of them while it doesn't know yet. */
        long expectedMessagesSoFar() {
            long expected = expectedMessages;
            return expected >= 0 ? expected : scenario.messageCount();
        }

        HdrLatencyRecorder receiveLatency() {
            return receiveLatency;
        }

        boolean joinsLater() {
            return scenario.joinSeconds(index) > 0;
        }

        /**
         * Creates the application's subscription on every topic at the earliest position, without keeping a
         * consumer, so that it keeps the backlog until the application joins.
         */
        void createSubscription(PulsarClientSharedResources sharedResources) throws Exception {
            try (PulsarClient client = SharedClientResources.clientBuilder(scenario, sharedResources).build()) {
                client.newConsumer(Schema.BYTES)
                        .topics(scenario.topicNames())
                        .subscriptionName(scenario.subscriptionName(index))
                        .subscriptionType(scenario.subscriptionType())
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()
                        .close();
            }
        }

        /** Marks every warmup round received, so that the gateways don't wait for an application that joins later. */
        void skipWarmup(Path coordinationDirectory, String runId) throws IOException {
            for (; nextWarmupRound <= scenario.warmupRounds() && scenario.warmupMessageCountPerRound() > 0;
                 nextWarmupRound++) {
                WarmupBarrier.markApplicationComplete(coordinationDirectory, runId, nextWarmupRound, index);
            }
        }

        /** Opens the application's pods at its join time, counted from the gateways' measurement start. */
        void scheduleJoin(PulsarClientSharedResources sharedResources, Path coordinationDirectory, String runId,
                          long deadlineNanos) {
            joiner = new Thread(() -> {
                try {
                    long measurementStart = MeasurementStartMarker.await(coordinationDirectory, runId, deadlineNanos);
                    long waitMillis = measurementStart + TimeUnit.SECONDS.toMillis(scenario.joinSeconds(index))
                            - System.currentTimeMillis();
                    if (waitMillis > 0) {
                        Thread.sleep(waitMillis);
                    }
                    if (scenario.preparesJoin()) {
                        // the launcher skips the late applications' messages and unloads the topics first
                        Map<Integer, Long> expected = JoinPreparedMarker.await(coordinationDirectory, runId,
                                deadlineNanos);
                        if (scenario.resetToMillis(index) >= 0) {
                            Long messages = expected.get(index);
                            if (messages == null) {
                                throw new IllegalStateException("The launcher didn't tell the messages that"
                                        + " application " + index + " expects after resetting its subscription");
                            }
                            expectedMessages = messages;
                        }
                    }
                    // when it starts to open its pods, so the catch-up includes connecting them
                    long joinEpochMs = System.currentTimeMillis();
                    catchUp.joined(joinEpochMs);
                    System.out.println("JOIN application=" + index + " epochMs=" + joinEpochMs);
                    openPods(sharedResources, new AtomicLong());
                    startRestarts(sharedResources);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } catch (TimeoutException notStarted) {
                    // the receive loop's timeout reports the run as failed and writes the applications' summaries
                    System.out.println("JOIN application=" + index + " didn't join: " + notStarted.getMessage());
                } catch (Throwable error) {
                    joinFailure.compareAndSet(null, error);
                }
            }, "iot-application-join-" + index);
            joiner.setDaemon(true);
            joiner.start();
        }

        void checkJoin() {
            if (joinFailure.get() != null) {
                throw new IllegalStateException("Application " + index + " couldn't join", joinFailure.get());
            }
        }

        void openPods(PulsarClientSharedResources sharedResources, AtomicLong opened) throws Exception {
            if (scenario.behaviors().processingFailures().enabled()) {
                ClientAndConsumer created = createDeadLetterPod(sharedResources);
                synchronized (pods) {
                    if (stopping.get()) {
                        created.closeAsync().exceptionally(failure -> null).join();
                        return;
                    }
                    deadLetterPod = created;
                }
            }
            for (int pod = 0; pod < scenario.podsPerApplication(); pod++) {
                ClientAndConsumer created = createPod(sharedResources, pod, false);
                synchronized (pods) {
                    // a late application's joiner can open a pod after the application stopped
                    if (stopping.get()) {
                        // uninterruptibly, so that the client is closed before the shared resources are
                        created.closeAsync().exceptionally(failure -> null).join();
                        return;
                    }
                    pods.add(created);
                }
                opened.incrementAndGet();
            }
        }

        void startRestarts(PulsarClientSharedResources sharedResources) {
            if (scenario.behaviors().podRestarts().enabled() && !stopping.get()) {
                Thread thread = new Thread(() -> restartPods(sharedResources), "iot-client-restarter-" + index);
                restarter = thread;
                thread.start();
            }
            if (scenario.behaviors().podPauses().enabled() && !stopping.get()) {
                Thread thread = new Thread(this::pausePods, "iot-client-pauser-" + index);
                pauser = thread;
                thread.start();
            }
        }

        void checkRestarts() {
            if (restartFailure.get() != null) {
                throw new IllegalStateException("Cannot restart IoT client", restartFailure.get());
            }
        }

        /** Tells the gateways about the next warmup round, once the application has received it. */
        void markWarmupRounds(Path coordinationDirectory, String runId) throws IOException {
            if (nextWarmupRound <= scenario.warmupRounds() && scenario.warmupMessageCountPerRound() > 0
                    && warmupMessagesReceived() >= scenario.warmupMessageCountPerRound() * nextWarmupRound) {
                WarmupBarrier.markApplicationComplete(coordinationDirectory, runId, nextWarmupRound, index);
                nextWarmupRound++;
            }
        }

        // A pod that reads compacted receives every message published after its start, so the warmup's are those of
        // the pod that has received the most
        private long warmupMessagesReceived() {
            if (applicationState != null) {
                return applicationState.receivedMessages();
            }
            if (!scenario.applications().readsCompacted()) {
                return tracker.uniqueMessages();
            }
            synchronized (pods) {
                return pods.stream().mapToLong(pod -> pod.state().receivedMessages()).max().orElse(0);
            }
        }

        boolean receivedEveryMessage() throws IOException {
            if (scenario.applications().keepsLatestState()) {
                long now = System.nanoTime();
                if (now - nextStaleCheckNanos < 0) {
                    return false;
                }
                nextStaleCheckNanos = now + TimeUnit.SECONDS.toNanos(1);
                completed = staleDevices() == 0
                        || applicationState != null && finalSequences != null && now - lastReceiptNanos > QUIET_NANOS;
                return completed;
            }
            if (expectedMessages < 0 || tracker.uniqueMessages() < expectedMessages) {
                return false;
            }
            // The dead letters are checked against the poison messages of each device's sequences, which the
            // gateways hand over after their last send has completed
            if (scenario.behaviors().processingFailures().enabled() && finalSequences == null) {
                finalSequences = FinalSequencesMarker.read(coordinationDirectory, runId).orElse(null);
                return finalSequences != null;
            }
            return true;
        }

        /**
         * For pods that read compacted: the most devices whose latest state a pod doesn't have, the last message of
         * the device, or every device while the gateways haven't finished or the pods aren't all open.
         */
        private long staleDevices() throws IOException {
            long[] next = finalSequences;
            if (next == null) {
                next = FinalSequencesMarker.read(coordinationDirectory, runId).orElse(null);
                if (next == null) {
                    return scenario.deviceCount();
                }
                finalSequences = next;
                // The gateways have finished: a restart now would only keep a pod rebuilding its state, so the pods
                // wouldn't all have every device's last state at the same time when the restarts take as long as
                // their interval
                restartsFinished = true;
                if (applicationState != null) {
                    // the paused pods resume, and the quiet time that completes the application starts now
                    try {
                        interruptAndAwait(pauser);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    lastReceiptNanos = System.nanoTime();
                }
            }
            if (applicationState != null) {
                return applicationState.staleDevices(next);
            }
            synchronized (pods) {
                if (pods.size() < scenario.podsPerApplication()) {
                    return scenario.deviceCount();
                }
                long stale = 0;
                for (ClientAndConsumer pod : pods) {
                    stale = Math.max(stale, pod.state().staleDevices(next));
                }
                return stale;
            }
        }

        /** The counts so far, for the progress. */
        DeviceSequenceTracker.Summary summarySoFar() {
            if (!scenario.applications().keepsLatestState()) {
                return tracker.summary();
            }
            return new DeviceSequenceTracker.Summary(stateUpdates.get(), stateRepeats.get(), stateRegressions.get(),
                    stateInvalid.get(), 0, 0);
        }

        /**
         * Stops the application and writes its outputs. Returns whether it received every message, valid and in
         * order.
         */
        boolean finish() throws Exception {
            stopRestarts();
            awaitListeners();
            if (scenario.applications().keepsLatestState()) {
                return finishReadingCompacted();
            }
            DeviceSequenceTracker.Summary summary = tracker.summary();
            long expectedDeadLetters = expectedDeadLetters();
            receiveLatency.close();
            tracker.writeState(output.resolve("application-state.bin"));
            tracker.writeViolationSamples(output.resolve("ordering-violations.txt"));
            tracker.writeRepublishedSamples(output.resolve("republished-duplicates.txt"));
            Files.writeString(output.resolve("application-summary.json"), "{\n"
                    + "  \"applicationIndex\": " + index + ",\n"
                    + "  \"uniqueMessages\": " + summary.uniqueMessages() + ",\n"
                    + "  \"skippedMessages\": " + (scenario.messageCount() - expectedMessagesSoFar())
                    + ",\n"
                    + "  \"duplicates\": " + summary.duplicates() + ",\n"
                    + "  \"redeliveredDuplicates\": " + summary.redeliveredDuplicates() + ",\n"
                    + "  \"republishedDuplicates\": " + summary.republishedDuplicates() + ",\n"
                    + "  \"orderingViolations\": " + summary.orderingViolations() + ",\n"
                    + "  \"invalidMessages\": " + summary.invalidMessages() + ",\n"
                    + "  \"firstMeasurementMessageReceivedEpochMs\": "
                    + firstMeasurementReceiptEpochMs.get() + ",\n"
                    + "  \"lastMeasurementMessageReceivedEpochMs\": "
                    + lastMeasurementReceiptEpochMs.get() + ",\n"
                    + "  \"joinSeconds\": " + scenario.joinSeconds(index) + ",\n"
                    + "  \"joinEpochMs\": " + catchUp.joinEpochMs() + ",\n"
                    + "  \"caughtUpLatencyMillis\": " + catchUp.thresholdMillis() + ",\n"
                    + "  \"caughtUpEpochMs\": " + catchUp.caughtUpEpochMs() + ",\n"
                    + "  \"messagesWhenCaughtUp\": " + catchUp.messagesWhenCaughtUp() + ",\n"
                    + "  \"failedAttempts\": " + failedAttempts.get() + ",\n"
                    + "  \"deadLetters\": " + deadLetters.get() + ",\n"
                    + "  \"expectedDeadLetters\": " + expectedDeadLetters + ",\n"
                    + "  \"unexpectedDeadLetters\": " + unexpectedDeadLetters.get() + ",\n"
                    + "  \"retryPublishFailures\": " + retryPublishFailures.get() + ",\n"
                    + "  \"earlyDeliveries\": " + earlyDeliveries.get() + ",\n"
                    + "  \"maxEarlyMillis\": " + maxEarlyMillis.get() + "\n}\n");
            closePods();
            finished = true;
            // with skipped messages, the count tells a lost message from a skipped one
            return summary.valid() && expectedMessages >= 0 && summary.uniqueMessages() == expectedMessages
                    && deadLetters.get() == expectedDeadLetters && unexpectedDeadLetters.get() == 0
                    && earlyDeliveries.get() == 0;
        }

        /**
         * The messages that fail at every attempt, which end in the dead letter topic: 0 without processing failures,
         * and -1 while the gateways haven't told each device's last message.
         */
        private long expectedDeadLetters() throws IOException {
            IotScenario.ProcessingFailures failures = scenario.behaviors().processingFailures();
            if (!failures.enabled()) {
                return 0;
            }
            long[] next = finalSequences;
            if (next == null) {
                next = FinalSequencesMarker.read(coordinationDirectory, runId).orElse(null);
                if (next == null) {
                    return -1;
                }
            }
            long poison = 0;
            for (int device = 0; device < next.length; device++) {
                for (long sequence = 0; sequence < next[device]; sequence++) {
                    if (failures.poison(device, sequence)) {
                        poison++;
                    }
                }
            }
            return poison;
        }

        /**
         * The most devices whose latest sequence, at a pod or at the application, passes the gateways' last one:
         * they received a message that the gateways didn't send.
         */
        private long aheadDevices() {
            long[] next = finalSequences;
            if (next == null) {
                return 0;
            }
            if (applicationState != null) {
                return applicationState.aheadDevices(next);
            }
            synchronized (pods) {
                return pods.stream().mapToLong(pod -> pod.state().aheadDevices(next)).max().orElse(0);
            }
        }

        /**
         * Finishes an application whose pods read compacted: a repeated or older sequence of a device, an invalid
         * message, or a pod without a device's latest state fails it.
         */
        private boolean finishReadingCompacted() throws Exception {
            long stale = staleDevices();
            // A pod that a restart replaced after the receive loop's last check rebuilds its state now that the
            // restarts have stopped, within a pod's start-up time
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
            while (stale > 0 && applicationState == null && finalSequences != null
                    && System.nanoTime() - deadline < 0) {
                Thread.sleep(1000);
                stale = staleDevices();
            }
            long ahead = aheadDevices();
            receiveLatency.close();
            // the state of the pod that lacks the most devices' last messages, or the application's, which the
            // launcher checks
            if (applicationState != null) {
                applicationState.writeState(output.resolve("application-state.bin"));
            } else {
                synchronized (pods) {
                    long[] next = finalSequences;
                    pods.stream()
                            .max(Comparator.comparingLong(pod -> next != null ? pod.state().staleDevices(next) : 0))
                            .ifPresent(pod -> {
                                try {
                                    pod.state().writeState(output.resolve("application-state.bin"));
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
                }
            }
            List<Long> startUps;
            synchronized (podStartUpMillis) {
                startUps = new ArrayList<>(podStartUpMillis);
            }
            Files.writeString(output.resolve("application-summary.json"), "{\n"
                    + "  \"applicationIndex\": " + index + ",\n"
                    + "  \"mode\": \"" + scenario.applications().mode() + "\",\n"
                    + "  \"uniqueMessages\": " + stateUpdates.get() + ",\n"
                    + "  \"skippedMessages\": 0,\n"
                    + "  \"duplicates\": " + stateRepeats.get() + ",\n"
                    + "  \"redeliveredDuplicates\": 0,\n"
                    + "  \"republishedDuplicates\": 0,\n"
                    + "  \"orderingViolations\": " + stateRegressions.get() + ",\n"
                    + "  \"invalidMessages\": " + (stateInvalid.get() + ahead + (applicationState != null ? 0 : stale))
                    + ",\n"
                    + "  \"staleDevices\": " + stale + ",\n"
                    + "  \"podStartUpMillis\": " + startUps + ",\n"
                    + "  \"firstMeasurementMessageReceivedEpochMs\": "
                    + firstMeasurementReceiptEpochMs.get() + ",\n"
                    + "  \"lastMeasurementMessageReceivedEpochMs\": "
                    + lastMeasurementReceiptEpochMs.get() + ",\n"
                    + "  \"joinSeconds\": 0,\n"
                    + "  \"joinEpochMs\": 0,\n"
                    + "  \"caughtUpLatencyMillis\": " + catchUp.thresholdMillis() + ",\n"
                    + "  \"caughtUpEpochMs\": 0,\n"
                    + "  \"messagesWhenCaughtUp\": 0\n}\n");
            closePods();
            finished = true;
            // A repeated state, as after a reader reconnects, is reported as a duplicate, but a state service takes it
            // with latestState, a device without its last state lost it to the message TTL or the backlog quota
            return (stale == 0 || applicationState != null && completed) && finalSequences != null && ahead == 0
                    && stateRegressions.get() == 0 && stateInvalid.get() == 0;
        }

        /** Closes the application's pods, when it hasn't finished, such as after a failure. */
        void close() throws Exception {
            stopping.set(true);
            // so that they don't open pods after they're closed, or use the shared resources after they are
            interruptAndAwait(joiner);
            interruptAndAwait(restarter);
            interruptAndAwait(pauser);
            closePods();
        }

        private void stopRestarts() throws InterruptedException {
            stopping.set(true);
            interruptAndAwait(joiner);
            interruptAndAwait(restarter);
            interruptAndAwait(pauser);
        }

        /**
         * Waits briefly for the listeners that are handling a message, such as the one that received the last message
         * and may record the catch-up with it, so that the summary includes what they record.
         */
        private void awaitListeners() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (listenersInFlight.get() > 0 && deadline - System.nanoTime() > 0) {
                Thread.sleep(1);
            }
        }

        /** Interrupts the thread and waits for it, which takes up to a client's close timeout when it opens a pod. */
        private static void interruptAndAwait(Thread thread) throws InterruptedException {
            if (thread == null) {
                return;
            }
            thread.interrupt();
            thread.join(TimeUnit.SECONDS.toMillis(90));
            if (thread.isAlive()) {
                System.err.println("WARN " + thread.getName() + " didn't stop within 90 s");
            }
        }

        // All at the same time: one after another, an application's 100 pods take more than a second to close
        private void closePods() throws Exception {
            synchronized (pods) {
                try {
                    List<ClientAndConsumer> all = new ArrayList<>(pods);
                    if (deadLetterPod != null) {
                        all.add(deadLetterPod);
                    }
                    CompletableFuture.allOf(all.stream().map(ClientAndConsumer::closeAsync)
                            .toArray(CompletableFuture[]::new)).get();
                } catch (ExecutionException e) {
                    throw e.getCause() instanceof Exception cause ? cause : e;
                } finally {
                    pods.clear();
                    deadLetterPod = null;
                }
            }
        }

        private ClientAndConsumer createPod(PulsarClientSharedResources sharedResources, int podIndex,
                                            boolean restart) throws Exception {
            if (scenario.applications().readsCompacted()) {
                return createReaderPod(sharedResources, restart);
            }
            PulsarClient client = SharedClientResources.clientBuilder(scenario, sharedResources).build();
            try {
                ConsumerBuilder<byte[]> builder = client.newConsumer(Schema.BYTES);
                if (scenario.topics().subscribedByPattern()) {
                    builder.topicsPattern(scenario.topics().pattern())
                            .patternAutoDiscoveryPeriod(scenario.topics().patternDiscoverySeconds(), TimeUnit.SECONDS);
                } else {
                    builder.topics(scenario.topicNames());
                }
                builder.subscriptionName(scenario.subscriptionName(index))
                        .consumerName("iot-application-" + index + "-pod-" + podIndex)
                        .subscriptionType(scenario.subscriptionType())
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .receiverQueueSize(receiverQueueSize())
                        .messageListener((currentConsumer, message) -> {
                            long receivedEpochMs = System.currentTimeMillis();
                            listenersInFlight.incrementAndGet();
                            try {
                                TelemetryMessage.Decoded decoded = decode(message);
                                // a failed attempt isn't a receipt: the message's latency is that of the attempt
                                // that succeeds, since its last publication, which is to the retry topic for a
                                // message that the retry topic retries, or since its delivery time when scheduled,
                                // which the retry topic keeps
                                if (failedAttempt(currentConsumer, message, decoded)) {
                                    return;
                                }
                                if (applicationState != null) {
                                    receivedLatestState(currentConsumer, message, decoded, receivedEpochMs);
                                    return;
                                }
                                if (decoded.measurement()) {
                                    firstMeasurementReceiptEpochMs.accumulateAndGet(receivedEpochMs,
                                            (current, received) -> current == 0 ? received
                                                    : Math.min(current, received));
                                    lastMeasurementReceiptEpochMs.accumulateAndGet(receivedEpochMs, Math::max);
                                }
                                receiveLatency.recordMillis(receivedEpochMs - deliveryDue(message, receivedEpochMs),
                                        decoded.measurement());
                                tracker.received(decoded.deviceId(), decoded.sequence(), message.getMessageId(),
                                        decoded.sentNanos(), message.getTopicName(),
                                        Thread.currentThread().getName(), message.getRedeliveryCount(),
                                        receivedEpochMs);
                                if (decoded.measurement()) {
                                    // after counting the message, which then counts for the catch-up
                                    catchUp.received(message.getTopicName(), message.getPublishTime(),
                                            receivedEpochMs, tracker::uniqueMessages);
                                }
                                currentConsumer.acknowledgeAsync(message);
                            } catch (RuntimeException error) {
                                tracker.invalidMessage();
                                if (applicationState != null) {
                                    stateInvalid.incrementAndGet();
                                }
                                currentConsumer.negativeAcknowledge(message);
                            } finally {
                                listenersInFlight.decrementAndGet();
                            }
                        });
                IotScenario.ProcessingFailures failures = scenario.behaviors().processingFailures();
                if (failures.enabled()) {
                    DeadLetterPolicy.DeadLetterPolicyBuilder deadLetterPolicy = DeadLetterPolicy.builder()
                            .maxRedeliverCount(failures.maxRedeliveries())
                            .deadLetterTopic(scenario.deadLetterTopicName(index));
                    if (failures.retriesThroughTopic()) {
                        deadLetterPolicy.retryLetterTopic(scenario.retryTopicName(index));
                    }
                    builder.negativeAckRedeliveryDelay(failures.retryDelayMillis(), TimeUnit.MILLISECONDS)
                            .enableRetry(failures.retriesThroughTopic())
                            .deadLetterPolicy(deadLetterPolicy.build());
                }
                Consumer<byte[]> consumer = builder.subscribe();
                return new ClientAndConsumer(client, consumer, List.of(), null);
            } catch (Throwable error) {
                // uninterruptibly: an interrupted subscription would otherwise leave the client closing
                client.closeAsync().exceptionally(failure -> null).join();
                throw error;
            }
        }

        // The client's default unless the scenario sets it
        private int receiverQueueSize() {
            Integer size = scenario.applications().client().receiverQueueSize();
            return size != null ? size : 1000;
        }

        /**
         * When the message was due: its delivery time, for a message whose delivery the gateway scheduled, which
         * counts as early when it arrives before the time by more than the tolerance; otherwise its publishing.
         */
        private long deliveryDue(Message<byte[]> message, long receivedEpochMs) {
            String deliverAt = message.getProperty(IotScenario.ScheduledDelivery.DELIVER_AT_PROPERTY);
            if (deliverAt == null) {
                return message.getPublishTime();
            }
            long due = Long.parseLong(deliverAt);
            long early = due - receivedEpochMs;
            if (early > 0) {
                maxEarlyMillis.accumulateAndGet(early, Math::max);
                if (early > scenario.behaviors().scheduledDelivery().earlyToleranceMillis()) {
                    earlyDeliveries.incrementAndGet();
                }
            }
            return Math.min(due, receivedEpochMs);
        }

        /**
         * With mode latestState: updates the device's latest state with the message, which may follow a gap of
         * expired or evicted messages, and acknowledges it.
         */
        private void receivedLatestState(Consumer<byte[]> consumer, Message<byte[]> message,
                                         TelemetryMessage.Decoded decoded, long receivedEpochMs) {
            lastReceiptNanos = System.nanoTime();
            switch (applicationState.received(decoded.deviceId(), decoded.sequence())) {
                case UPDATE -> stateUpdates.incrementAndGet();
                case REPEAT -> stateRepeats.incrementAndGet();
                case REGRESSION -> stateRegressions.incrementAndGet();
                case INVALID -> stateInvalid.incrementAndGet();
            }
            if (decoded.measurement()) {
                firstMeasurementReceiptEpochMs.accumulateAndGet(receivedEpochMs,
                        (current, received) -> current == 0 ? received : Math.min(current, received));
                lastMeasurementReceiptEpochMs.accumulateAndGet(receivedEpochMs, Math::max);
            }
            receiveLatency.recordMillis(receivedEpochMs - message.getPublishTime(), decoded.measurement());
            consumer.acknowledgeAsync(message);
        }

        /**
         * Fails the attempt to process the message when the processing failures fail it, and retries it then.
         * Returns whether it failed.
         */
        private boolean failedAttempt(Consumer<byte[]> consumer, Message<byte[]> message,
                                      TelemetryMessage.Decoded decoded) {
            IotScenario.ProcessingFailures failures = scenario.behaviors().processingFailures();
            if (!failures.enabled()) {
                return false;
            }
            int attempt = failures.retriesThroughTopic() ? reconsumeTimes(message) : message.getRedeliveryCount();
            if (!failures.fails(decoded.deviceId(), decoded.sequence(), attempt)) {
                return false;
            }
            failedAttempts.incrementAndGet();
            if (failures.retriesThroughTopic()) {
                consumer.reconsumeLaterAsync(message, failures.retryDelayMillis(), TimeUnit.MILLISECONDS)
                        .exceptionally(failure -> {
                            retryPublishFailures.incrementAndGet();
                            return null;
                        });
            } else {
                consumer.negativeAcknowledge(message);
            }
            return true;
        }

        // The times that the retry topic has retried the message, 0 for a message from its topic
        private static int reconsumeTimes(Message<byte[]> message) {
            String times = message.getProperty(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES);
            return times != null ? Integer.parseInt(times) : 0;
        }

        /**
         * The application's consumer of its dead letter topic, which receives the messages that failed every
         * attempt: they count as received, once, and those that weren't to fail every attempt are unexpected.
         */
        private ClientAndConsumer createDeadLetterPod(PulsarClientSharedResources sharedResources) throws Exception {
            PulsarClient client = SharedClientResources.clientBuilder(scenario, sharedResources).build();
            IotScenario.ProcessingFailures failures = scenario.behaviors().processingFailures();
            try {
                Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                        .topic(scenario.deadLetterTopicName(index))
                        .subscriptionName(scenario.subscriptionName(index))
                        .consumerName("iot-application-" + index + "-dead-letters")
                        .subscriptionType(SubscriptionType.Shared)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .messageListener((currentConsumer, message) -> {
                            listenersInFlight.incrementAndGet();
                            try {
                                TelemetryMessage.Decoded decoded = decode(message);
                                if (deadLettered.add((decoded.deviceId() << 40) | decoded.sequence())) {
                                    deadLetters.incrementAndGet();
                                    if (!failures.poison(decoded.deviceId(), decoded.sequence())) {
                                        unexpectedDeadLetters.incrementAndGet();
                                    }
                                }
                                tracker.received(decoded.deviceId(), decoded.sequence(), message.getMessageId(),
                                        decoded.sentNanos(), message.getTopicName(),
                                        Thread.currentThread().getName(), message.getRedeliveryCount(),
                                        System.currentTimeMillis());
                                currentConsumer.acknowledgeAsync(message);
                            } catch (RuntimeException error) {
                                tracker.invalidMessage();
                                currentConsumer.acknowledgeAsync(message);
                            } finally {
                                listenersInFlight.decrementAndGet();
                            }
                        })
                        .subscribe();
                return new ClientAndConsumer(client, consumer, List.of(), null);
            } catch (Throwable error) {
                client.closeAsync().exceptionally(failure -> null).join();
                throw error;
            }
        }

        /**
         * A pod that reads each topic compacted from the start, as a digital twin service rebuilds the devices'
         * latest state, with each device's latest sequence its state.
         */
        private ClientAndConsumer createReaderPod(PulsarClientSharedResources sharedResources, boolean restart)
                throws Exception {
            PulsarClient client = SharedClientResources.clientBuilder(scenario, sharedResources).build();
            LatestValueTracker state = new LatestValueTracker(scenario.deviceCount());
            long startEpochMs = System.currentTimeMillis();
            // A restarted pod has read the compacted state and the backlog when it receives a message published
            // after its start
            AtomicBoolean startedUp = new AtomicBoolean(!restart);
            List<Reader<byte[]>> readers = new ArrayList<>(scenario.topicCount());
            try {
                for (String topic : scenario.topicNames()) {
                    readers.add(client.newReader(Schema.BYTES)
                            .topic(topic)
                            .startMessageId(MessageId.earliest)
                            .readCompacted(true)
                            .readerListener((reader, message) ->
                                    receivedState(message, state, startEpochMs, startedUp))
                            .create());
                }
                return new ClientAndConsumer(client, null, readers, state);
            } catch (Throwable error) {
                // uninterruptibly: an interrupted creation would otherwise leave the client closing
                readers.forEach(reader -> reader.closeAsync().exceptionally(failure -> null).join());
                client.closeAsync().exceptionally(failure -> null).join();
                throw error;
            }
        }

        // A message whose payload isn't whole, such as a chunked message put together wrongly, or whose key isn't its
        // device's is invalid
        private TelemetryMessage.Decoded decode(Message<byte[]> message) {
            byte[] payload = message.getData();
            if (payload.length != scenario.payloadBytes()) {
                throw new IllegalArgumentException("Telemetry payload has " + payload.length + " bytes, not "
                        + scenario.payloadBytes());
            }
            TelemetryMessage.Decoded decoded = TelemetryMessage.decode(payload);
            if (!TelemetryMessage.hasKey(message, decoded.deviceId(), scenario.stringKeys())) {
                throw new IllegalArgumentException("Telemetry key does not match payload device ID");
            }
            return decoded;
        }

        private void receivedState(Message<byte[]> message, LatestValueTracker state, long podStartEpochMs,
                                   AtomicBoolean startedUp) {
            long receivedEpochMs = System.currentTimeMillis();
            listenersInFlight.incrementAndGet();
            try {
                TelemetryMessage.Decoded decoded = decode(message);
                switch (state.received(decoded.deviceId(), decoded.sequence())) {
                    case UPDATE -> stateUpdates.incrementAndGet();
                    case REPEAT -> stateRepeats.incrementAndGet();
                    case REGRESSION -> stateRegressions.incrementAndGet();
                    case INVALID -> stateInvalid.incrementAndGet();
                }
                // Only the messages published since the pod started have a delivery latency: the earlier ones are
                // the state that it rebuilds
                if (message.getPublishTime() >= podStartEpochMs) {
                    if (startedUp.compareAndSet(false, true)) {
                        podStartUpMillis.add(receivedEpochMs - podStartEpochMs);
                    }
                    if (decoded.measurement()) {
                        firstMeasurementReceiptEpochMs.accumulateAndGet(receivedEpochMs,
                                (current, received) -> current == 0 ? received : Math.min(current, received));
                        lastMeasurementReceiptEpochMs.accumulateAndGet(receivedEpochMs, Math::max);
                    }
                    receiveLatency.recordMillis(receivedEpochMs - message.getPublishTime(), decoded.measurement());
                }
            } catch (RuntimeException error) {
                stateInvalid.incrementAndGet();
            } finally {
                listenersInFlight.decrementAndGet();
            }
        }

        private void restartPods(PulsarClientSharedResources sharedResources) {
            int restartCount = Math.max(1,
                    (int) Math.ceil(scenario.podsPerApplication() * scenario.behaviors().podRestarts().fraction()));
            while (!stopping.get() && !restartsFinished) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(scenario.behaviors().podRestarts().intervalSeconds()));
                    // A pod that reads compacted counts the warmup's messages that it received since its start, which
                    // a restart in the warmup could keep every pod from reaching
                    if (scenario.applications().readsCompacted() && nextWarmupRound <= scenario.warmupRounds()
                            && scenario.warmupMessageCountPerRound() > 0) {
                        continue;
                    }
                    for (int i = 0; i < restartCount && !stopping.get() && !restartsFinished; i++) {
                        synchronized (pods) {
                            int podIndex = ThreadLocalRandom.current().nextInt(pods.size());
                            pods.get(podIndex).close();
                            pods.set(podIndex, createPod(sharedResources, podIndex, true));
                        }
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception error) {
                    // a restart that the application's stop interrupts isn't a failure
                    if (!stopping.get()) {
                        restartFailure.compareAndSet(null, error);
                    }
                    return;
                }
            }
        }

        // Pauses random pods' consumers for the pause's time every interval, and resumes them then, also when the
        // pauses stop. A pod that a restart replaces meanwhile is resumed, closed, harmlessly.
        private void pausePods() {
            IotScenario.PodPauses podPauses = scenario.behaviors().podPauses();
            int pauseCount = Math.max(1, (int) Math.ceil(scenario.podsPerApplication() * podPauses.fraction()));
            List<Consumer<byte[]>> paused = new ArrayList<>(pauseCount);
            try {
                while (!stopping.get()) {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(podPauses.intervalSeconds()) - podPauses.pauseMillis());
                    synchronized (pods) {
                        List<ClientAndConsumer> candidates = new ArrayList<>(pods);
                        Collections.shuffle(candidates);
                        for (ClientAndConsumer pod : candidates.subList(0, Math.min(pauseCount, candidates.size()))) {
                            pod.consumer().pause();
                            paused.add(pod.consumer());
                        }
                    }
                    Thread.sleep(podPauses.pauseMillis());
                    paused.forEach(Consumer::resume);
                    paused.clear();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException error) {
                restartFailure.compareAndSet(null, error);
            } finally {
                paused.forEach(Consumer::resume);
            }
        }
    }

    /**
     * A pod: its client, and either its consumer of the subscription, or its readers of the topics with the devices'
     * state that they keep.
     */
    private record ClientAndConsumer(PulsarClient client, Consumer<byte[]> consumer, List<Reader<byte[]>> readers,
                                     LatestValueTracker state) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            try {
                if (consumer != null) {
                    consumer.close();
                }
                for (Reader<byte[]> reader : readers) {
                    reader.close();
                }
            } finally {
                client.close();
            }
        }

        /**
         * Closes the consumer or the readers, then the client, also when closing them failed. A pod that a restart
         * closed and couldn't replace, as when the application stops during the restart, is already closed, which
         * isn't a failure.
         */
        CompletableFuture<Void> closeAsync() {
            CompletableFuture<Void> closed = consumer != null ? consumer.closeAsync()
                    : CompletableFuture.allOf(readers.stream().map(Reader::closeAsync)
                    .toArray(CompletableFuture[]::new));
            return closed.handle((ignored, consumerFailure) -> unlessAlreadyClosed(consumerFailure))
                    .thenCompose(consumerFailure -> client.closeAsync()
                            .exceptionallyCompose(failure -> unlessAlreadyClosed(failure) == null
                                    ? CompletableFuture.completedFuture(null) : CompletableFuture.failedFuture(failure))
                            .thenRun(() -> {
                                if (consumerFailure != null) {
                                    throw new CompletionException(consumerFailure);
                                }
                            }));
        }

        private static Throwable unlessAlreadyClosed(Throwable failure) {
            Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                    ? failure.getCause() : failure;
            return cause instanceof PulsarClientException.AlreadyClosedException ? null : failure;
        }
    }
}

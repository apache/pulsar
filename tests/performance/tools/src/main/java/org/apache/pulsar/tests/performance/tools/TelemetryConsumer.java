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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import picocli.CommandLine.Command;

/**
 * Runs every application of the scenario in this JVM, as the gateways run in one. An application is a Key_Shared
 * subscription of its own on every topic, consumed through its pods, each a Pulsar client with a consumer of the
 * subscription; the applications differ only in their subscription. Every pod's client shares one set of client
 * resources, the event loop and the thread pools, sized by {@code applications.client}.
 *
 * <p>Each application checks its own delivery and ordering, and writes its outputs into a directory named after its
 * subscription in {@code --output}, as the run report names it. The progress stream sums the applications' counts.
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
                applications.add(new Application(scenario, index, applicationOutput));
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
            phase.set("receiving");
            System.out.println("READY applications=" + applications.size() + " clients="
                    + (long) applications.size() * scenario.podsPerApplication());
            for (Application application : applications) {
                application.startRestarts(sharedResources);
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
        long pods = (long) applications.size() * applications.get(0).scenario.podsPerApplication();
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
                    application.openPods(sharedResources, opened);
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
            DeviceSequenceTracker.Summary summary = application.tracker.summary();
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
        line.put("messageCount", Math.multiplyExact(scenario.messageCount(), (long) applications.size()));
    }

    /** An application: its subscription, the pods that consume it, and the checks of what it received. */
    private static final class Application {
        private final IotScenario scenario;
        private final int index;
        private final Path output;
        private final DeviceSequenceTracker tracker;
        private final HdrLatencyRecorder receiveLatency;
        private final AtomicLong firstMeasurementReceiptEpochMs = new AtomicLong();
        private final AtomicLong lastMeasurementReceiptEpochMs = new AtomicLong();
        // Guarded by itself, as the restarts replace pods
        private final List<ClientAndConsumer> pods;
        private final AtomicBoolean stopping = new AtomicBoolean();
        private final AtomicReference<Throwable> restartFailure = new AtomicReference<>();
        private Thread restarter;
        private int nextWarmupRound = 1;
        private volatile boolean finished;

        Application(IotScenario scenario, int index, Path output) throws IOException {
            this.scenario = scenario;
            this.index = index;
            this.output = output;
            tracker = new DeviceSequenceTracker(scenario.deviceCount());
            pods = new ArrayList<>(scenario.podsPerApplication());
            receiveLatency = new HdrLatencyRecorder(output.resolve("application-latency.hdr"),
                    PerformanceTool.MAX_LATENCY_MICROS);
        }

        HdrLatencyRecorder receiveLatency() {
            return receiveLatency;
        }

        void openPods(PulsarClientSharedResources sharedResources, AtomicLong opened) throws Exception {
            for (int pod = 0; pod < scenario.podsPerApplication(); pod++) {
                ClientAndConsumer created = createPod(sharedResources, pod);
                synchronized (pods) {
                    pods.add(created);
                }
                opened.incrementAndGet();
            }
        }

        void startRestarts(PulsarClientSharedResources sharedResources) {
            if (scenario.behaviors().podRestarts().enabled()) {
                restarter = new Thread(() -> restartPods(sharedResources), "iot-client-restarter-" + index);
                restarter.start();
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
                    && tracker.uniqueMessages() >= scenario.warmupMessageCountPerRound() * nextWarmupRound) {
                WarmupBarrier.markApplicationComplete(coordinationDirectory, runId, nextWarmupRound, index);
                nextWarmupRound++;
            }
        }

        boolean receivedEveryMessage() {
            return tracker.uniqueMessages() >= scenario.messageCount();
        }

        /**
         * Stops the application and writes its outputs. Returns whether it received every message, valid and in
         * order.
         */
        boolean finish() throws Exception {
            stopRestarts();
            DeviceSequenceTracker.Summary summary = tracker.summary();
            receiveLatency.close();
            tracker.writeState(output.resolve("application-state.bin"));
            tracker.writeViolationSamples(output.resolve("ordering-violations.txt"));
            Files.writeString(output.resolve("application-summary.json"), "{\n"
                    + "  \"applicationIndex\": " + index + ",\n"
                    + "  \"uniqueMessages\": " + summary.uniqueMessages() + ",\n"
                    + "  \"duplicates\": " + summary.duplicates() + ",\n"
                    + "  \"orderingViolations\": " + summary.orderingViolations() + ",\n"
                    + "  \"invalidMessages\": " + summary.invalidMessages() + ",\n"
                    + "  \"firstMeasurementMessageReceivedEpochMs\": "
                    + firstMeasurementReceiptEpochMs.get() + ",\n"
                    + "  \"lastMeasurementMessageReceivedEpochMs\": "
                    + lastMeasurementReceiptEpochMs.get() + "\n}\n");
            closePods();
            finished = true;
            return summary.valid() && summary.uniqueMessages() == scenario.messageCount();
        }

        /** Closes the application's pods, when it hasn't finished, such as after a failure. */
        void close() throws Exception {
            stopping.set(true);
            if (restarter != null) {
                restarter.interrupt();
            }
            closePods();
        }

        private void stopRestarts() throws InterruptedException {
            stopping.set(true);
            if (restarter != null) {
                restarter.interrupt();
                restarter.join(TimeUnit.SECONDS.toMillis(10));
            }
        }

        // All at the same time: one after another, an application's 100 pods take more than a second to close
        private void closePods() throws Exception {
            synchronized (pods) {
                try {
                    CompletableFuture.allOf(pods.stream().map(ClientAndConsumer::closeAsync)
                            .toArray(CompletableFuture[]::new)).get();
                } catch (ExecutionException e) {
                    throw e.getCause() instanceof Exception cause ? cause : e;
                } finally {
                    pods.clear();
                }
            }
        }

        private ClientAndConsumer createPod(PulsarClientSharedResources sharedResources, int podIndex)
                throws Exception {
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(scenario.serviceUrl())
                    .sharedResources(sharedResources)
                    .build();
            try {
                Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                        .topics(scenario.topicNames())
                        .subscriptionName(scenario.subscriptionName(index))
                        .consumerName("iot-application-" + index + "-pod-" + podIndex)
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .messageListener((currentConsumer, message) -> {
                            long receivedEpochMs = System.currentTimeMillis();
                            try {
                                TelemetryMessage.Decoded decoded = TelemetryMessage.decode(message.getData());
                                byte[] key = message.getKeyBytes();
                                if (key == null || key.length != Long.BYTES
                                        || ByteBuffer.wrap(key).getLong() != decoded.deviceId()) {
                                    throw new IllegalArgumentException(
                                            "Telemetry key does not match payload device ID");
                                }
                                if (decoded.measurement()) {
                                    firstMeasurementReceiptEpochMs.accumulateAndGet(receivedEpochMs,
                                            (current, received) -> current == 0 ? received
                                                    : Math.min(current, received));
                                    lastMeasurementReceiptEpochMs.accumulateAndGet(receivedEpochMs, Math::max);
                                }
                                receiveLatency.recordMillis(receivedEpochMs - message.getPublishTime(),
                                        decoded.measurement());
                                tracker.received(decoded.deviceId(), decoded.sequence(), message.getMessageId(),
                                        decoded.sentNanos(), message.getTopicName(),
                                        Thread.currentThread().getName());
                                currentConsumer.acknowledgeAsync(message);
                            } catch (RuntimeException error) {
                                tracker.invalidMessage();
                                currentConsumer.negativeAcknowledge(message);
                            }
                        })
                        .subscribe();
                return new ClientAndConsumer(client, consumer);
            } catch (Throwable error) {
                client.close();
                throw error;
            }
        }

        private void restartPods(PulsarClientSharedResources sharedResources) {
            int restartCount = Math.max(1,
                    (int) Math.ceil(scenario.podsPerApplication() * scenario.behaviors().podRestarts().fraction()));
            while (!stopping.get()) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(scenario.behaviors().podRestarts().intervalSeconds()));
                    for (int i = 0; i < restartCount && !stopping.get(); i++) {
                        synchronized (pods) {
                            int podIndex = ThreadLocalRandom.current().nextInt(pods.size());
                            pods.get(podIndex).close();
                            pods.set(podIndex, createPod(sharedResources, podIndex));
                        }
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception error) {
                    restartFailure.compareAndSet(null, error);
                    return;
                }
            }
        }
    }

    private record ClientAndConsumer(PulsarClient client, Consumer<byte[]> consumer) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            consumer.close();
            client.close();
        }

        /** Closes the consumer, then the client, also when closing the consumer failed. */
        CompletableFuture<Void> closeAsync() {
            return consumer.closeAsync().handle((ignored, consumerFailure) -> consumerFailure)
                    .thenCompose(consumerFailure -> client.closeAsync().thenRun(() -> {
                        if (consumerFailure != null) {
                            throw new CompletionException(consumerFailure);
                        }
                    }));
        }
    }
}

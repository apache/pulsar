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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "iot-produce", description = "Produce keyed IoT telemetry through isolated gateway clients")
final class TelemetryProducer extends PerformanceTool.ScenarioCommand {
    private static final int STATE_VERSION = 1;

    @Option(names = "--await-measurement-start",
            description = "Before the first measured message, wait for the launcher to start the measurement over "
                    + "the control port, for example after letting the host cool down")
    boolean awaitMeasurementStart;

    // What the producer is doing, for the progress stream
    private volatile String phase = "connecting";
    private volatile int warmupRound;
    private volatile long measurementStartEpochMs = -1;

    @Override
    public Integer call() throws Exception {
        IotScenario scenario = scenario();
        Files.createDirectories(output);
        long[] deviceSequences = new long[scenario.deviceCount()];
        long[] producerSequences = new long[Math.multiplyExact(scenario.gatewayCount(), scenario.topicCount())];
        @SuppressWarnings("unchecked")
        Producer<byte[]>[] producers = new Producer[producerSequences.length];
        List<PulsarClient> clients = new ArrayList<>(scenario.gatewayCount());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicLong completed = new AtomicLong();
        AtomicLong warmupCompleted = new AtomicLong();
        AtomicLong measurementCompleted = new AtomicLong();
        HdrLatencyRecorder sendLatency = new HdrLatencyRecorder(output.resolve("gateways-latency.hdr"),
                PerformanceTool.MAX_LATENCY_MICROS);
        int maxOutstanding = Math.min(scenario.gateways().producer().maxOutstanding(), scenario.deviceCount());
        Semaphore outstanding = new Semaphore(maxOutstanding);
        Set<Integer> devicesInFlight = ConcurrentHashMap.newKeySet();

        if (awaitMeasurementStart && controlPort == null) {
            throw new IllegalArgumentException("--await-measurement-start needs --control-port");
        }
        ProgressStream progress = new ProgressStream(sendLatency, line -> {
            line.put("role", "producer");
            line.put("phase", phase);
            line.put("sent", completed.get());
            line.put("pending", maxOutstanding - outstanding.availablePermits());
            line.put("messageCount", scenario.messageCount());
            line.put("warmupMessageCount", scenario.warmupMessageCount());
            line.put("warmupRound", warmupRound);
            line.put("warmupRounds", scenario.warmupMessageCount() > 0 ? scenario.warmupRounds() : 0);
            line.put("measurementStartEpochMs", measurementStartEpochMs);
        });
        MeasurementControl control = null;
        if (controlPort != null) {
            control = MeasurementControl.start(controlPort);
            control.serveProgress(progress);
            System.out.println("CONTROL_READY port=" + control.port());
        }
        PulsarClientSharedResources sharedResources = SharedClientResources.create(
                scenario.gateways().producer().ioThreads(), scenario.gateways().producer().listenerThreads());
        try {
            for (int gateway = 0; gateway < scenario.gatewayCount(); gateway++) {
                clients.add(PulsarClient.builder()
                        .serviceUrl(scenario.serviceUrl())
                        .sharedResources(sharedResources)
                        .build());
            }
            if (scenario.gateways().producer().precreate()) {
                for (int gateway = 0; gateway < scenario.gatewayCount(); gateway++) {
                    for (int topic = 0; topic < scenario.topicCount(); topic++) {
                        int producerIndex = gateway * scenario.topicCount() + topic;
                        producers[producerIndex] = createProducer(scenario, clients, gateway, topic);
                    }
                }
            }
            phase = scenario.warmupMessageCount() > 0 ? "warmup" : "measurement";
            SplittableRandom random = new SplittableRandom(0x51c0ffeeL);
            long intervalNanos = scenario.rate() == 0 ? 0 : TimeUnit.SECONDS.toNanos(1) / scenario.rate();
            long nextSend = System.nanoTime();
            long startedNanos = nextSend;
            long runDeadlineNanos = startedNanos
                    + TimeUnit.SECONDS.toNanos(scenario.timeoutSeconds());
            long warmupMessageCount = scenario.warmupMessageCount();
            long warmupMessagesPerRound = scenario.warmupMessageCountPerRound();
            long measurementStartedNanos = -1;
            long measurementStartEpochMs = -1;
            for (long sent = 0; sent < scenario.messageCount(); sent++) {
                Throwable sendFailure = failure.get();
                if (sendFailure != null) {
                    throw new IllegalStateException("Telemetry send failed", sendFailure);
                }
                outstanding.acquire();
                int device;
                do {
                    device = random.nextInt(scenario.deviceCount());
                } while (!devicesInFlight.add(device));
                int gateway = random.nextInt(scenario.gatewayCount());
                int topic = device % scenario.topicCount();
                int producerIndex = gateway * scenario.topicCount() + topic;
                Producer<byte[]> producer = producers[producerIndex];
                if (producer == null) {
                    producer = createProducer(scenario, clients, gateway, topic);
                    producers[producerIndex] = producer;
                }

                boolean measurementMessage = sent >= warmupMessageCount;
                if (measurementMessage && measurementStartedNanos < 0) {
                    if (awaitMeasurementStart) {
                        // The warmup rounds have been received; the launcher lets the host cool down first.
                        phase = "awaiting-measurement-start";
                        control.markReady();
                        System.out.println("MEASUREMENT_READY");
                        control.awaitStart(runDeadlineNanos);
                        // Do not turn the wait into a rate-limiter catch-up burst.
                        nextSend = System.nanoTime();
                    }
                    measurementStartedNanos = System.nanoTime();
                    measurementStartEpochMs = System.currentTimeMillis();
                    this.measurementStartEpochMs = measurementStartEpochMs;
                    phase = "measurement";
                    System.out.println("MEASUREMENT_START epochMs=" + measurementStartEpochMs);
                    if (scenario.hasLateApplications()) {
                        // the applications that join later count their join time from it
                        MeasurementStartMarker.mark(coordinationDirectory(), runId, measurementStartEpochMs);
                    }
                }
                long deviceSequence = deviceSequences[device]++;
                long producerSequence = producerSequences[producerIndex]++;
                byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(device).array();
                byte[] payload = TelemetryMessage.encode(device, deviceSequence, measurementMessage,
                        scenario.payloadBytes());
                int completedDevice = device;
                long sendStartedNanos = System.nanoTime();
                producer.newMessage()
                        .keyBytes(key)
                        .sequenceId(producerSequence)
                        .value(payload)
                        .sendAsync()
                        .whenComplete((messageId, error) -> {
                            if (error != null) {
                                failure.compareAndSet(null, error);
                            } else {
                                completed.incrementAndGet();
                                sendLatency.recordNanos(System.nanoTime() - sendStartedNanos, measurementMessage);
                                if (measurementMessage) {
                                    measurementCompleted.incrementAndGet();
                                } else {
                                    warmupCompleted.incrementAndGet();
                                }
                            }
                            devicesInFlight.remove(completedDevice);
                            outstanding.release();
                        });

                nextSend += intervalNanos;
                long wait = nextSend - System.nanoTime();
                if (wait > 0) {
                    LockSupport.parkNanos(wait);
                }
                if (!measurementMessage && warmupMessagesPerRound > 0
                        && (sent + 1) % warmupMessagesPerRound == 0) {
                    awaitOutstanding(outstanding, maxOutstanding);
                    if (failure.get() != null) {
                        throw new IllegalStateException("Telemetry warmup send failed", failure.get());
                    }
                    int round = Math.toIntExact((sent + 1) / warmupMessagesPerRound);
                    warmupRound = round;
                    WarmupBarrier.awaitApplications(coordinationDirectory(), runId, round, scenario.applicationCount(),
                            runDeadlineNanos);
                    System.out.println("WARMUP_ROUND_COMPLETE round=" + round + "/" + scenario.warmupRounds()
                            + " produced=" + warmupCompleted.get()
                            + " applicationsReceived=" + scenario.applicationCount()
                            + " delaySeconds=" + scenario.warmupRoundDelaySeconds());
                    if (scenario.warmupRoundDelaySeconds() > 0) {
                        TimeUnit.SECONDS.sleep(scenario.warmupRoundDelaySeconds());
                    }
                    // Do not turn time spent draining or paused into a rate-limiter catch-up burst.
                    nextSend = System.nanoTime();
                }
            }
            phase = "draining";
            awaitOutstanding(outstanding, maxOutstanding);
            if (failure.get() != null) {
                throw new IllegalStateException("Telemetry send failed", failure.get());
            }
            phase = "finished";
            long measurementEndEpochMs = System.currentTimeMillis();
            long finishedNanos = System.nanoTime();
            long elapsedNanos = finishedNanos - startedNanos;
            long measurementElapsedNanos = finishedNanos - measurementStartedNanos;
            sendLatency.close();
            writeState(deviceSequences);
            Files.writeString(output.resolve("gateways-summary.json"),
                    "{\n  \"sent\": " + completed.get()
                            + ",\n  \"warmupMessages\": " + warmupCompleted.get()
                            + ",\n  \"warmupMessagesPerRound\": " + warmupMessagesPerRound
                            + ",\n  \"warmupRounds\": " + scenario.warmupRounds()
                            + ",\n  \"warmupRoundDelaySeconds\": " + scenario.warmupRoundDelaySeconds()
                            + ",\n  \"measurementMessages\": " + measurementCompleted.get()
                            + ",\n  \"devices\": " + scenario.deviceCount()
                            + ",\n  \"elapsedSeconds\": " + elapsedNanos / 1_000_000_000.0
                            + ",\n  \"measurementElapsedSeconds\": "
                            + measurementElapsedNanos / 1_000_000_000.0
                            + ",\n  \"wholeRunMessagesPerSecond\": "
                            + completed.get() * 1_000_000_000.0 / elapsedNanos
                            + ",\n  \"messagesPerSecond\": "
                            + measurementCompleted.get() * 1_000_000_000.0 / measurementElapsedNanos
                            + ",\n  \"measurementStartEpochMs\": " + measurementStartEpochMs
                            + ",\n  \"measurementEndEpochMs\": " + measurementEndEpochMs + "\n}\n");
        } finally {
            for (Producer<byte[]> producer : producers) {
                if (producer != null) {
                    producer.close();
                }
            }
            for (PulsarClient client : clients) {
                client.close();
            }
            sharedResources.close();
            if (!"finished".equals(phase)) {
                phase = "failed";
            }
            progress.finish();
            if (control != null) {
                control.close();
            }
        }
        return 0;
    }

    private static void awaitOutstanding(Semaphore outstanding, int permits) throws InterruptedException {
        outstanding.acquire(permits);
        outstanding.release(permits);
    }

    private Producer<byte[]> createProducer(IotScenario scenario, List<PulsarClient> clients,
                                            int gateway, int topic) throws Exception {
        return clients.get(gateway).newProducer()
                .topic(scenario.topicNames().get(topic))
                .producerName("iot-gateway-" + gateway + "-topic-" + topic)
                .batcherBuilder(BatcherBuilder.KEY_BASED)
                .enableBatching(scenario.gateways().producer().batchingEnabled())
                .blockIfQueueFull(true)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();
    }

    private void writeState(long[] sequences) throws Exception {
        try (var data = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(output.resolve("gateways-state.bin"))))) {
            data.writeInt(STATE_VERSION);
            data.writeInt(sequences.length);
            for (long sequence : sequences) {
                data.writeLong(sequence);
            }
        }
    }
}

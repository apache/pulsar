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

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
import picocli.CommandLine.Option;

@Command(name = "iot-consume", description = "Consume and validate one IoT application subscription")
final class TelemetryConsumer extends PerformanceTool.ScenarioCommand {
    @Option(names = "--application-index", required = true)
    int applicationIndex;

    @Override
    public Integer call() throws Exception {
        IotScenario scenario = scenario();
        Files.createDirectories(output);
        DeviceSequenceTracker tracker = new DeviceSequenceTracker(scenario.deviceCount());
        List<ClientAndConsumer> pods = new ArrayList<>(scenario.clientsPerApplication());
        AtomicBoolean stopping = new AtomicBoolean();
        AtomicReference<Throwable> restarterFailure = new AtomicReference<>();
        HdrLatencyRecorder receiveLatency = new HdrLatencyRecorder();
        AtomicLong firstMeasurementReceiptEpochMs = new AtomicLong();
        AtomicLong lastMeasurementReceiptEpochMs = new AtomicLong();
        int nextWarmupRound = 1;
        Thread restarter = null;

        PulsarClientSharedResources sharedResources = SharedClientResources.create(scenario);
        try {
            for (int pod = 0; pod < scenario.clientsPerApplication(); pod++) {
                pods.add(createPod(scenario, sharedResources, tracker, receiveLatency,
                        firstMeasurementReceiptEpochMs, lastMeasurementReceiptEpochMs, pod));
            }
            System.out.println("READY application=" + applicationIndex + " clients=" + pods.size());
            if (scenario.clientRestartIntervalSeconds() > 0 && scenario.clientRestartFraction() > 0) {
                restarter = new Thread(() -> restartClients(scenario, sharedResources, tracker, pods, stopping,
                                restarterFailure, receiveLatency, firstMeasurementReceiptEpochMs,
                                lastMeasurementReceiptEpochMs),
                        "iot-client-restarter");
                restarter.start();
            }

            long deadline = System.nanoTime() + Duration.ofSeconds(scenario.consumerTimeoutSeconds()).toNanos();
            while (tracker.uniqueMessages() < scenario.messageCount() && System.nanoTime() < deadline) {
                if (restarterFailure.get() != null) {
                    throw new IllegalStateException("Cannot restart IoT client", restarterFailure.get());
                }
                if (nextWarmupRound <= scenario.warmupRounds() && scenario.warmupMessageCountPerRound() > 0
                        && tracker.uniqueMessages() >= scenario.warmupMessageCountPerRound() * nextWarmupRound) {
                    WarmupBarrier.markApplicationComplete(coordinationDirectory(), runId,
                            nextWarmupRound, applicationIndex);
                    nextWarmupRound++;
                }
                Thread.sleep(100);
            }
            if (restarterFailure.get() != null) {
                throw new IllegalStateException("Cannot restart IoT client", restarterFailure.get());
            }
            stopping.set(true);
            if (restarter != null) {
                restarter.interrupt();
                restarter.join(TimeUnit.SECONDS.toMillis(10));
            }
            DeviceSequenceTracker.Summary summary = tracker.summary();
            receiveLatency.write(output.resolve("consume-latency.hdr"),
                    firstMeasurementReceiptEpochMs.get(), lastMeasurementReceiptEpochMs.get());
            tracker.writeState(output.resolve("consumed-state.bin"));
            tracker.writeViolationSamples(output.resolve("ordering-violations.txt"));
            Files.writeString(output.resolve("consumer-summary.json"), "{\n"
                    + "  \"applicationIndex\": " + applicationIndex + ",\n"
                    + "  \"uniqueMessages\": " + summary.uniqueMessages() + ",\n"
                    + "  \"duplicates\": " + summary.duplicates() + ",\n"
                    + "  \"orderingViolations\": " + summary.orderingViolations() + ",\n"
                    + "  \"invalidMessages\": " + summary.invalidMessages() + ",\n"
                    + "  \"firstMeasurementMessageReceivedEpochMs\": "
                    + firstMeasurementReceiptEpochMs.get() + ",\n"
                    + "  \"lastMeasurementMessageReceivedEpochMs\": "
                    + lastMeasurementReceiptEpochMs.get() + "\n}\n");
            return summary.valid() && summary.uniqueMessages() == scenario.messageCount() ? 0 : 1;
        } finally {
            stopping.set(true);
            synchronized (pods) {
                for (ClientAndConsumer pod : pods) {
                    pod.close();
                }
            }
            sharedResources.close();
        }
    }

    private ClientAndConsumer createPod(IotScenario scenario, PulsarClientSharedResources sharedResources,
                                        DeviceSequenceTracker tracker, HdrLatencyRecorder receiveLatency,
                                        AtomicLong firstMeasurementReceiptEpochMs,
                                        AtomicLong lastMeasurementReceiptEpochMs,
                                        int podIndex) throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(scenario.serviceUrl())
                .sharedResources(sharedResources)
                .build();
        try {
            Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                    .topics(scenario.topics())
                    .subscriptionName(scenario.subscriptionName(applicationIndex))
                    .consumerName("iot-application-" + applicationIndex + "-pod-" + podIndex)
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .messageListener((currentConsumer, message) -> {
                        long receivedEpochMs = System.currentTimeMillis();
                        try {
                            TelemetryMessage.Decoded decoded = TelemetryMessage.decode(message.getData());
                            byte[] key = message.getKeyBytes();
                            if (key == null || key.length != Long.BYTES
                                    || ByteBuffer.wrap(key).getLong() != decoded.deviceId()) {
                                throw new IllegalArgumentException("Telemetry key does not match payload device ID");
                            }
                            if (decoded.measurement()) {
                                firstMeasurementReceiptEpochMs.accumulateAndGet(receivedEpochMs,
                                        (current, received) -> current == 0 ? received : Math.min(current, received));
                                lastMeasurementReceiptEpochMs.accumulateAndGet(receivedEpochMs, Math::max);
                                receiveLatency.recordMillis(receivedEpochMs - message.getPublishTime());
                            }
                            tracker.received(decoded.deviceId(), decoded.sequence(), message.getMessageId(),
                                    decoded.sentNanos(), message.getTopicName(), Thread.currentThread().getName());
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

    private void restartClients(IotScenario scenario, PulsarClientSharedResources sharedResources,
                                DeviceSequenceTracker tracker, List<ClientAndConsumer> pods,
                                AtomicBoolean stopping, AtomicReference<Throwable> failure,
                                HdrLatencyRecorder receiveLatency, AtomicLong firstMeasurementReceiptEpochMs,
                                AtomicLong lastMeasurementReceiptEpochMs) {
        int restartCount = Math.max(1,
                (int) Math.ceil(scenario.clientsPerApplication() * scenario.clientRestartFraction()));
        while (!stopping.get()) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(scenario.clientRestartIntervalSeconds()));
                for (int i = 0; i < restartCount && !stopping.get(); i++) {
                    int index = ThreadLocalRandom.current().nextInt(pods.size());
                    synchronized (pods) {
                        ClientAndConsumer previous = pods.get(index);
                        previous.close();
                        pods.set(index, createPod(scenario, sharedResources, tracker, receiveLatency,
                                firstMeasurementReceiptEpochMs, lastMeasurementReceiptEpochMs, index));
                    }
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception error) {
                failure.compareAndSet(null, error);
                return;
            }
        }
    }

    private record ClientAndConsumer(PulsarClient client, Consumer<byte[]> consumer) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            consumer.close();
            client.close();
        }
    }
}

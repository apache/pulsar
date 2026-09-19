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

@Command(name = "iot-produce", description = "Produce keyed IoT telemetry through isolated gateway clients")
final class TelemetryProducer extends PerformanceTool.ScenarioCommand {
    private static final int STATE_VERSION = 1;

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
        Semaphore outstanding = new Semaphore(scenario.maxOutstanding());
        Set<Integer> devicesInFlight = ConcurrentHashMap.newKeySet();

        PulsarClientSharedResources sharedResources = SharedClientResources.create(scenario);
        try {
            for (int gateway = 0; gateway < scenario.gatewayCount(); gateway++) {
                clients.add(PulsarClient.builder()
                        .serviceUrl(scenario.serviceUrl())
                        .sharedResources(sharedResources)
                        .build());
            }
            if (scenario.precreateProducers()) {
                for (int gateway = 0; gateway < scenario.gatewayCount(); gateway++) {
                    for (int topic = 0; topic < scenario.topicCount(); topic++) {
                        int producerIndex = gateway * scenario.topicCount() + topic;
                        producers[producerIndex] = createProducer(scenario, clients, gateway, topic);
                    }
                }
            }
            SplittableRandom random = new SplittableRandom(0x51c0ffeeL);
            long intervalNanos = scenario.rate() == 0 ? 0 : TimeUnit.SECONDS.toNanos(1) / scenario.rate();
            long nextSend = System.nanoTime();
            long startedNanos = nextSend;
            for (long sent = 0; sent < scenario.messageCount(); sent++) {
                Throwable sendFailure = failure.get();
                if (sendFailure != null) {
                    throw new IllegalStateException("Telemetry send failed", sendFailure);
                }
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

                outstanding.acquire();
                long deviceSequence = deviceSequences[device]++;
                long producerSequence = producerSequences[producerIndex]++;
                byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(device).array();
                byte[] payload = TelemetryMessage.encode(device, deviceSequence, scenario.payloadBytes());
                int completedDevice = device;
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
                            }
                            devicesInFlight.remove(completedDevice);
                            outstanding.release();
                        });

                nextSend += intervalNanos;
                long wait = nextSend - System.nanoTime();
                if (wait > 0) {
                    LockSupport.parkNanos(wait);
                }
            }
            outstanding.acquire(scenario.maxOutstanding());
            if (failure.get() != null) {
                throw new IllegalStateException("Telemetry send failed", failure.get());
            }
            long elapsedNanos = System.nanoTime() - startedNanos;
            writeState(deviceSequences);
            Files.writeString(output.resolve("producer-summary.json"),
                    "{\n  \"sent\": " + completed.get()
                            + ",\n  \"devices\": " + scenario.deviceCount()
                            + ",\n  \"elapsedSeconds\": " + elapsedNanos / 1_000_000_000.0
                            + ",\n  \"messagesPerSecond\": "
                            + completed.get() * 1_000_000_000.0 / elapsedNanos + "\n}\n");
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
        }
        return 0;
    }

    private Producer<byte[]> createProducer(IotScenario scenario, List<PulsarClient> clients,
                                            int gateway, int topic) throws Exception {
        return clients.get(gateway).newProducer()
                .topic(scenario.topics().get(topic))
                .producerName("iot-gateway-" + gateway + "-topic-" + topic)
                .batcherBuilder(BatcherBuilder.KEY_BASED)
                .enableBatching(scenario.batchingEnabled())
                .blockIfQueueFull(true)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();
    }

    private void writeState(long[] sequences) throws Exception {
        try (var data = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(output.resolve("produced-state.bin"))))) {
            data.writeInt(STATE_VERSION);
            data.writeInt(sequences.length);
            for (long sequence : sequences) {
                data.writeLong(sequence);
            }
        }
    }
}

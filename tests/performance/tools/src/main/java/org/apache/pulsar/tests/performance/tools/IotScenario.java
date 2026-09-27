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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Configuration selected from the {@code workloads.iotTelemetry} scenario subtree.
 *
 * @param serviceUrl the broker's service URL, which the launcher sets
 * @param timeoutSeconds the longest the workload may run: the applications stop waiting for messages, and the
 *                       gateways for the warmup and the measurement's start, after it
 */
public record IotScenario(String serviceUrl, Warmup warmup, Measurement measurement, int rate, Payload payload,
                          Devices devices, Gateways gateways, Topics topics,
                          Applications applications, Behaviors behaviors, int timeoutSeconds) {
    /**
     * The traffic before the measurement, which the applications receive and check but which the measurement leaves
     * out: {@code rounds} rounds of {@code seconds} at the rate, or of {@code messages} messages, each received by
     * every application before the next starts, {@code roundDelaySeconds} apart.
     */
    public record Warmup(int seconds, long messages, int rounds, int roundDelaySeconds) {
        static final Warmup NONE = new Warmup(0, 0, 1, 0);

        public Warmup {
            // A single round unless set
            rounds = rounds == 0 ? 1 : rounds;
        }
    }

    /**
     * The measured traffic after the warmup: {@code seconds} at the rate, or {@code messages} messages when set, as
     * a workload without a rate limit needs.
     */
    public record Measurement(int seconds, long messages) {
    }

    /** The telemetry messages' payload: {@code size} bytes, the device's ID, sequence and send time included. */
    public record Payload(int size) {
    }

    /** The devices whose telemetry the gateways publish, each with its own key and sequence. */
    public record Devices(int count) {
    }

    /**
     * The gateways, each a Pulsar client with a producer per topic, in one container.
     *
     * @param env the environment variables of the gateways' container, which the launcher sets, such as
     *            {@code PULSAR_MEM}
     */
    public record Gateways(int count, Producer producer, Map<String, String> env) {
    }

    /**
     * The gateways' producer settings.
     *
     * @param maxOutstanding the most messages in flight across the gateways
     * @param precreate open every gateway's producer of every topic before the first message
     */
    public record Producer(int ioThreads, int listenerThreads, int maxOutstanding, boolean batchingEnabled,
                           boolean precreate) {
    }

    /** The telemetry topics, named {@code <prefix><index>}. */
    public record Topics(int count, String prefix) {
    }

    /**
     * The applications, each consuming every topic on a Key_Shared subscription of its own, named
     * {@code <subscriptionPrefix><index>}, through {@code podsPerApplication} pods, each a Pulsar client with a
     * consumer, in a container of its own.
     *
     * @param env the environment variables of each application's container, which the launcher sets, such as
     *            {@code PULSAR_MEM}
     */
    public record Applications(int count, int podsPerApplication, String subscriptionPrefix, Client client,
                               Map<String, String> env) {
    }

    /** The applications' client settings. */
    public record Client(int ioThreads, int listenerThreads) {
    }

    /** What happens to the simulated system during the run, beyond the traffic. */
    public record Behaviors(PodRestarts podRestarts) {
        static final Behaviors NONE = new Behaviors(PodRestarts.NONE);

        public Behaviors {
            podRestarts = podRestarts != null ? podRestarts : PodRestarts.NONE;
        }
    }

    /**
     * Each application restarts some of its pods periodically, which moves devices between the pods mid-stream.
     *
     * @param intervalSeconds how often each application restarts pods; 0 never
     * @param fraction the fraction of an application's pods that each restart replaces
     */
    public record PodRestarts(int intervalSeconds, double fraction) {
        static final PodRestarts NONE = new PodRestarts(0, 0);

        boolean enabled() {
            return intervalSeconds > 0 && fraction > 0;
        }
    }

    public IotScenario {
        warmup = warmup != null ? warmup : Warmup.NONE;
        behaviors = behaviors != null ? behaviors : Behaviors.NONE;
        if (measurement == null) {
            throw new IllegalArgumentException("The IoT scenario needs measurement");
        }
        int durationSeconds = measurement.seconds();
        long numberOfMessages = measurement.messages();
        int warmupSeconds = warmup.seconds();
        long warmupMessages = warmup.messages();
        int warmupRounds = warmup.rounds();
        int warmupRoundDelaySeconds = warmup.roundDelaySeconds();
        if (payload == null || devices == null || gateways == null || gateways.producer() == null || topics == null
                || applications == null || applications.client() == null) {
            throw new IllegalArgumentException("The IoT scenario needs payload, devices, gateways with producer, "
                    + "topics and applications with client");
        }
        if (serviceUrl == null || serviceUrl.isBlank() || topics.prefix() == null || topics.prefix().isBlank()
                || applications.subscriptionPrefix() == null || applications.subscriptionPrefix().isBlank()) {
            throw new IllegalArgumentException("Service URL, topic prefix and subscription prefix are required");
        }
        long warmupRuntimeSeconds = warmupSeconds;
        if (warmupMessages > 0 && rate > 0) {
            warmupRuntimeSeconds = Math.floorDiv(warmupMessages, rate)
                    + (warmupMessages % rate == 0 ? 0 : 1);
        }
        long minimumRuntimeSeconds = Math.addExact(durationSeconds,
                Math.addExact(Math.multiplyExact(warmupRuntimeSeconds, warmupRounds),
                        Math.multiplyExact((long) warmupRoundDelaySeconds, warmupRounds)));
        Producer producer = gateways.producer();
        Client client = applications.client();
        if (durationSeconds < 1 || warmupSeconds < 0 || warmupMessages < 0 || warmupRounds < 1
                || warmupRoundDelaySeconds < 0
                || (warmupSeconds > 0 && warmupMessages > 0)
                || (warmupSeconds > 0 && rate == 0)
                || rate < 0 || numberOfMessages < 0
                || (rate == 0 && numberOfMessages == 0) || payload.size() < TelemetryMessage.HEADER_BYTES
                || devices.count() < 1 || gateways.count() < 1 || topics.count() < 1 || applications.count() < 1
                || applications.podsPerApplication() < 1 || producer.ioThreads() < 1
                || producer.listenerThreads() < 1 || client.ioThreads() < 1 || client.listenerThreads() < 1
                || producer.maxOutstanding() < 1 || timeoutSeconds < minimumRuntimeSeconds
                || behaviors.podRestarts().intervalSeconds() < 0 || behaviors.podRestarts().fraction() < 0
                || behaviors.podRestarts().fraction() > 1) {
            throw new IllegalArgumentException("IoT scenario counts and sizes are invalid");
        }
    }

    public int warmupSeconds() {
        return warmup.seconds();
    }

    public long warmupMessages() {
        return warmup.messages();
    }

    public int warmupRounds() {
        return warmup.rounds();
    }

    public int warmupRoundDelaySeconds() {
        return warmup.roundDelaySeconds();
    }

    public int payloadBytes() {
        return payload.size();
    }

    public int deviceCount() {
        return devices.count();
    }

    public int gatewayCount() {
        return gateways.count();
    }

    public int topicCount() {
        return topics.count();
    }

    public int applicationCount() {
        return applications.count();
    }

    public int podsPerApplication() {
        return applications.podsPerApplication();
    }

    public long messageCount() {
        return Math.addExact(warmupMessageCount(), measurementMessageCount());
    }

    public long warmupMessageCount() {
        return Math.multiplyExact(warmupMessageCountPerRound(), warmup.rounds());
    }

    public long warmupMessageCountPerRound() {
        return warmup.messages() > 0
                ? warmup.messages()
                : Math.multiplyExact((long) warmup.seconds(), rate);
    }

    public long measurementMessageCount() {
        return measurement.messages() > 0 ? measurement.messages()
                : Math.multiplyExact((long) measurement.seconds(), rate);
    }

    public List<String> topicNames() {
        List<String> result = new ArrayList<>(topics.count());
        for (int i = 0; i < topics.count(); i++) {
            result.add(topics.prefix() + i);
        }
        return result;
    }

    public String subscriptionName(int applicationIndex) {
        if (applicationIndex < 0 || applicationIndex >= applications.count()) {
            throw new IllegalArgumentException("Application index out of range: " + applicationIndex);
        }
        return applications.subscriptionPrefix() + applicationIndex;
    }
}

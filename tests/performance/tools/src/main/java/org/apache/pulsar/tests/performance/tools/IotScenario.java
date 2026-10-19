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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.SubscriptionType;

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

    /**
     * The devices whose telemetry the gateways publish, each with its own key and sequence.
     *
     * @param keyType the messages' key: {@code bytes}, the default, the device's ID as 8 big-endian bytes, which the
     *                broker receives base64-encoded, or {@code string}, {@code device-<ID>} as a plain string key,
     *                which Key_Shared hashes as a string
     */
    public record Devices(int count, String keyType) {
        /** The {@code keyType} of binary keys, the default. */
        public static final String BYTES_KEYS = "bytes";
        /** The {@code keyType} of string keys. */
        public static final String STRING_KEYS = "string";
        static final List<String> KEY_TYPES = List.of(BYTES_KEYS, STRING_KEYS);

        public Devices {
            if (keyType == null) {
                keyType = BYTES_KEYS;
            }
        }

        public Devices(int count) {
            this(count, null);
        }
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
     * @param precreateConcurrency with {@code precreate}, the most producers that the gateways create at the same
     *                             time, in a random order of the gateways and topics; 1 creates them one at a time in
     *                             order
     * @param batchingMaxPublishDelayMillis with {@code batchingEnabled}, how long a producer's open batch waits for
     *                                      more messages, in milliseconds; null for the client's default, 1 ms. A
     *                                      producer receives a message every {@code gateways.count * topics.count /
     *                                      rate} seconds on average, so a batch has more than one message only when
     *                                      the delay is longer than that
     */
    public record Producer(int ioThreads, int listenerThreads, int maxOutstanding, boolean batchingEnabled,
                           boolean precreate, Integer precreateConcurrency, Integer batchingMaxPublishDelayMillis) {
        /** The default of {@code precreateConcurrency}, for scenarios written before it existed. */
        public static final int DEFAULT_PRECREATE_CONCURRENCY = 32;

        public Producer {
            if (precreateConcurrency == null) {
                precreateConcurrency = DEFAULT_PRECREATE_CONCURRENCY;
            }
        }

        /** Settings with the client's default batching delay. */
        public Producer(int ioThreads, int listenerThreads, int maxOutstanding, boolean batchingEnabled,
                        boolean precreate, Integer precreateConcurrency) {
            this(ioThreads, listenerThreads, maxOutstanding, batchingEnabled, precreate, precreateConcurrency, null);
        }
    }

    /** The telemetry topics, named {@code <prefix><index>}. */
    public record Topics(int count, String prefix) {
    }

    /**
     * The applications, in one container, as the gateways are. Each application is a subscription of its own on every
     * topic, named {@code <subscriptionPrefix><index>}, consumed through {@code podsPerApplication} pods, each a Pulsar
     * client with a consumer of the subscription; the applications differ only in their subscription.
     *
     * @param client the client resources that every pod's client shares
     * @param env the environment variables of the applications' container, which the launcher sets, such as
     *            {@code PULSAR_MEM}
     * @param joinSeconds when each application opens its pods, in seconds after the measurement starts, 0 for at the
     *                    start; empty for every application at the start. An application that joins later has its
     *                    subscription from the start, so it catches up on the backlog that builds up until it joins
     * @param caughtUpLatencyMillis an application that joined later has caught up when it first receives a measured
     *                              message within this time of its publishing
     * @param skipMessages how many messages of each topic each application that joins later skips before it joins:
     *                     the launcher moves its subscription's position forward by that many entries, which are
     *                     messages since the gateways mustn't batch then, so that the applications read the backlog
     *                     from staggered positions; empty for none. An application that skips messages expects every
     *                     message but those, each device's from the first one that it receives
     * @param unloadBeforeJoin the launcher unloads every topic before the applications that join later join, after
     *                         it has skipped their messages, so that they read the backlog from a topic loaded anew
     * @param resetToMillis where each application that joins later resets its subscription to before it joins: the
     *                      publish time this many milliseconds after the measurement's start, which the launcher
     *                      resets the subscription to by timestamp on every topic once the gateways have finished;
     *                      -1 for none, and empty for none at all. An application that resets its subscription
     *                      expects as many messages as its backlog then has, each device's from the first one that it
     *                      receives
     * @param subscriptionType the subscriptions' type: {@code Key_Shared}, the default, {@code Shared} or
     *                         {@code Failover}. A Shared subscription doesn't keep a device's messages in order, so
     *                         the applications check it for lost and duplicated messages only
     */
    public record Applications(int count, int podsPerApplication, String subscriptionPrefix, Client client,
                               Map<String, String> env, List<Integer> joinSeconds, Integer caughtUpLatencyMillis,
                               List<Long> skipMessages, boolean unloadBeforeJoin, List<Long> resetToMillis,
                               String subscriptionType) {
        /** The default of {@code caughtUpLatencyMillis}. */
        public static final int DEFAULT_CAUGHT_UP_LATENCY_MILLIS = 1000;
        /** The subscription types that the applications support. */
        static final List<SubscriptionType> SUBSCRIPTION_TYPES =
                List.of(SubscriptionType.Key_Shared, SubscriptionType.Shared, SubscriptionType.Failover);

        public Applications {
            // kept with any nulls, which the scenario's validation rejects with its message
            joinSeconds = joinSeconds != null ? Collections.unmodifiableList(new ArrayList<>(joinSeconds)) : List.of();
            skipMessages = skipMessages != null ? Collections.unmodifiableList(new ArrayList<>(skipMessages))
                    : List.of();
            resetToMillis = resetToMillis != null ? Collections.unmodifiableList(new ArrayList<>(resetToMillis))
                    : List.of();
            if (caughtUpLatencyMillis == null) {
                caughtUpLatencyMillis = DEFAULT_CAUGHT_UP_LATENCY_MILLIS;
            }
            if (subscriptionType == null) {
                subscriptionType = SubscriptionType.Key_Shared.name();
            }
        }

        public Applications(int count, int podsPerApplication, String subscriptionPrefix, Client client,
                            Map<String, String> env, List<Integer> joinSeconds, Integer caughtUpLatencyMillis) {
            this(count, podsPerApplication, subscriptionPrefix, client, env, joinSeconds, caughtUpLatencyMillis,
                    null, false);
        }

        public Applications(int count, int podsPerApplication, String subscriptionPrefix, Client client,
                            Map<String, String> env, List<Integer> joinSeconds, Integer caughtUpLatencyMillis,
                            List<Long> skipMessages, boolean unloadBeforeJoin) {
            this(count, podsPerApplication, subscriptionPrefix, client, env, joinSeconds, caughtUpLatencyMillis,
                    skipMessages, unloadBeforeJoin, null);
        }

        public Applications(int count, int podsPerApplication, String subscriptionPrefix, Client client,
                            Map<String, String> env, List<Integer> joinSeconds, Integer caughtUpLatencyMillis,
                            List<Long> skipMessages, boolean unloadBeforeJoin, List<Long> resetToMillis) {
            this(count, podsPerApplication, subscriptionPrefix, client, env, joinSeconds, caughtUpLatencyMillis,
                    skipMessages, unloadBeforeJoin, resetToMillis, null);
        }

        public Applications(int count, int podsPerApplication, String subscriptionPrefix, Client client,
                            Map<String, String> env) {
            this(count, podsPerApplication, subscriptionPrefix, client, env, null, null);
        }
    }

    /** The applications' client resources: the event loop's and the listener thread pool's threads. */
    public record Client(int ioThreads, int listenerThreads) {
    }

    /** What happens to the simulated system during the run, beyond the traffic. */
    public record Behaviors(PodRestarts podRestarts, PodPauses podPauses) {
        static final Behaviors NONE = new Behaviors(PodRestarts.NONE, PodPauses.NONE);

        public Behaviors {
            podRestarts = podRestarts != null ? podRestarts : PodRestarts.NONE;
            podPauses = podPauses != null ? podPauses : PodPauses.NONE;
        }

        public Behaviors(PodRestarts podRestarts) {
            this(podRestarts, null);
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

    /**
     * Each application pauses some of its pods periodically, as a pod stalled by a garbage collection, a slow
     * downstream or a debugger would: a paused pod's consumer stops asking for messages, so the broker holds back its
     * keys' messages, which a Key_Shared subscription keeps in its replay queue, until it resumes.
     *
     * @param intervalSeconds how often each application pauses pods; 0 never
     * @param fraction the fraction of an application's pods that each pause stops
     * @param pauseMillis how long the pods stay paused, shorter than the interval
     */
    public record PodPauses(int intervalSeconds, double fraction, int pauseMillis) {
        static final PodPauses NONE = new PodPauses(0, 0, 0);

        boolean enabled() {
            return intervalSeconds > 0 && fraction > 0 && pauseMillis > 0;
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
        PodRestarts podRestarts = behaviors.podRestarts();
        PodPauses podPauses = behaviors.podPauses();
        require(durationSeconds >= 1, "measurement.seconds must be at least 1");
        require(warmupSeconds >= 0 && warmupMessages >= 0 && warmupRoundDelaySeconds >= 0,
                "warmup.seconds, warmup.messages and warmup.roundDelaySeconds must not be negative");
        require(warmupRounds >= 1, "warmup.rounds must be at least 1");
        require(warmupSeconds == 0 || warmupMessages == 0, "Set warmup.seconds or warmup.messages, not both");
        require(warmupSeconds == 0 || rate > 0, "warmup.seconds needs a rate; without one, set warmup.messages");
        require(rate >= 0 && numberOfMessages >= 0, "rate and measurement.messages must not be negative");
        require(rate > 0 || numberOfMessages > 0, "Without a rate, set measurement.messages");
        require(payload.size() >= TelemetryMessage.HEADER_BYTES,
                "payload.size must be at least " + TelemetryMessage.HEADER_BYTES + " bytes, the message header");
        require(devices.count() >= 1 && gateways.count() >= 1 && topics.count() >= 1 && applications.count() >= 1
                        && applications.podsPerApplication() >= 1,
                "devices.count, gateways.count, topics.count, applications.count and "
                        + "applications.podsPerApplication must be at least 1");
        require(Devices.KEY_TYPES.contains(devices.keyType()),
                "devices.keyType must be one of " + Devices.KEY_TYPES + ", not " + devices.keyType());
        require(producer.ioThreads() >= 1 && producer.listenerThreads() >= 1 && client.ioThreads() >= 1
                        && client.listenerThreads() >= 1,
                "The gateways' and the applications' ioThreads and listenerThreads must be at least 1");
        require(producer.maxOutstanding() >= 1, "gateways.producer.maxOutstanding must be at least 1");
        require(applications.joinSeconds().isEmpty() || applications.joinSeconds().size() == applications.count(),
                "applications.joinSeconds must have a value for each of the " + applications.count()
                        + " applications, or none");
        // the joins count from the measurement's start, after the warmup
        long warmupTotalSeconds = minimumRuntimeSeconds - durationSeconds;
        require(applications.joinSeconds().stream().allMatch(seconds -> seconds != null && seconds >= 0
                        && warmupTotalSeconds + seconds < timeoutSeconds),
                "applications.joinSeconds must be at least 0 with no null value, and the warmup ("
                        + warmupTotalSeconds + " s) and the latest join must be within timeoutSeconds");
        require(applications.caughtUpLatencyMillis() >= 1, "applications.caughtUpLatencyMillis must be at least 1");
        // as messageCount() counts them, which the fields aren't assigned for yet
        long totalMessages = (warmupMessages > 0 ? warmupMessages : (long) warmupSeconds * rate) * warmupRounds
                + (numberOfMessages > 0 ? numberOfMessages : (long) durationSeconds * rate);
        require(applications.skipMessages().isEmpty() || applications.skipMessages().size() == applications.count(),
                "applications.skipMessages must have a value for each of the " + applications.count()
                        + " applications, or none");
        for (int index = 0; index < applications.skipMessages().size(); index++) {
            Long skipped = applications.skipMessages().get(index);
            require(skipped != null && skipped >= 0, "applications.skipMessages must be at least 0 with no null value");
            // An application that receives from the start can't skip what it may already have received
            require(skipped == 0 || !applications.joinSeconds().isEmpty() && applications.joinSeconds().get(index) > 0,
                    "applications.skipMessages: only an application that joins later, with joinSeconds above 0,"
                            + " can skip messages, not application " + index);
            require(skipped == 0 || Math.multiplyExact(skipped, (long) topics.count()) < totalMessages,
                    "applications.skipMessages must leave messages to receive: application " + index + " skips "
                            + skipped + " of each of the " + topics.count()
                            + " topics");
        }
        // The launcher skips entries, which are the messages only when the gateways don't batch
        require(applications.skipMessages().stream().allMatch(skipped -> skipped == null || skipped == 0)
                        || !producer.batchingEnabled(),
                "applications.skipMessages needs gateways.producer.batchingEnabled: false, since the launcher skips"
                        + " entries");
        require(applications.resetToMillis().isEmpty()
                        || applications.resetToMillis().size() == applications.count(),
                "applications.resetToMillis must have a value for each of the " + applications.count()
                        + " applications, or none");
        for (int index = 0; index < applications.resetToMillis().size(); index++) {
            Long resetTo = applications.resetToMillis().get(index);
            require(resetTo != null && resetTo >= -1,
                    "applications.resetToMillis must be -1 for none, or at least 0, with no null value");
            require(resetTo == -1 || !applications.joinSeconds().isEmpty() && applications.joinSeconds().get(index) > 0,
                    "applications.resetToMillis: only an application that joins later, with joinSeconds above 0,"
                            + " can reset its subscription, not application " + index);
            require(resetTo == -1 || applications.skipMessages().isEmpty()
                            || applications.skipMessages().get(index) == null
                            || applications.skipMessages().get(index) == 0,
                    "applications.resetToMillis: application " + index + " can't also skip messages");
        }
        // The applications expect as many messages as the backlog has, which counts messages only without batching
        require(applications.resetToMillis().stream().allMatch(resetTo -> resetTo == null || resetTo == -1)
                        || !producer.batchingEnabled(),
                "applications.resetToMillis needs gateways.producer.batchingEnabled: false");
        require(!applications.unloadBeforeJoin() || applications.joinSeconds().stream()
                        .anyMatch(seconds -> seconds != null && seconds > 0),
                "applications.unloadBeforeJoin needs an application that joins later, with joinSeconds above 0");
        require(Applications.SUBSCRIPTION_TYPES.stream()
                        .anyMatch(type -> type.name().equals(applications.subscriptionType())),
                "applications.subscriptionType must be one of " + Applications.SUBSCRIPTION_TYPES + ", not "
                        + applications.subscriptionType());
        // An application that skips messages or resets its subscription expects each device's messages from the first
        // one that it receives, which is the device's first one only when the subscription keeps the device's order
        require(!SubscriptionType.Shared.name().equals(applications.subscriptionType())
                        || applications.skipMessages().stream().allMatch(skipped -> skipped == null || skipped == 0)
                        && applications.resetToMillis().stream().allMatch(resetTo -> resetTo == null || resetTo == -1),
                "applications.skipMessages and applications.resetToMillis need a subscription type that keeps each"
                        + " device's order, not Shared");
        require(producer.precreateConcurrency() >= 1, "gateways.producer.precreateConcurrency must be at least 1");
        require(producer.batchingMaxPublishDelayMillis() == null || producer.batchingMaxPublishDelayMillis() >= 1,
                "gateways.producer.batchingMaxPublishDelayMillis must be at least 1");
        if (timeoutSeconds < minimumRuntimeSeconds) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid IoT scenario: timeoutSeconds is %d,"
                            + " but the workload needs %d s: %d s of warmup (%d round(s) of %d s%s) and %d s of"
                            + " measurement%s. The applications stop waiting at the timeout, so set timeoutSeconds to"
                            + " at least %d, and better about %d, which leaves room for startup and for the"
                            + " applications to catch up.",
                    timeoutSeconds, minimumRuntimeSeconds, warmupTotalSeconds, warmupRounds, warmupRuntimeSeconds,
                    warmupRoundDelaySeconds > 0 ? String.format(Locale.ROOT, " and a %d s delay after each",
                            warmupRoundDelaySeconds) : "",
                    durationSeconds, rate > 0 ? String.format(Locale.ROOT, " at %,d msg/s", rate) : "",
                    minimumRuntimeSeconds, minimumRuntimeSeconds + Math.max(100, minimumRuntimeSeconds / 5)));
        }
        require(podRestarts.intervalSeconds() >= 0 && podRestarts.fraction() >= 0 && podRestarts.fraction() <= 1,
                "behaviors.podRestarts.intervalSeconds must not be negative, and fraction must be from 0 to 1");
        require(podPauses.intervalSeconds() >= 0 && podPauses.fraction() >= 0 && podPauses.fraction() <= 1
                        && podPauses.pauseMillis() >= 0,
                "behaviors.podPauses.intervalSeconds and pauseMillis must not be negative, and fraction must be from 0"
                        + " to 1");
        require(!podPauses.enabled()
                        || podPauses.pauseMillis() < TimeUnit.SECONDS.toMillis(podPauses.intervalSeconds()),
                "behaviors.podPauses.pauseMillis must be shorter than intervalSeconds");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("Invalid IoT scenario: " + message);
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

    /** Whether the gateways key the messages with strings rather than with bytes. */
    public boolean stringKeys() {
        return Devices.STRING_KEYS.equals(devices.keyType());
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

    /** The applications' subscription type. */
    public SubscriptionType subscriptionType() {
        return SubscriptionType.valueOf(applications.subscriptionType());
    }

    /** Whether the subscription type keeps each device's messages in order, which the applications then check. */
    public boolean keepsDeviceOrder() {
        return subscriptionType() != SubscriptionType.Shared;
    }

    /** When the application opens its pods, in seconds after the measurement starts; 0 for at the start. */
    public int joinSeconds(int applicationIndex) {
        return applications.joinSeconds().isEmpty() ? 0 : applications.joinSeconds().get(applicationIndex);
    }

    /** Whether any application joins after the measurement starts. */
    public boolean hasLateApplications() {
        return applications.joinSeconds().stream().anyMatch(seconds -> seconds > 0);
    }

    /** How many messages of each topic the application skips before it joins; 0 for none. */
    public long skipMessages(int applicationIndex) {
        return applications.skipMessages().isEmpty() ? 0 : applications.skipMessages().get(applicationIndex);
    }

    /** Where the application resets its subscription to, in milliseconds after the measurement's start; -1 for none. */
    public long resetToMillis(int applicationIndex) {
        return applications.resetToMillis().isEmpty() ? -1 : applications.resetToMillis().get(applicationIndex);
    }

    /**
     * Whether the application receives each device's messages from the first one that it receives, since it skips
     * messages or resets its subscription.
     */
    public boolean startsAtFirstSequence(int applicationIndex) {
        return skipMessages(applicationIndex) > 0 || resetToMillis(applicationIndex) >= 0;
    }

    /**
     * The messages that the application receives: every message but those it skips. For an application that resets
     * its subscription, the backlog after the reset, which the launcher tells it.
     */
    public long expectedMessageCount(int applicationIndex) {
        return messageCount() - Math.multiplyExact(skipMessages(applicationIndex), (long) topics.count());
    }

    /**
     * Whether the launcher prepares the late applications' join, by skipping messages or unloading the topics, which
     * they wait for.
     */
    public boolean preparesJoin() {
        return applications.unloadBeforeJoin() || applications.skipMessages().stream().anyMatch(skipped -> skipped > 0)
                || resetsSubscriptions();
    }

    /** Whether an application resets its subscription by timestamp before it joins. */
    public boolean resetsSubscriptions() {
        return applications.resetToMillis().stream().anyMatch(resetTo -> resetTo >= 0);
    }

    /** When the earliest application that joins later joins, in seconds after the measurement starts. */
    public int earliestLateJoinSeconds() {
        return applications.joinSeconds().stream().filter(seconds -> seconds > 0).mapToInt(Integer::intValue).min()
                .orElseThrow(() -> new IllegalStateException("No application joins later"));
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
            result.add(topicName(i));
        }
        return result;
    }

    /** The topic with the index, from 0 to {@code topics.count} - 1. */
    public String topicName(int topicIndex) {
        return topics.prefix() + topicIndex;
    }

    public String subscriptionName(int applicationIndex) {
        if (applicationIndex < 0 || applicationIndex >= applications.count()) {
            throw new IllegalArgumentException("Application index out of range: " + applicationIndex);
        }
        return applications.subscriptionPrefix() + applicationIndex;
    }
}

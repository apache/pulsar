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
package org.apache.pulsar.tests.performance.launcher;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A scenario's {@code network} section: the network conditions (delay, jitter, loss, a rate limit) and the network
 * faults (partitions, connection resets, silent disconnections) that the launcher applies between the run's containers,
 * each for the whole run or for a time. See {@code docs/network-conditions.md}.
 *
 * <pre>
 * network:
 *   offloads: auto
 *   impairments:
 *     gateway-wan:
 *       between: [gateways, brokers]
 *       delay: 20ms
 *       jitter: 5ms
 *       loss: 0.1%
 *     broker-1-isolated:
 *       between: [clients, broker-1]
 *       partition: true
 *       start: 30s
 *       duration: 20s
 * </pre>
 *
 * <p>The impairments are a mapping by name, so that a scenario that extends another adds impairments or changes one
 * of them, as the scenario files merge mappings and replace lists.
 */
record NetworkSettings(Offloads offloads, List<Impairment> impairments) {
    static final NetworkSettings NONE = new NetworkSettings(Offloads.AUTO, List.of());
    static final String OFFLOADS = "offloads";
    static final String IMPAIRMENTS = "impairments";
    private static final List<String> KEYS = List.of(OFFLOADS, IMPAIRMENTS);
    static final String BETWEEN = "between";
    static final String FROM = "from";
    static final String TO = "to";
    static final String DELAY = "delay";
    static final String JITTER = "jitter";
    static final String REORDER = "reorder";
    static final String LOSS = "loss";
    static final String LOSS_BURST = "lossBurst";
    static final String RATE = "rate";
    static final String QUEUE = "queue";
    static final String PARTITION = "partition";
    static final String RESET = "reset";
    static final String SILENT_DISCONNECT = "silentDisconnect";
    static final String ANCHOR = "anchor";
    static final String START = "start";
    static final String DURATION = "duration";
    private static final List<String> IMPAIRMENT_KEYS = List.of(BETWEEN, FROM, TO, DELAY, JITTER, REORDER, LOSS,
            LOSS_BURST, RATE, QUEUE, PARTITION, RESET, SILENT_DISCONNECT, ANCHOR, START, DURATION);
    private static final List<String> CONDITION_KEYS = List.of(DELAY, JITTER, REORDER, LOSS, LOSS_BURST, RATE,
            QUEUE);
    /** The endpoints that name a group of containers, beside broker-N and bookie-N. */
    static final String GATEWAYS = "gateways";
    static final String APPLICATIONS = "applications";
    static final String CLIENTS = "clients";
    static final String BROKERS = "brokers";
    static final String BOOKIES = "bookies";
    static final String ZOOKEEPER = "zookeeper";
    static final String CONFIGURATION_STORE = "configuration-store";
    static final String METADATA = "metadata";
    /** The endpoints of the remote cluster, which a scenario with {@code cluster.remote} has. */
    static final String REMOTE_PREFIX = "remote-";
    static final String REMOTE_BROKERS = REMOTE_PREFIX + BROKERS;
    static final String REMOTE_BOOKIES = REMOTE_PREFIX + BOOKIES;
    static final String REMOTE_ZOOKEEPER = REMOTE_PREFIX + ZOOKEEPER;
    static final String REMOTE_APPLICATIONS = REMOTE_PREFIX + APPLICATIONS;
    /**
     * Every container on the first cluster's side: its clients, brokers, bookies and ZooKeeper, and the configuration
     * store, which both clusters share.
     */
    static final String LOCAL_REGION = "local-region";
    /** Every container on the remote cluster's side: its brokers, bookies, ZooKeeper and applications. */
    static final String REMOTE_REGION = "remote-region";
    private static final List<String> GROUPS = List.of(GATEWAYS, APPLICATIONS, CLIENTS, BROKERS, BOOKIES, ZOOKEEPER,
            CONFIGURATION_STORE, METADATA);
    private static final List<String> REMOTE_GROUPS = List.of(REMOTE_BROKERS, REMOTE_BOOKIES, REMOTE_ZOOKEEPER,
            REMOTE_APPLICATIONS, LOCAL_REGION, REMOTE_REGION);
    // Without leading zeros, as the launcher names the containers
    private static final Pattern INDEXED =
            Pattern.compile("((?:" + REMOTE_PREFIX + ")?)(broker|bookie)-(0|[1-9][0-9]*)");
    /** A name usable in file names, Grafana's annotations and the report. */
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");
    private static final Pattern DURATION_VALUE = Pattern.compile("([0-9]+(?:\\.[0-9]+)?)(us|ms|s|m)");
    private static final Pattern RATE_VALUE = Pattern.compile("([0-9]+(?:\\.[0-9]+)?)(bit|kbit|mbit|gbit)");
    private static final Pattern PERCENT_VALUE = Pattern.compile("([0-9]+(?:\\.[0-9]+)?)%?");
    /** The bottleneck queue of a rate limit, unless the impairment sets it: a typical uplink's buffer. */
    static final Duration DEFAULT_QUEUE = Duration.ofMillis(100);
    /**
     * The impairments that a device can take: {@code prio} has at most 16 bands, and the first three carry the
     * unimpaired traffic.
     */
    static final int MAX_IMPAIRMENTS_PER_DEVICE = 13;

    /**
     * Where the TCP segmentation offloads (TSO, GSO) of the containers' interfaces are turned off: with them, netem
     * sees GSO super-packets of up to 64 KB, so that a loss drops a burst of segments and a rate limit is bursty.
     */
    enum Offloads {
        /** Off on the containers that send through an impairment with a loss, a rate or a jitter. */
        AUTO,
        /** Off on every container of the run, for an unimpaired run to compare with a lossy one. */
        OFF,
        /** Never changed. */
        ON
    }

    /** What an impairment does. */
    enum Kind {
        /** Delays, drops or limits the packets. */
        CONDITION,
        /** Drops every packet. */
        PARTITION,
        /** Destroys the TCP connections once, which resets them on both ends. */
        RESET,
        /**
         * Destroys the senders' TCP connections to the receivers once without the receivers knowing: the connections'
         * packets are dropped first, so that the senders' resets never arrive, and the receivers keep half-open
         * connections, as when a NAT gateway or a firewall that forgot the connections answers only the senders'
         * packets with resets, or a sender's host restarts behind one.
         */
        SILENT_DISCONNECT
    }

    /** What an impairment's start counts from. */
    enum Anchor {
        /** When the gateways start creating their clients, before the warmup. */
        RUN,
        /** When the producers start sending the measured messages, after the warmup. */
        MEASUREMENT
    }

    /**
     * An impairment.
     *
     * @param name its name in the scenario
     * @param from the endpoints whose packets it impairs, such as {@code gateways}
     * @param to the endpoints that the packets go to
     * @param bothDirections whether it impairs the packets from {@code to} to {@code from} too ({@code between})
     * @param kind what it does
     * @param delay the packets' delay, one way; zero for none
     * @param jitter the delay's variation; zero for none
     * @param reorder whether the jitter may reorder the packets, as netem's independent delays do; without it, a
     *     packet is never sent before an earlier one
     * @param lossPercent the percentage of packets lost
     * @param lossBurst the mean number of packets lost in a row; 1 for independent losses
     * @param rateBitsPerSecond the sender's rate limit, in bits per second; 0 for none
     * @param queue the bottleneck queue of the rate limit, as a time at the rate
     * @param anchor what {@code start} counts from
     * @param start when it starts, after its anchor; null when it lasts the whole run, applied before the gateways
     *     start
     * @param duration how long it lasts; null until the run ends
     */
    record Impairment(String name, List<String> from, List<String> to, boolean bothDirections, Kind kind,
                      Duration delay, Duration jitter, boolean reorder, double lossPercent,
                      double lossBurst, long rateBitsPerSecond, Duration queue, Anchor anchor,
                      Duration start, Duration duration) {
        boolean wholeRun() {
            return start == null;
        }

        /**
         * Whether it acts on the path to the receiver: a delay, a jitter or a loss, a partition, or the dropped packets
         * of a silent disconnection.
         */
        boolean pathImpairment() {
            return kind == Kind.PARTITION || kind == Kind.SILENT_DISCONNECT || (kind == Kind.CONDITION
                    && (!delay.isZero() || !jitter.isZero() || lossPercent > 0));
        }

        /** Whether it limits the senders' rate. */
        boolean rateLimited() {
            return kind == Kind.CONDITION && rateBitsPerSecond > 0;
        }

        /**
         * Whether netem would see GSO super-packets in a way that distorts it: a loss would drop a burst of segments,
         * a rate would be bursty, and a jitter would delay a burst of segments together.
         */
        boolean needsSegmentOffloadsOff() {
            return kind == Kind.CONDITION && (lossPercent > 0 || rateBitsPerSecond > 0 || !jitter.isZero());
        }

        /** A short description of what it does, such as "20 ms ± 5 ms delay, 0.1 % loss, 20 Mbit/s". */
        String description() {
            if (kind == Kind.PARTITION) {
                return "partition";
            }
            if (kind == Kind.RESET) {
                return "connection reset";
            }
            if (kind == Kind.SILENT_DISCONNECT) {
                return "silent disconnection";
            }
            List<String> parts = new ArrayList<>();
            if (!delay.isZero()) {
                parts.add(millis(delay) + (jitter.isZero() ? "" : " ± " + millis(jitter)) + " delay"
                        + (reorder ? ", reordering" : ""));
            }
            if (lossPercent > 0) {
                parts.add(number(lossPercent) + " % loss"
                        + (lossBurst > 1 ? " in bursts of " + number(lossBurst) : ""));
            }
            if (rateBitsPerSecond > 0) {
                // The queue holds at least TcCommands.MIN_RATE_LIMIT frames, which is longer at a low rate
                long packets = TcCommands.rateLimit(rateBitsPerSecond, queue);
                Duration effective = Duration.ofNanos(packets * TcCommands.FRAME_BYTES * 8 * 1_000_000_000L
                        / rateBitsPerSecond);
                parts.add(rate(rateBitsPerSecond) + " with a " + millis(effective.compareTo(queue) > 0
                        ? effective.truncatedTo(ChronoUnit.MILLIS) : queue) + " queue");
            }
            return String.join(", ", parts);
        }

        /** The endpoints, as the scenario names them, such as "gateways ↔ brokers". */
        String endpoints() {
            return String.join(" + ", from) + (bothDirections ? " ↔ " : " → ") + String.join(" + ", to);
        }
    }

    /**
     * The containers that the endpoints can name: how many brokers and bookies the cluster has, and the remote cluster,
     * when the scenario has one, with its brokers and bookies; a topology without a remote cluster has none of them.
     */
    record Topology(int brokers, int bookies, int remoteBrokers, int remoteBookies) {
        Topology(int brokers, int bookies) {
            this(brokers, bookies, 0, 0);
        }

        /** Whether the scenario has a remote cluster. */
        boolean remote() {
            return remoteBrokers > 0;
        }
    }

    /**
     * Reads the {@code network} section, which may be missing, and checks it against the cluster's topology and the
     * measurement's estimated length in seconds, or a negative number when it isn't known.
     */
    static NetworkSettings read(JsonNode network, Topology topology, double measurementSeconds) {
        if (network.isMissingNode() || network.isNull()) {
            return NONE;
        }
        if (!network.isObject()) {
            throw new IllegalArgumentException("network must be a mapping with " + KEYS);
        }
        network.fieldNames().forEachRemaining(field -> {
            if (!KEYS.contains(field)) {
                throw new IllegalArgumentException("network." + field + " isn't a setting; network has " + KEYS);
            }
        });
        Offloads offloads = Offloads.AUTO;
        JsonNode offloadsSetting = network.path(OFFLOADS);
        if (offloadsSetting.isBoolean()) {
            // YAML reads an unquoted off or on as a boolean
            offloads = offloadsSetting.booleanValue() ? Offloads.ON : Offloads.OFF;
        } else if (!offloadsSetting.isMissingNode() && !offloadsSetting.isNull()) {
            try {
                offloads = Offloads.valueOf(offloadsSetting.asText().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("network." + OFFLOADS + " must be auto, off or on, not "
                        + offloadsSetting.asText());
            }
        }
        List<Impairment> impairments = new ArrayList<>();
        JsonNode section = network.path(IMPAIRMENTS);
        if (!section.isMissingNode() && !section.isNull()) {
            if (!section.isObject()) {
                throw new IllegalArgumentException("network." + IMPAIRMENTS + " must be a mapping of impairments by"
                        + " name");
            }
            section.properties().forEach(entry -> {
                // An overlay removes an impairment by setting it to null, which the scenario loader removes
                if (!entry.getValue().isNull()) {
                    impairments.add(impairment(entry.getKey(), entry.getValue(), topology, measurementSeconds));
                }
            });
        }
        NetworkSettings settings = new NetworkSettings(offloads, List.copyOf(impairments));
        checkOverlaps(settings, topology);
        checkDevices(settings, topology);
        return settings;
    }

    /** Whether the network needs the impairments' sidecar: an impairment, or the offloads off everywhere. */
    boolean any() {
        return !impairments.isEmpty() || offloads == Offloads.OFF;
    }

    /** Whether an impairment starts at a time after the measurement's start. */
    boolean anyMeasurementAnchored() {
        return impairments.stream().anyMatch(impairment -> !impairment.wholeRun()
                && impairment.anchor() == Anchor.MEASUREMENT);
    }

    /**
     * The containers that an endpoint names, as the launcher names them: {@code gateways}, {@code applications},
     * {@code broker-N}, {@code bookie-N}, {@code zookeeper} and {@code configuration-store}, and with a remote cluster
     * {@code remote-broker-N}, {@code remote-bookie-N}, {@code remote-zookeeper} and {@code remote-applications}.
     */
    static List<String> containers(String endpoint, Topology topology) {
        if (!topology.remote()) {
            Matcher indexed = INDEXED.matcher(endpoint);
            if (endpoint.equals(LOCAL_REGION)) {
                throw new IllegalArgumentException("is the first cluster's side of a geo-replicated pair, which needs"
                        + " a remote cluster: add cluster.remote, as network/geo-replication does");
            }
            if (REMOTE_GROUPS.contains(endpoint) || indexed.matches() && !indexed.group(1).isEmpty()) {
                throw new IllegalArgumentException("is an endpoint of the remote cluster, which the scenario hasn't:"
                        + " add cluster.remote, as network/geo-replication does");
            }
        }
        return switch (endpoint) {
            case GATEWAYS, APPLICATIONS, ZOOKEEPER, CONFIGURATION_STORE, REMOTE_ZOOKEEPER, REMOTE_APPLICATIONS ->
                    List.of(endpoint);
            case CLIENTS -> List.of(GATEWAYS, APPLICATIONS);
            case METADATA -> List.of(ZOOKEEPER, CONFIGURATION_STORE);
            case BROKERS -> indexed("broker", topology.brokers());
            case BOOKIES -> indexed("bookie", topology.bookies());
            case REMOTE_BROKERS -> indexed(REMOTE_PREFIX + "broker", topology.remoteBrokers());
            case REMOTE_BOOKIES -> indexed(REMOTE_PREFIX + "bookie", topology.remoteBookies());
            case LOCAL_REGION -> containers(List.of(CLIENTS, BROKERS, BOOKIES, METADATA), topology);
            case REMOTE_REGION -> containers(List.of(REMOTE_BROKERS, REMOTE_BOOKIES, REMOTE_ZOOKEEPER,
                    REMOTE_APPLICATIONS), topology);
            default -> {
                Matcher matcher = INDEXED.matcher(endpoint);
                if (!matcher.matches()) {
                    throw new IllegalArgumentException("isn't an endpoint; the endpoints are " + GROUPS
                            + ", broker-N and bookie-N, and with a remote cluster " + REMOTE_GROUPS
                            + ", remote-broker-N and remote-bookie-N");
                }
                boolean remote = !matcher.group(1).isEmpty();
                String kind = matcher.group(2);
                int index = Integer.parseInt(matcher.group(3));
                int count = kind.equals("broker") ? (remote ? topology.remoteBrokers() : topology.brokers())
                        : (remote ? topology.remoteBookies() : topology.bookies());
                if (index >= count) {
                    throw new IllegalArgumentException("isn't an endpoint: the " + (remote ? "remote " : "")
                            + "cluster has " + count + " " + kind + "(s), from " + matcher.group(1) + kind + "-0");
                }
                yield List.of(endpoint);
            }
        };
    }

    /** The containers that a list of endpoints names, in order, each once. */
    static List<String> containers(List<String> endpoints, Topology topology) {
        Set<String> containers = new LinkedHashSet<>();
        endpoints.forEach(endpoint -> containers.addAll(containers(endpoint, topology)));
        return List.copyOf(containers);
    }

    /** A sender and a receiver container whose packets an impairment impairs. */
    record Pair(String sender, String receiver) {
    }

    /** The directed pairs of containers that an impairment impairs. */
    static List<Pair> pairs(Impairment impairment, Topology topology) {
        List<String> from = containers(impairment.from(), topology);
        List<String> to = containers(impairment.to(), topology);
        List<Pair> pairs = new ArrayList<>();
        for (String sender : from) {
            for (String receiver : to) {
                pairs.add(new Pair(sender, receiver));
            }
        }
        if (impairment.bothDirections()) {
            for (String sender : to) {
                for (String receiver : from) {
                    pairs.add(new Pair(sender, receiver));
                }
            }
        }
        return pairs;
    }

    private static List<String> indexed(String prefix, int count) {
        List<String> names = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            names.add(prefix + "-" + index);
        }
        return names;
    }

    private static Impairment impairment(String name, JsonNode section, Topology topology,
                                         double measurementSeconds) {
        String path = "network." + IMPAIRMENTS + "." + name;
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(path + ": an impairment's name has letters, digits, '.', '_' and '-'");
        }
        if (!section.isObject()) {
            throw new IllegalArgumentException(path + " must be a mapping with " + IMPAIRMENT_KEYS);
        }
        section.fieldNames().forEachRemaining(field -> {
            if (!IMPAIRMENT_KEYS.contains(field)) {
                throw new IllegalArgumentException(path + "." + field + " isn't a setting; an impairment has "
                        + IMPAIRMENT_KEYS);
            }
        });
        List<String> from;
        List<String> to;
        boolean bothDirections;
        if (section.has(BETWEEN)) {
            if (section.has(FROM) || section.has(TO)) {
                throw new IllegalArgumentException(path + " has " + BETWEEN + " or " + FROM + " and " + TO
                        + ", not both");
            }
            JsonNode between = section.get(BETWEEN);
            if (!between.isArray() || between.size() != 2) {
                throw new IllegalArgumentException(path + "." + BETWEEN + " must be a list of two sides, such as"
                        + " [gateways, brokers]");
            }
            from = endpoints(between.get(0), path + "." + BETWEEN, topology);
            to = endpoints(between.get(1), path + "." + BETWEEN, topology);
            bothDirections = true;
        } else {
            if (!section.has(FROM) || !section.has(TO)) {
                throw new IllegalArgumentException(path + " needs " + BETWEEN + ", or " + FROM + " and " + TO);
            }
            from = endpoints(section.get(FROM), path + "." + FROM, topology);
            to = endpoints(section.get(TO), path + "." + TO, topology);
            bothDirections = false;
        }
        Set<String> common = new LinkedHashSet<>(containers(from, topology));
        common.retainAll(containers(to, topology));
        if (!common.isEmpty()) {
            throw new IllegalArgumentException(path + ": both sides have " + common);
        }
        boolean partition = bool(section, PARTITION, path);
        boolean reset = bool(section, RESET, path);
        boolean silentDisconnect = bool(section, SILENT_DISCONNECT, path);
        List<String> conditionKeys = CONDITION_KEYS.stream().filter(section::has).toList();
        List<String> faults = new ArrayList<>();
        if (partition) {
            faults.add(PARTITION);
        }
        if (reset) {
            faults.add(RESET);
        }
        if (silentDisconnect) {
            faults.add(SILENT_DISCONNECT);
        }
        Kind kind;
        if (!faults.isEmpty()) {
            if (faults.size() > 1) {
                throw new IllegalArgumentException(path + " is one of " + faults + ", not several");
            }
            if (!conditionKeys.isEmpty()) {
                throw new IllegalArgumentException(path + " is a " + faults.get(0) + ", which takes no "
                        + conditionKeys);
            }
            kind = partition ? Kind.PARTITION : reset ? Kind.RESET : Kind.SILENT_DISCONNECT;
        } else {
            if (conditionKeys.isEmpty()) {
                throw new IllegalArgumentException(path + " does nothing: it needs a condition " + CONDITION_KEYS
                        + ", " + PARTITION + ": true, " + RESET + ": true or " + SILENT_DISCONNECT + ": true");
            }
            kind = Kind.CONDITION;
        }
        Duration delay = duration(section.path(DELAY), path + "." + DELAY, Duration.ZERO);
        Duration jitter = duration(section.path(JITTER), path + "." + JITTER, Duration.ZERO);
        boolean reorder = bool(section, REORDER, path);
        if ((!jitter.isZero() || reorder) && delay.isZero()) {
            throw new IllegalArgumentException(path + ": " + (reorder ? REORDER : JITTER) + " needs a " + DELAY);
        }
        if (reorder && jitter.isZero()) {
            throw new IllegalArgumentException(path + ": " + REORDER + " needs a " + JITTER
                    + ", which reorders the packets");
        }
        if (jitter.compareTo(delay) > 0) {
            throw new IllegalArgumentException(path + ": the " + JITTER + " can't be larger than the " + DELAY);
        }
        double loss = percent(section.path(LOSS), path + "." + LOSS);
        double lossBurst = 1;
        JsonNode burst = section.path(LOSS_BURST);
        if (!burst.isMissingNode() && !burst.isNull()) {
            if (!burst.isNumber() || burst.doubleValue() < 1 || burst.doubleValue() > 1000) {
                throw new IllegalArgumentException(path + "." + LOSS_BURST + " must be the mean number of packets lost"
                        + " in a row, from 1 to 1000, not " + burst.asText());
            }
            if (loss == 0) {
                throw new IllegalArgumentException(path + ": " + LOSS_BURST + " needs a " + LOSS);
            }
            lossBurst = burst.doubleValue();
            // A burst of b packets on average follows at least one packet that isn't lost, so at most b / (b + 1)
            // of the packets can be lost. A burst of 1 is independent loss, which has no such bound.
            double maxLoss = 100 * lossBurst / (lossBurst + 1);
            if (lossBurst > 1 && loss > maxLoss) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "%s: a %s of %s %% can't come in bursts"
                        + " of %s packets: at most %s %% can", path, LOSS, number(loss), number(lossBurst),
                        number(Math.floor(maxLoss * 1000) / 1000)));
            }
        }
        long rate = rate(section.path(RATE), path + "." + RATE);
        Duration queue = duration(section.path(QUEUE), path + "." + QUEUE, null);
        if (queue != null && rate == 0) {
            throw new IllegalArgumentException(path + ": " + QUEUE + " is the queue of a " + RATE + " limit");
        }
        if (queue != null && queue.isZero()) {
            throw new IllegalArgumentException(path + "." + QUEUE + " must be longer than 0");
        }
        Anchor anchor = Anchor.MEASUREMENT;
        JsonNode anchorSetting = section.path(ANCHOR);
        Duration start = duration(section.path(START), path + "." + START, null);
        if (!anchorSetting.isMissingNode() && !anchorSetting.isNull()) {
            try {
                anchor = Anchor.valueOf(anchorSetting.asText().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(path + "." + ANCHOR + " must be measurement or run, not "
                        + anchorSetting.asText());
            }
            if (start == null) {
                throw new IllegalArgumentException(path + "." + ANCHOR + " is what " + START + " counts from; without"
                        + " a " + START + ", the impairment lasts the whole run");
            }
        }
        Duration duration = duration(section.path(DURATION), path + "." + DURATION, null);
        if (duration != null && duration.isZero()) {
            throw new IllegalArgumentException(path + "." + DURATION + " must be longer than 0");
        }
        if (kind == Kind.RESET && duration != null) {
            throw new IllegalArgumentException(path + ": a " + RESET + " happens once, without a " + DURATION);
        }
        if (kind == Kind.SILENT_DISCONNECT && bothDirections) {
            throw new IllegalArgumentException(path + ": a " + SILENT_DISCONNECT + " needs " + FROM + ", the side that"
                    + " drops the connections, and " + TO + ", the side that doesn't learn it, not " + BETWEEN);
        }
        if ((kind == Kind.RESET || kind == Kind.SILENT_DISCONNECT) && start == null) {
            throw new IllegalArgumentException(path + ": a " + (kind == Kind.RESET ? RESET : SILENT_DISCONNECT)
                    + " needs a " + START);
        }
        if (start != null && anchor == Anchor.MEASUREMENT && measurementSeconds >= 0
                && start.toMillis() >= measurementSeconds * 1000) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s.%s: the measurement lasts about %.0f s,"
                    + " so an impairment that starts %s after its start would never happen", path, START,
                    measurementSeconds, millis(start)));
        }
        return new Impairment(name, from, to, bothDirections, kind, delay, jitter, reorder, loss, lossBurst,
                rate, rate > 0 ? (queue != null ? queue : DEFAULT_QUEUE) : Duration.ZERO, anchor, start, duration);
    }

    /**
     * Rejects two conditions that impair the same pair of containers at the same time in the same place, since a
     * pair's packets can only go through one netem queue there: two path conditions (delay, jitter, loss), on the
     * receiver's side, or two rate limits, on the sender's. A path condition and a rate limit compose. Conditions with
     * different anchors on the same pair are rejected, since their relative times aren't known before the run.
     * Partitions and resets may overlap conditions and each other: a partition takes precedence over a condition.
     */
    private static void checkOverlaps(NetworkSettings settings, Topology topology) {
        Map<Pair, List<Impairment>> byPair = new HashMap<>();
        for (Impairment impairment : settings.impairments()) {
            if (impairment.kind() != Kind.CONDITION) {
                continue;
            }
            for (Pair pair : pairs(impairment, topology)) {
                List<Impairment> others = byPair.computeIfAbsent(pair, key -> new ArrayList<>());
                for (Impairment other : others) {
                    boolean samePlace = other.kind() == Kind.CONDITION && impairment.kind() == Kind.CONDITION
                            && (other.pathImpairment() && impairment.pathImpairment()
                            || other.rateLimited() && impairment.rateLimited());
                    if (samePlace && overlap(other, impairment)) {
                        throw new IllegalArgumentException("network." + IMPAIRMENTS + ": " + other.name() + " and "
                                + impairment.name() + " both impair the packets from " + pair.sender() + " to "
                                + pair.receiver() + (sameAnchor(other, impairment) ? " at the same time"
                                : ", with starts that count from different anchors"));
                    }
                }
                others.add(impairment);
            }
        }
    }

    /** What an impairment's start counts from; a whole-run impairment starts with the run. */
    static Anchor effectiveAnchor(Impairment impairment) {
        return impairment.wholeRun() ? Anchor.RUN : impairment.anchor();
    }

    private static boolean sameAnchor(Impairment a, Impairment b) {
        return effectiveAnchor(a) == effectiveAnchor(b);
    }

    /**
     * Whether two impairments' intervals overlap, as half-open intervals. Impairments whose starts count from
     * different anchors are taken to overlap, since their relative times aren't known before the run.
     */
    static boolean overlap(Impairment a, Impairment b) {
        if (!sameAnchor(a, b)) {
            return true;
        }
        long aStart = a.wholeRun() ? 0 : a.start().toNanos();
        long bStart = b.wholeRun() ? 0 : b.start().toNanos();
        long aEnd = a.duration() == null ? Long.MAX_VALUE : aStart + a.duration().toNanos();
        long bEnd = b.duration() == null ? Long.MAX_VALUE : bStart + b.duration().toNanos();
        return aStart < bEnd && bStart < aEnd;
    }

    /**
     * Checks that no device has more impairments than {@code prio} has bands for: each path impairment and partition
     * takes a band on each of its receivers, and each rate limit a band on each of its senders.
     */
    private static void checkDevices(NetworkSettings settings, Topology topology) {
        Map<String, Integer> receiverBands = new LinkedHashMap<>();
        Map<String, Integer> senderBands = new LinkedHashMap<>();
        for (Impairment impairment : settings.impairments()) {
            Set<String> receivers = new LinkedHashSet<>();
            Set<String> senders = new LinkedHashSet<>();
            for (Pair pair : pairs(impairment, topology)) {
                receivers.add(pair.receiver());
                senders.add(pair.sender());
            }
            if (impairment.kind() == Kind.SILENT_DISCONNECT) {
                // The disconnected connections' packets are dropped both ways
                receivers.addAll(senders);
            }
            if (impairment.pathImpairment()) {
                receivers.forEach(receiver -> receiverBands.merge(receiver, 1, Integer::sum));
            }
            if (impairment.rateLimited()) {
                senders.forEach(sender -> senderBands.merge(sender, 1, Integer::sum));
            }
        }
        for (Map<String, Integer> bands : List.of(receiverBands, senderBands)) {
            bands.forEach((container, count) -> {
                if (count > MAX_IMPAIRMENTS_PER_DEVICE) {
                    throw new IllegalArgumentException("network." + IMPAIRMENTS + ": " + count + " impairments act on "
                            + (bands == receiverBands ? "the packets to " : "the rate of ") + container
                            + ", more than the " + MAX_IMPAIRMENTS_PER_DEVICE + " that a device takes");
                }
            });
        }
    }

    private static List<String> endpoints(JsonNode value, String path, Topology topology) {
        List<String> endpoints = new ArrayList<>();
        if (value.isTextual()) {
            endpoints.add(value.textValue());
        } else if (value.isArray() && !value.isEmpty()) {
            for (JsonNode endpoint : value) {
                if (!endpoint.isTextual()) {
                    throw new IllegalArgumentException(path + " must be an endpoint or a list of endpoints, such as"
                            + " brokers or [broker-0, bookies]");
                }
                endpoints.add(endpoint.textValue());
            }
        } else {
            throw new IllegalArgumentException(path + " must be an endpoint or a list of endpoints, such as brokers"
                    + " or [broker-0, bookies]");
        }
        for (String endpoint : endpoints) {
            try {
                containers(endpoint, topology);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(path + ": " + endpoint + " " + e.getMessage());
            }
        }
        return List.copyOf(endpoints);
    }

    private static boolean bool(JsonNode section, String field, String path) {
        JsonNode value = section.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return false;
        }
        if (!value.isBoolean()) {
            throw new IllegalArgumentException(path + "." + field + " must be true or false");
        }
        return value.booleanValue();
    }

    /** A duration such as {@code 20ms}, {@code 1.5s}, {@code 2m} or {@code 500us}. */
    static Duration duration(JsonNode value, String path, Duration defaultValue) {
        if (value.isMissingNode() || value.isNull()) {
            return defaultValue;
        }
        Matcher matcher = DURATION_VALUE.matcher(value.asText().trim());
        if (!value.isTextual() || !matcher.matches()) {
            throw new IllegalArgumentException(path + " must be a duration with its unit, us, ms, s or m, such as 20ms"
                    + " or 1.5s, not " + value.asText());
        }
        BigDecimal amount = new BigDecimal(matcher.group(1));
        BigDecimal nanosPerUnit = BigDecimal.valueOf(switch (matcher.group(2)) {
            case "us" -> 1_000L;
            case "ms" -> 1_000_000L;
            case "s" -> 1_000_000_000L;
            default -> 60_000_000_000L;
        });
        return Duration.ofNanos(amount.multiply(nanosPerUnit).longValue());
    }

    /** A rate in bits per second, such as {@code 20mbit}; 0 when missing. */
    static long rate(JsonNode value, String path) {
        if (value.isMissingNode() || value.isNull()) {
            return 0;
        }
        Matcher matcher = RATE_VALUE.matcher(value.asText().trim().toLowerCase(Locale.ROOT));
        if (!value.isTextual() || !matcher.matches()) {
            throw new IllegalArgumentException(path + " must be a rate in bits per second with its unit, bit, kbit,"
                    + " mbit or gbit, such as 20mbit, not " + value.asText());
        }
        long bits = new BigDecimal(matcher.group(1)).multiply(BigDecimal.valueOf(switch (matcher.group(2)) {
            case "bit" -> 1L;
            case "kbit" -> 1_000L;
            case "mbit" -> 1_000_000L;
            default -> 1_000_000_000L;
        })).longValue();
        if (bits < 8_000) {
            throw new IllegalArgumentException(path + " must be at least 8kbit, not " + value.asText());
        }
        return bits;
    }

    /** A percentage such as {@code 0.5%} or {@code 0.5}; 0 when missing. */
    static double percent(JsonNode value, String path) {
        if (value.isMissingNode() || value.isNull()) {
            return 0;
        }
        String text = value.asText().trim();
        Matcher matcher = PERCENT_VALUE.matcher(text);
        if (!(value.isTextual() || value.isNumber()) || !matcher.matches()) {
            throw new IllegalArgumentException(path + " must be a percentage, such as 0.5%, not " + text);
        }
        double percent = Double.parseDouble(matcher.group(1));
        if (percent > 100) {
            throw new IllegalArgumentException(path + " must be a percentage from 0 to 100, not " + text);
        }
        return percent;
    }

    /** A duration in milliseconds for people, such as "20 ms" or "0.5 ms". */
    static String millis(Duration duration) {
        return number(duration.toNanos() / 1e6) + " ms";
    }

    /** A rate for people, such as "20 Mbit/s". */
    static String rate(long bitsPerSecond) {
        if (bitsPerSecond >= 1_000_000_000L) {
            return number(bitsPerSecond / 1e9) + " Gbit/s";
        }
        if (bitsPerSecond >= 1_000_000L) {
            return number(bitsPerSecond / 1e6) + " Mbit/s";
        }
        return number(bitsPerSecond / 1e3) + " kbit/s";
    }

    private static String number(double value) {
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }
}

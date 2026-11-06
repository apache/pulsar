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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * The Linux traffic control, {@code ss} and {@code ethtool} commands that apply the network impairments, as shell
 * command lines.
 *
 * <p>A device that impairments act on gets a {@code prio} root qdisc with a band for each impairment after the three
 * bands of the unimpaired traffic, which its {@link #PRIOMAP} keeps in them, as {@code pfifo_fast} would. Each
 * impairment's band has its netem qdisc from the start; a {@code u32} filter with the impairment's own preference sends
 * the impairment's packets into it, so that adding and deleting the filters applies and removes the impairment
 * without touching the others.
 */
final class TcCommands {
    /** The bands of the unimpaired traffic: {@code prio}'s three, for the packets' priorities. */
    static final int UNIMPAIRED_BANDS = 3;
    /** {@code pfifo_fast}'s mapping of the priorities to the first three bands. */
    static final String PRIOMAP = "1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1";
    /** The filter preferences of partitions, below those of conditions, so that a partition takes precedence. */
    static final int PARTITION_PREFERENCE = 10;
    static final int CONDITION_PREFERENCE = 1000;
    /**
     * A rate that never limits, which makes netem send each packet no earlier than the previous one: the jitter then
     * keeps the packets' order.
     */
    static final String ORDER_PRESERVING_RATE = "100gbit";
    /**
     * The packets that a path queue holds at most. A path queue only delays packets, it doesn't limit their rate, so
     * it holds the packets in flight on the path, which the traffic decides. netem's default of 1,000 packets drops
     * the packets of a fast TCP connection at a delay of a few milliseconds, a loss that nobody asked for. netem
     * allocates nothing for its limit, so the limit is a cap that the host's traffic can't reach: 10 million packets
     * per second for 100 ms.
     */
    static final int PATH_LIMIT = 1_000_000;
    /** A full Ethernet frame, which the bottleneck queue's size counts in. */
    static final int FRAME_BYTES = 1_514;
    static final int MIN_RATE_LIMIT = 64;
    private static final Pattern DEVICE = Pattern.compile("[A-Za-z0-9._-]{1,15}");
    private static final Pattern IPV4 = Pattern.compile("[0-9]{1,3}(\\.[0-9]{1,3}){3}");

    private TcCommands() {
    }

    /** The band of an impairment, from 1, after the unimpaired bands. */
    static int band(int slot) {
        return UNIMPAIRED_BANDS + 1 + slot;
    }

    /** A band's class ID; class minors are hexadecimal, so that band 10 is {@code 1:a}. */
    static String classId(int band) {
        return "1:" + Integer.toHexString(band);
    }

    /** The handle of a band's netem qdisc, such as {@code 40:} for band 4 and {@code a0:} for band 10. */
    static String netemHandle(int band) {
        return Integer.toHexString(band) + "0:";
    }

    /** The {@code prio} root qdisc with a band for each of {@code slots} impairments. */
    static String root(String device, int slots) {
        if (slots < 1 || slots > NetworkSettings.MAX_IMPAIRMENTS_PER_DEVICE) {
            throw new IllegalArgumentException("A device takes 1 to " + NetworkSettings.MAX_IMPAIRMENTS_PER_DEVICE
                    + " impairments, not " + slots);
        }
        return "tc qdisc add dev " + device(device) + " root handle 1: prio bands " + (UNIMPAIRED_BANDS + slots)
                + " priomap " + PRIOMAP;
    }

    /** The netem qdisc of an impairment's band. */
    static String netem(String device, int slot, String netemArguments) {
        int band = band(slot);
        return "tc qdisc add dev " + device(device) + " parent " + classId(band) + " handle " + netemHandle(band)
                + " netem " + netemArguments;
    }

    /**
     * The netem arguments of an impairment on the path to the receivers: its delay, jitter and loss, or the loss of
     * every packet of a partition. The jitter keeps the packets' order unless the impairment reorders.
     */
    static String pathNetem(NetworkSettings.Impairment impairment) {
        if (impairment.kind() == NetworkSettings.Kind.PARTITION
                || impairment.kind() == NetworkSettings.Kind.SILENT_DISCONNECT) {
            return "limit " + PATH_LIMIT + " loss 100%";
        }
        List<String> arguments = new ArrayList<>();
        arguments.add("limit " + PATH_LIMIT);
        if (!impairment.delay().isZero()) {
            arguments.add("delay " + micros(impairment.delay())
                    + (impairment.jitter().isZero() ? "" : " " + micros(impairment.jitter())));
        }
        if (impairment.lossPercent() > 0) {
            arguments.add(loss(impairment.lossPercent(), impairment.lossBurst()));
        }
        if (!impairment.jitter().isZero() && !impairment.reorder()) {
            arguments.add("rate " + ORDER_PRESERVING_RATE);
        }
        return String.join(" ", arguments);
    }

    /**
     * netem's loss of {@code percent} of the packets in bursts of {@code burst} packets on average. Independent losses
     * are netem's random loss. Bursts are the Gilbert-Elliott model: a good state that loses nothing and a bad state
     * that loses every packet, left with the probability 1 / burst, and entered with the probability that gives the
     * mean loss. netem's correlated random loss ({@code loss random P C}) isn't used: on Linux 7.1, 1 % with a 25 %
     * correlation dropped 1 of 179,193 packets in a run and none of 3,000 pings, since the correlation changes the
     * probabilities in ways that are hard to predict.
     */
    static String loss(double percent, double burst) {
        if (burst <= 1) {
            return "loss random " + percent(percent);
        }
        double leave = 1 / burst;
        double fraction = percent / 100;
        double enter = fraction * leave / (1 - fraction);
        return "loss gemodel " + percent(enter * 100) + " " + percent(leave * 100) + " 100% 0%";
    }

    /** The netem arguments of an impairment's rate limit on its senders, with its bottleneck queue. */
    static String rateNetem(NetworkSettings.Impairment impairment) {
        return "limit " + rateLimit(impairment.rateBitsPerSecond(), impairment.queue()) + " rate "
                + impairment.rateBitsPerSecond() + "bit";
    }

    /**
     * The packets of a bottleneck queue: {@code queue} at the rate, in full frames, at least {@link #MIN_RATE_LIMIT}.
     */
    static long rateLimit(long bitsPerSecond, Duration queue) {
        double bytes = bitsPerSecond / 8.0 * queue.toNanos() / 1e9;
        return Math.max(MIN_RATE_LIMIT, (long) Math.ceil(bytes / FRAME_BYTES));
    }

    /** The filter that sends the packets from {@code source} into an impairment's band. */
    static String sourceFilter(String device, int preference, int slot, String source) {
        return filter(device, preference, slot, "src", source);
    }

    /** The filter that sends the packets to {@code destination} into an impairment's band. */
    static String destinationFilter(String device, int preference, int slot, String destination) {
        return filter(device, preference, slot, "dst", destination);
    }

    private static String filter(String device, int preference, int slot, String direction, String address) {
        return "tc filter add dev " + device(device) + " parent 1: protocol ip pref " + preference + " u32 match ip "
                + direction + " " + ipv4(address) + "/32 flowid " + classId(band(slot));
    }

    /**
     * The filter that sends one TCP connection's packets from {@code source} to {@code destination} into an
     * impairment's band, by the connection's addresses and ports.
     */
    static String flowFilter(String device, int preference, int slot, Endpoint source, Endpoint destination) {
        return "tc filter add dev " + device(device) + " parent 1: protocol ip pref " + preference + " u32"
                + " match ip src " + ipv4(source.address()) + "/32 match ip dst " + ipv4(destination.address())
                + "/32 match ip sport " + port(source.port()) + " 0xffff match ip dport " + port(destination.port())
                + " 0xffff flowid " + classId(band(slot));
    }

    /** One end of a TCP connection. */
    record Endpoint(String address, int port) {
    }

    /** One TCP connection, as {@code ss} shows it in the namespace of its local end. */
    record Connection(Endpoint local, Endpoint peer) {
    }

    /** Lists the established TCP connections to {@code destination}, one per line: local and peer address:port. */
    static String connections(String destination) {
        return "ss -t -n -H state established dst " + ipv4(destination);
    }

    /**
     * The connections in {@code ss -t -n -H state established}'s output: the two fields of each line that are
     * {@code address:port}, the local end first. A dual-stack socket, such as a Java server's, shows its IPv4 address
     * mapped into IPv6, as {@code [::ffff:a.b.c.d]:port}; its packets are IPv4, and ss takes its IPv4 address.
     */
    static List<Connection> parseConnections(String output) {
        List<Connection> connections = new ArrayList<>();
        for (String line : output.lines().toList()) {
            List<Endpoint> ends = new ArrayList<>();
            for (String field : line.strip().split("\\s+")) {
                int colon = field.lastIndexOf(':');
                if (colon <= 0) {
                    continue;
                }
                String address = field.substring(0, colon);
                if (address.startsWith("[::ffff:") && address.endsWith("]")) {
                    address = address.substring("[::ffff:".length(), address.length() - 1);
                }
                if (IPV4.matcher(address).matches()) {
                    ends.add(new Endpoint(address, Integer.parseInt(field.substring(colon + 1))));
                }
            }
            if (ends.size() == 2) {
                connections.add(new Connection(ends.get(0), ends.get(1)));
            }
        }
        return connections;
    }

    /** Destroys one TCP connection, by its local and peer ends; prints it when it was destroyed. */
    static String destroy(Connection connection) {
        return "ss -K -t -n -H src " + ipv4(connection.local().address()) + ":" + port(connection.local().port())
                + " dst " + ipv4(connection.peer().address()) + ":" + port(connection.peer().port());
    }

    private static int port(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Not a TCP port: " + port);
        }
        return port;
    }

    /** Deletes an impairment's filters, which removes the impairment; its queued packets are still sent. */
    static String deleteFilters(String device, int preference) {
        return "tc filter del dev " + device(device) + " parent 1: protocol ip pref " + preference + " u32";
    }

    /** Deletes the root qdisc and with it the bands, back to the device's default qdisc. */
    static String deleteRoot(String device) {
        return "tc qdisc del dev " + device(device) + " root";
    }

    /** Shows the root qdisc, to check that it is the default before replacing it. */
    static String showRoot(String device) {
        return "tc qdisc show dev " + device(device) + " root";
    }

    /** The qdiscs' counters, as JSON. */
    static String statistics(String device) {
        return "tc -s -j qdisc show dev " + device(device);
    }

    /**
     * Destroys the established TCP connections to {@code destination}, which resets them on both ends; prints each
     * destroyed socket on a line.
     */
    static String reset(String destination) {
        return "ss -K -t -n -H state established dst " + ipv4(destination);
    }

    /** Sets the TCP and the generic segmentation offloads. */
    static String setOffloads(String device, boolean tso, boolean gso) {
        return "ethtool -K " + device(device) + " tso " + (tso ? "on" : "off") + " gso " + (gso ? "on" : "off");
    }

    /** Shows the offloads. */
    static String showOffloads(String device) {
        return "ethtool -k " + device(device);
    }

    /**
     * Whether a root qdisc, as {@code tc qdisc show dev <device> root} prints it, is a default one that the
     * impairments may replace: {@code noqueue} on a veth, or the kernel's default qdiscs.
     */
    static boolean defaultRoot(String shown) {
        String qdisc = shown.strip().split("\\s+", 3)[shown.isBlank() ? 0 : 1];
        return shown.isBlank() || List.of("noqueue", "pfifo_fast", "fq_codel", "fq", "mq").contains(qdisc);
    }

    static String micros(Duration duration) {
        return duration.toNanos() / 1_000 + "us";
    }

    static String percent(double percent) {
        return BigDecimal.valueOf(percent).setScale(6, RoundingMode.HALF_UP).stripTrailingZeros()
                .toPlainString() + "%";
    }

    /** A device name as the kernel allows it, so that it is safe in a command line. */
    static String device(String device) {
        if (!DEVICE.matcher(device).matches()) {
            throw new IllegalArgumentException("Not a network device's name: " + device);
        }
        return device;
    }

    static String ipv4(String address) {
        if (!IPV4.matcher(address).matches()) {
            throw new IllegalArgumentException("Not an IPv4 address: " + address + "; the network impairments"
                    + " support IPv4 only");
        }
        return address;
    }

    /** The {@code ip -o link} line's peer index of a veth in a container, such as 23076 of {@code eth0@if23076}. */
    static int peerIndex(String linkLine) {
        String[] fields = linkLine.split(":\\s+", 3);
        if (fields.length < 2) {
            throw new IllegalArgumentException("Not an ip -o link line: " + linkLine);
        }
        int at = fields[1].indexOf("@if");
        if (at < 0) {
            throw new IllegalArgumentException("The interface " + fields[1] + " has no peer: " + linkLine);
        }
        return Integer.parseInt(fields[1].substring(at + 3));
    }

    /** The {@code ip -o link} line's own index, such as 2 of {@code 2: eth0@if23076}. */
    static int linkIndex(String linkLine) {
        String[] fields = linkLine.split(":\\s+", 3);
        if (fields.length < 2) {
            throw new IllegalArgumentException("Not an ip -o link line: " + linkLine);
        }
        return Integer.parseInt(fields[0].strip());
    }

    /**
     * The name of the interface with {@code index} in {@code ip -o link}'s output, without its {@code @} suffix: the
     * host's end of a container's veth, which must name the container's end, {@code peerOf}, as its peer, so that the
     * impairments never land on another interface that happens to have the index.
     */
    static String interfaceWithIndex(String links, int index, int peerOf) {
        for (String line : links.lines().toList()) {
            String[] fields = line.split(":\\s+", 3);
            if (fields.length >= 2 && fields[0].strip().equals(Integer.toString(index))) {
                int at = fields[1].indexOf("@if");
                if (at < 0 || !fields[1].substring(at + 3).equals(Integer.toString(peerOf))) {
                    throw new IllegalArgumentException("The interface with index " + index + ", " + fields[1]
                            + ", isn't the peer of the container's interface " + peerOf);
                }
                return fields[1].substring(0, at);
            }
        }
        throw new IllegalArgumentException("No interface with index " + index);
    }

    /** The name of the interface with {@code address} in {@code ip -o -4 addr show}'s output. */
    static String interfaceWithAddress(String addresses, String address) {
        for (String line : addresses.lines().toList()) {
            String[] fields = line.strip().split("\\s+");
            if (fields.length >= 4 && fields[2].equals("inet") && fields[3].startsWith(address + "/")) {
                return fields[1];
            }
        }
        throw new IllegalArgumentException("No interface has the address " + address);
    }

    /** Whether {@code ethtool -k}'s output shows a feature on, such as {@code tcp-segmentation-offload}. */
    static boolean featureOn(String features, String feature) {
        for (String line : features.lines().toList()) {
            String stripped = line.strip();
            if (stripped.startsWith(feature + ":")) {
                return stripped.substring(feature.length() + 1).strip().toLowerCase(Locale.ROOT).startsWith("on");
            }
        }
        throw new IllegalArgumentException("ethtool doesn't show " + feature);
    }
}

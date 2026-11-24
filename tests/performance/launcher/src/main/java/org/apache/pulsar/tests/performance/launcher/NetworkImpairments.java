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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ContainerNetwork;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Anchor;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Impairment;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Kind;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Pair;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;

/**
 * Applies a scenario's network impairments to the run's containers with Linux traffic control, from a privileged
 * sidecar container in the Docker engine host's network and PID namespaces. See {@code docs/network-conditions.md}.
 *
 * <ul>
 *   <li>A delay, a jitter, a loss or a partition acts on the path to the receiver: a netem qdisc on the receiver's
 *   veth in the engine's host, which sends the packets into the receiver's namespace, filtered by the senders' IPs.
 *   The packets have left the sender, so its socket accounting and its own qdisc don't see them.</li>
 *   <li>A rate limit is the sender's uplink: a netem qdisc on the sender's interface in its namespace, filtered by the
 *   receivers' IPs, with a bounded queue.</li>
 *   <li>A reset destroys the senders' TCP connections to the receivers with {@code ss -K}, which resets them on both
 *   ends.</li>
 * </ul>
 *
 * <p>Every device's qdiscs are installed when the run starts, before the gateways create their clients; applying an
 * impairment adds its filters and removing it deletes them. The whole-run impairments are applied then too, the timed
 * ones on a timeline from their anchor, the gateways' start or the measurement's. Each event is appended to
 * {@value #EVENTS_FILE} as it happens, and {@link #close()} removes everything that was installed, restores the
 * offloads, and writes {@value RunReport#NETWORK_SUMMARY_FILE} for the run report.
 */
final class NetworkImpairments implements AutoCloseable {
    static final String EVENTS_FILE = RunReport.NETWORK_EVENTS_FILE;
    private static final String IMAGE = "pulsar-performance-network:alpine-3.20";
    /**
     * How often the impaired containers' TCP counters are read: the workloads' containers exit when they finish, so the
     * counters of the run are their last reading.
     */
    private static final long TCP_SAMPLE_SECONDS = 5;
    /** The longest script passed as an argument; a longer one goes into a file in the sidecar. */
    private static final int MAX_INLINE_SCRIPT = 64 * 1024;
    /** The longest a command may take in the sidecar. */
    private static final int COMMAND_TIMEOUT_SECONDS = 30;
    /**
     * Checks in a throwaway network namespace that the kernel and the tools do what the impairments need: a prio
     * qdisc with a netem band, a u32 filter added and deleted, and ss -K destroying a TCP connection.
     */
    static final String PROBE_SCRIPT = "set -e\n"
            + "ip link set lo up\n"
            + "tc qdisc add dev lo root handle 1: prio bands 4 priomap " + TcCommands.PRIOMAP + "\n"
            + "tc qdisc add dev lo parent 1:4 handle 40: netem limit 1000 delay 1ms 1ms loss random 1% rate "
            + TcCommands.ORDER_PRESERVING_RATE + "\n"
            + "tc filter add dev lo parent 1: protocol ip pref 10 u32 match ip dst 127.0.0.2/32 flowid 1:4\n"
            + "tc filter del dev lo parent 1: protocol ip pref 10 u32\n"
            + "tc qdisc del dev lo root\n"
            + "ethtool --version > /dev/null\n"
            + "nc -l -p 7000 > /dev/null &\n"
            + "sleep 0.3\n"
            + "sleep 5 | nc 127.0.0.1 7000 &\n"
            + "sleep 0.3\n"
            + "destroyed=$(ss -K -t -n -H dst 127.0.0.1 | wc -l)\n"
            + "if [ \"$destroyed\" -lt 1 ]; then echo 'ss -K destroyed no TCP socket' >&2; exit 1; fi\n";

    /** A container whose network the impairments act on, as the launcher resolved it. */
    record Target(String name, GenericContainer<?> container, long pid, String ip, String containerInterface,
                  String hostInterface) {
    }

    /**
     * Where the impairments' commands run: the Docker engine's host, through the sidecar, or a fake in the tests.
     */
    interface Host {
        /**
         * Runs a shell script in the engine host's network namespace, when {@code target} is null, or in the
         * target's, and returns its output; fails when the script fails.
         */
        String exec(Target target, String script) throws IOException;

        /** Whether a target's container still runs the process whose namespace was resolved. */
        boolean running(Target target);

        /**
         * Resolves a container: its IP on the cluster's network, its interface with that IP, and that interface's
         * peer in the engine's host.
         */
        Target resolve(String name, GenericContainer<?> container, String networkId) throws IOException;

        /** Releases the host, such as the sidecar. */
        void close();
    }

    // The workload containers exit when they have finished, before the impairments' counters are read at the end
    private static boolean isWorkload(Target target) {
        return NetworkSettings.GATEWAYS.equals(target.name()) || NetworkSettings.APPLICATIONS.equals(target.name())
                || NetworkSettings.REMOTE_APPLICATIONS.equals(target.name());
    }

    /** A device with impairment bands: a receiver's host-side veth, or a sender's own interface. */
    private static final class Device {
        final Target owner;
        // Whether the device is the receiver's veth in the engine's host, rather than the sender's own interface
        final boolean hostSide;
        final List<Activation> slots = new ArrayList<>();
        boolean installed;

        Device(Target owner, boolean hostSide) {
            this.owner = owner;
            this.hostSide = hostSide;
        }

        String name() {
            return hostSide ? owner.hostInterface() : owner.containerInterface();
        }

        String description() {
            return (hostSide ? "the packets to " : "the packets from ") + owner.name() + " (" + name() + ")";
        }
    }

    /** An impairment's band on a device, with the addresses whose packets its filters send into it. */
    private record Placement(Device device, int slot, List<String> addresses) {
    }

    private enum Status { PENDING, ACTIVE, REMOVED, MISSED, SKIPPED, FAILED }

    /** An impairment and what became of it. */
    private static final class Activation {
        final Impairment impairment;
        final int preference;
        final List<Placement> placements = new ArrayList<>();
        // A reset's senders and the receivers' addresses
        final Map<Target, List<String>> resets = new LinkedHashMap<>();
        Status status = Status.PENDING;
        // The containers that an applied impairment couldn't act on, since they had exited
        List<String> skipped = List.of();
        Instant applied;
        Instant removed;
        int resetSockets;
        // The devices that a silent disconnection added filters to; null for the other kinds, whose filters are on
        // every placement's device
        Set<String> filteredDevices;
        String note;

        Activation(Impairment impairment, int preference) {
            this.impairment = impairment;
            this.preference = preference;
        }
    }

    private final Host host;
    private final NetworkSettings settings;
    private final NetworkSettings.Topology topology;
    private final ObjectMapper mapper;
    private final Path runDirectory;
    private final Consumer<String> status;
    private final BufferedWriter events;
    private final Map<String, Target> targets = new LinkedHashMap<>();
    private final Map<String, Device> devices = new LinkedHashMap<>();
    private final List<Activation> activations = new ArrayList<>();
    // The containers whose offloads were turned off, with whether TSO and GSO were on
    private final Map<Target, boolean[]> offloads = new LinkedHashMap<>();
    private final Map<String, long[]> tcpAtStart = new LinkedHashMap<>();
    // Each placement's last counters, for a device whose container has exited; the sampler thread updates them too
    private final Map<Placement, ObjectNode> lastStatistics = new ConcurrentHashMap<>();
    private final Map<String, long[]> tcpAtEnd = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timeline = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "network-impairments");
        thread.setDaemon(true);
        return thread;
    });
    private final ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "network-tcp-counters");
        thread.setDaemon(true);
        return thread;
    });
    private final CompletableFuture<Void> failure = new CompletableFuture<>();
    private Instant measurementStart;
    // Set before the timeline is stopped, so that an interrupted command isn't taken for a failed impairment
    private volatile boolean closing;
    private boolean closed;

    NetworkImpairments(Host host, NetworkSettings settings, NetworkSettings.Topology topology, Path runDirectory,
                       Consumer<String> status) throws IOException {
        this.host = host;
        this.settings = settings;
        this.topology = topology;
        // JSON, also for the events' lines and tc's JSON output, whatever the launcher reads the scenario with
        this.mapper = new ObjectMapper();
        this.runDirectory = runDirectory;
        this.status = status;
        this.events = Files.newBufferedWriter(runDirectory.resolve(EVENTS_FILE));
    }

    /**
     * Starts the sidecar and checks that it can apply the impairments; fails, with the sidecar stopped, when it can't.
     */
    @SuppressWarnings("resource")
    static NetworkImpairments start(NetworkSettings settings, NetworkSettings.Topology topology, Path runDirectory,
                                    Consumer<String> status) throws IOException {
        GenericContainer<?> sidecar = new GenericContainer<>(new ImageFromDockerfile(IMAGE, false)
                .withDockerfileFromBuilder(builder -> builder.from("alpine:3.20")
                        .run("apk add --no-cache iproute2 ethtool util-linux").build()))
                .withPrivilegedMode(true)
                .withNetworkMode("host")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPidMode("host"))
                .withCommand("sleep", "infinity")
                .waitingFor(Wait.forSuccessfulCommand("true").withStartupTimeout(Duration.ofSeconds(120)));
        sidecar.start();
        try {
            Container.ExecResult probe = sidecar.execInContainer("timeout", "20", "unshare", "-n", "sh", "-c",
                    PROBE_SCRIPT);
            if (probe.getExitCode() != 0) {
                throw new IllegalStateException("The Docker engine's host can't apply network impairments: "
                        + (probe.getStderr() + probe.getStdout()).strip());
            }
            return new NetworkImpairments(new DockerHost(sidecar), settings, topology, runDirectory, status);
        } catch (InterruptedException e) {
            sidecar.stop();
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (IOException | RuntimeException e) {
            sidecar.stop();
            throw e;
        }
    }

    /** Completes exceptionally when applying or removing an impairment failed, which fails the run. */
    CompletableFuture<Void> failure() {
        return failure;
    }

    /** Fails the run with {@code cause}, as a failed impairment does. */
    void fail(Throwable cause) {
        failure.completeExceptionally(cause);
    }

    /**
     * Resolves the containers, installs every device's qdiscs, turns off the offloads, and applies the whole-run
     * impairments. Called once every container has started, the gateways waiting before they create their clients.
     *
     * @param containers the run's containers by the endpoints' names: gateways, applications, broker-N, bookie-N,
     *     zookeeper and configuration-store
     * @param networkId the ID of the cluster's Docker network
     */
    synchronized void setUp(Map<String, GenericContainer<?>> containers, String networkId) throws IOException {
        Map<String, Target> resolved = new LinkedHashMap<>();
        for (String name : used(containers.keySet())) {
            GenericContainer<?> container = containers.get(name);
            if (container == null) {
                throw new IllegalStateException("The run has no container " + name + " for the network impairments");
            }
            resolved.put(name, host.resolve(name, container, networkId));
        }
        setUpTargets(resolved);
    }

    /** The containers that the impairments act on, of {@code containers}: all of them with the offloads off. */
    private Set<String> used(Set<String> containers) {
        Set<String> used = new LinkedHashSet<>();
        for (Impairment impairment : settings.impairments()) {
            for (Pair pair : NetworkSettings.pairs(impairment, topology)) {
                used.add(pair.sender());
                used.add(pair.receiver());
            }
        }
        if (settings.offloads() == NetworkSettings.Offloads.OFF) {
            used.addAll(containers);
        }
        return used;
    }

    /** {@link #setUp(Map, String)} with the containers resolved: every container that the impairments act on. */
    synchronized void setUpTargets(Map<String, Target> containers) throws IOException {
        for (String name : used(containers.keySet())) {
            Target target = containers.get(name);
            if (target == null) {
                throw new IllegalStateException("The run has no container " + name + " for the network impairments");
            }
            targets.put(name, target);
        }
        plan();
        for (Device device : devices.values()) {
            String root = exec(device.hostSide ? null : device.owner, TcCommands.showRoot(device.name()));
            if (!TcCommands.defaultRoot(root)) {
                throw new IllegalStateException("The network device of " + device.description() + " already has the"
                        + " qdisc " + root.strip() + ", which the network impairments would replace");
            }
        }
        setOffloads();
        for (Device device : devices.values()) {
            exec(device.hostSide ? null : device.owner, TcCommands.root(device.name(), device.slots.size()));
            // Installed from now on, so that the cleanup deletes the root also when a band fails
            device.installed = true;
            List<String> commands = new ArrayList<>();
            for (int slot = 0; slot < device.slots.size(); slot++) {
                Impairment impairment = device.slots.get(slot).impairment;
                commands.add(TcCommands.netem(device.name(), slot, device.hostSide
                        ? TcCommands.pathNetem(impairment) : TcCommands.rateNetem(impairment)));
            }
            exec(device.hostSide ? null : device.owner, String.join("\n", commands));
        }
        for (Target target : targets.values()) {
            tcpAtStart.put(target.name(), tcpCounters(target));
        }
        record("set-up", null, node -> {
            ArrayNode resolved = node.putArray("containers");
            targets.values().forEach(target -> resolved.addObject().put("name", target.name())
                    .put("ip", target.ip()).put("pid", target.pid())
                    .put("interface", target.containerInterface()).put("hostInterface", target.hostInterface()));
            ArrayNode installed = node.putArray("devices");
            devices.values().forEach(device -> {
                ObjectNode entry = installed.addObject().put("device", device.name())
                        .put("container", device.owner.name()).put("side", device.hostSide ? "receiver" : "sender");
                ArrayNode slots = entry.putArray("impairments");
                device.slots.forEach(activation -> slots.add(activation.impairment.name()));
            });
        });
        for (Activation activation : activations) {
            if (activation.impairment.wholeRun()) {
                apply(activation);
                if (activation.status == Status.FAILED) {
                    throw new IllegalStateException("Applying the network impairment " + activation.impairment.name()
                            + " failed: " + activation.note);
                }
            }
        }
    }

    /**
     * Starts the impairments that count from the gateways' start: the run's start, when the gateways create their
     * clients. Also schedules the end of the whole-run impairments with a duration.
     */
    synchronized void startRun() {
        sampler.scheduleWithFixedDelay(this::sampleTcp, TCP_SAMPLE_SECONDS, TCP_SAMPLE_SECONDS, TimeUnit.SECONDS);
        long anchor = System.nanoTime();
        for (Activation activation : activations) {
            Impairment impairment = activation.impairment;
            if (impairment.wholeRun()) {
                if (impairment.duration() != null) {
                    schedule(anchor, impairment.duration(), () -> remove(activation));
                }
            } else if (impairment.anchor() == Anchor.RUN) {
                scheduleTimed(anchor, activation);
            }
        }
    }

    /**
     * Applies the impairments that start with the measurement; the launcher calls it before it lets the producers
     * start the measurement, and then {@link #startMeasurement()}. Fails when an impairment fails, and the
     * measurement mustn't start then.
     */
    synchronized void prepareMeasurement() {
        for (Activation activation : activations) {
            Impairment impairment = activation.impairment;
            if (!impairment.wholeRun() && impairment.anchor() == Anchor.MEASUREMENT && impairment.start().isZero()) {
                apply(activation);
            }
        }
        if (activations.stream().anyMatch(activation -> activation.status == Status.FAILED)) {
            throw new IllegalStateException("Applying the network impairments of the measurement's start failed");
        }
    }

    /**
     * Starts the timeline of the impairments that count from the measurement's start, which is now: the launcher
     * calls it once the producers have been told to start the measurement.
     */
    synchronized void startMeasurement() {
        if (closing || closed) {
            // A measurement gate that outlived the run's shutdown: the timeline has stopped
            return;
        }
        measurementStart = Instant.now();
        long anchor = System.nanoTime();
        try {
            for (Activation activation : activations) {
                Impairment impairment = activation.impairment;
                if (!impairment.wholeRun() && impairment.anchor() == Anchor.MEASUREMENT) {
                    if (impairment.start().isZero()) {
                        if (impairment.duration() != null) {
                            schedule(anchor, impairment.duration(), () -> remove(activation));
                        }
                    } else {
                        scheduleTimed(anchor, activation);
                    }
                }
            }
        } catch (RejectedExecutionException e) {
            // The run is shutting down, and stopped the timeline meanwhile
        }
    }

    /**
     * Reads the workload containers' TCP counters and the queue counters of their devices, off the timeline's thread
     * and lock, so that a workload container that has exited when it has finished keeps its last reading. The
     * cluster's containers run until the end, where the counters are read once. An observation, so a failed reading
     * is left for the next one, or the one at the end.
     */
    private void sampleTcp() {
        for (Target target : targets.values()) {
            try {
                if (!closing && isWorkload(target) && running(target)) {
                    tcpAtEnd.put(target.name(), tcpCounters(target));
                }
            } catch (IOException | RuntimeException e) {
                // The next reading counts
            }
        }
        for (Activation activation : activations) {
            try {
                if (!closing) {
                    statistics(activation, true);
                }
            } catch (IOException | RuntimeException e) {
                // The next reading counts
            }
        }
    }

    private void scheduleTimed(long anchor, Activation activation) {
        Impairment impairment = activation.impairment;
        schedule(anchor, impairment.start(), () -> apply(activation));
        if (impairment.duration() != null) {
            schedule(anchor, impairment.start().plus(impairment.duration()), () -> remove(activation));
        }
    }

    // At monotonic deadlines; a removal and an application at the same time run in the order they were scheduled
    private void schedule(long anchorNanos, Duration after, Runnable action) {
        long delay = anchorNanos + after.toNanos() - System.nanoTime();
        timeline.schedule(() -> {
            try {
                action.run();
            } catch (RuntimeException e) {
                failure.completeExceptionally(e);
            }
        }, Math.max(0, delay), TimeUnit.NANOSECONDS);
    }

    /** Applies an impairment: adds its filters, or resets its connections. */
    private synchronized void apply(Activation activation) {
        if (closed || activation.status != Status.PENDING) {
            return;
        }
        Impairment impairment = activation.impairment;
        try {
            if (impairment.kind() == Kind.RESET) {
                int destroyed = 0;
                List<String> sockets = new ArrayList<>();
                for (Map.Entry<Target, List<String>> reset : activation.resets.entrySet()) {
                    if (!running(reset.getKey())) {
                        continue;
                    }
                    for (String address : reset.getValue()) {
                        String output = exec(reset.getKey(), TcCommands.reset(address));
                        for (String line : output.lines().filter(line -> !line.isBlank()).toList()) {
                            sockets.add(reset.getKey().name() + ": " + line.strip());
                            destroyed++;
                        }
                    }
                }
                activation.resetSockets = destroyed;
                activation.applied = Instant.now();
                activation.status = Status.REMOVED;
                if (destroyed == 0) {
                    activation.note = "matched no TCP connection";
                }
                status.accept(String.format(Locale.ROOT, "Network: %s: reset %d TCP connection(s) (%s)",
                        impairment.name(), destroyed, impairment.endpoints()));
                record("reset", activation, node -> {
                    node.put("destroyed", activation.resetSockets);
                    ArrayNode list = node.putArray("sockets");
                    sockets.forEach(list::add);
                });
                return;
            }
            if (impairment.kind() == Kind.SILENT_DISCONNECT) {
                disconnectSilently(activation);
                return;
            }
            List<String> skipped = new ArrayList<>();
            for (Placement placement : activation.placements) {
                if (!running(placement.device().owner)) {
                    skipped.add(placement.device().owner.name());
                    continue;
                }
                List<String> commands = new ArrayList<>();
                for (String address : placement.addresses()) {
                    commands.add(placement.device().hostSide
                            ? TcCommands.sourceFilter(placement.device().name(), activation.preference,
                            placement.slot(), address)
                            : TcCommands.destinationFilter(placement.device().name(), activation.preference,
                            placement.slot(), address));
                }
                try {
                    exec(placement.device().hostSide ? null : placement.device().owner,
                            String.join("\n", commands));
                } catch (IOException | RuntimeException e) {
                    // A workload container that exits as it finishes takes its interfaces with it
                    if (running(placement.device().owner)) {
                        throw e;
                    }
                    skipped.add(placement.device().owner.name());
                }
            }
            activation.applied = Instant.now();
            if (!skipped.isEmpty() && skipped.size() == activation.placements.size()) {
                activation.status = Status.SKIPPED;
                activation.note = "its containers had exited: " + String.join(", ", skipped);
                status.accept("Network: " + impairment.name() + " not applied: " + activation.note);
                record("skipped", activation, node -> { });
                return;
            }
            activation.status = Status.ACTIVE;
            if (!skipped.isEmpty()) {
                activation.skipped = List.copyOf(skipped);
                activation.note = "not applied to the exited " + String.join(", ", skipped);
            }
            status.accept("Network: applied " + impairment.name() + " (" + impairment.description() + ", "
                    + impairment.endpoints() + ")");
            ObjectNode statistics = statistics(activation);
            record("applied", activation, node -> node.set("statistics", statistics));
        } catch (IOException | RuntimeException e) {
            if (closing) {
                return;
            }
            activation.status = Status.FAILED;
            activation.note = e.getMessage();
            record("failed", activation, node -> node.put("error", String.valueOf(e.getMessage())));
            status.accept("Network: applying " + impairment.name() + " failed: " + e.getMessage());
            failure.completeExceptionally(new IllegalStateException("Applying the network impairment "
                    + impairment.name() + " failed: " + e.getMessage(), e));
        }
    }

    /**
     * Disconnects the senders' TCP connections to the receivers without the receivers knowing: it lists the
     * connections in each sender's namespace, adds filters that drop each connection's packets both ways, on both
     * ends' veths, and then destroys the senders' sockets. The senders get an error at once and reconnect, on new
     * ports, while their resets are dropped, so that the receivers keep the connections until something of theirs,
     * such as a keep-alive, finds them dead. The filters stay until the impairment's end.
     */
    private void disconnectSilently(Activation activation) throws IOException {
        Impairment impairment = activation.impairment;
        Map<String, Placement> placementsByContainer = new LinkedHashMap<>();
        activation.placements.forEach(placement -> placementsByContainer.put(placement.device().owner.name(),
                placement));
        Map<Target, List<TcCommands.Connection>> connections = new LinkedHashMap<>();
        List<String> filters = new ArrayList<>();
        Set<String> filteredDevices = new LinkedHashSet<>();
        for (Map.Entry<Target, List<String>> entry : activation.resets.entrySet()) {
            Target sender = entry.getKey();
            if (!running(sender)) {
                continue;
            }
            for (String address : entry.getValue()) {
                Target receiver = targets.values().stream().filter(target -> target.ip().equals(address))
                        .findFirst().orElseThrow();
                for (TcCommands.Connection connection : TcCommands.parseConnections(
                        exec(sender, TcCommands.connections(address)))) {
                    connections.computeIfAbsent(sender, key -> new ArrayList<>()).add(connection);
                    Placement toReceiver = placementsByContainer.get(receiver.name());
                    Placement toSender = placementsByContainer.get(sender.name());
                    filters.add(TcCommands.flowFilter(toReceiver.device().name(), activation.preference,
                            toReceiver.slot(), connection.local(), connection.peer()));
                    filters.add(TcCommands.flowFilter(toSender.device().name(), activation.preference,
                            toSender.slot(), connection.peer(), connection.local()));
                    filteredDevices.add(toReceiver.device().name());
                    filteredDevices.add(toSender.device().name());
                }
            }
        }
        // Only these devices have the impairment's filters to remove: a device whose container had no connection to
        // disconnect has none, and deleting a filter preference that a device doesn't have fails
        activation.filteredDevices = filteredDevices;
        if (!filters.isEmpty()) {
            // Every device of a silent disconnection is a veth in the engine's host
            exec(null, String.join("\n", filters));
        }
        int destroyed = 0;
        for (Map.Entry<Target, List<TcCommands.Connection>> entry : connections.entrySet()) {
            String output = exec(entry.getKey(), entry.getValue().stream().map(TcCommands::destroy)
                    .collect(Collectors.joining("\n")));
            destroyed += (int) output.lines().filter(line -> !line.isBlank()).count();
        }
        activation.resetSockets = destroyed;
        activation.applied = Instant.now();
        activation.status = Status.ACTIVE;
        int listed = connections.values().stream().mapToInt(List::size).sum();
        if (destroyed == 0) {
            activation.note = "matched no TCP connection";
        }
        status.accept(String.format(Locale.ROOT, "Network: %s: silently disconnected %d of %d TCP connection(s) (%s)",
                impairment.name(), destroyed, listed, impairment.endpoints()));
        int finalDestroyed = destroyed;
        record("silently-disconnected", activation, node -> node.put("connections", listed)
                .put("destroyed", finalDestroyed));
    }

    /**
     * Removes an impairment: deletes its filters. The packets that its queues hold are still sent, at their time.
     * A device whose container has exited needs nothing, since the container's interfaces have gone with it.
     */
    private synchronized void remove(Activation activation) {
        if (closed || activation.status != Status.ACTIVE) {
            return;
        }
        Impairment impairment = activation.impairment;
        try {
            ObjectNode statistics = statistics(activation);
            for (Placement placement : activation.placements) {
                if (activation.filteredDevices != null
                        && !activation.filteredDevices.contains(placement.device().name())) {
                    continue;
                }
                if (running(placement.device().owner)) {
                    try {
                        exec(placement.device().hostSide ? null : placement.device().owner,
                                TcCommands.deleteFilters(placement.device().name(), activation.preference));
                    } catch (IOException | RuntimeException e) {
                        // A container that exited meanwhile has taken its interfaces and their filters with it
                        if (running(placement.device().owner)) {
                            throw e;
                        }
                    }
                }
            }
            activation.removed = Instant.now();
            activation.status = Status.REMOVED;
            status.accept("Network: removed " + impairment.name());
            record("removed", activation, node -> node.set("statistics", statistics));
        } catch (IOException | RuntimeException e) {
            if (closing) {
                return;
            }
            activation.status = Status.FAILED;
            activation.note = e.getMessage();
            record("failed", activation, node -> node.put("error", String.valueOf(e.getMessage())));
            failure.completeExceptionally(new IllegalStateException("Removing the network impairment "
                    + impairment.name() + " failed: " + e.getMessage(), e));
        }
    }

    /**
     * Stops the timeline when the workloads have finished: an impairment that hasn't started by then is missed, since
     * no traffic would see it. The impairments that are applied stay until {@link #close()}. Fails with the first
     * failure of an impairment.
     */
    void finishRun() throws IOException {
        stopTimeline();
        synchronized (this) {
            for (Activation activation : activations) {
                if (activation.status == Status.PENDING) {
                    activation.status = Status.MISSED;
                    record("missed", activation, node -> { });
                }
            }
        }
        if (failure.isCompletedExceptionally()) {
            try {
                failure.join();
            } catch (CompletionException e) {
                throw new IOException(e.getCause().getMessage(), e.getCause());
            }
        }
    }

    private void stopTimeline() {
        closing = true;
        timeline.shutdownNow();
        sampler.shutdownNow();
        try {
            timeline.awaitTermination(COMMAND_TIMEOUT_SECONDS + 5, TimeUnit.SECONDS);
            sampler.awaitTermination(COMMAND_TIMEOUT_SECONDS + 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * The impairments that didn't do what the scenario asked: one that never started (missed) or wasn't applied
     * because its containers had exited (skipped), and a reset that matched no TCP connection. A run with any of them
     * didn't test what it was meant to.
     */
    synchronized List<String> unexercised() {
        List<String> problems = new ArrayList<>();
        for (Activation activation : activations) {
            String name = activation.impairment.name();
            switch (activation.status) {
                case MISSED -> problems.add(name + " never started: the run ended before it");
                case SKIPPED -> problems.add(name + " wasn't applied: " + activation.note);
                case PENDING -> problems.add(name + " never started");
                case FAILED -> problems.add(name + " failed: " + activation.note);
                default -> {
                    Kind kind = activation.impairment.kind();
                    if ((kind == Kind.RESET || kind == Kind.SILENT_DISCONNECT) && activation.resetSockets == 0) {
                        problems.add(name + " " + activation.note);
                    } else if (!activation.skipped.isEmpty()) {
                        problems.add(name + " was applied only in part: " + activation.note);
                    }
                }
            }
        }
        return problems;
    }

    /** The impairments' applications and removals, for Grafana's annotations. */
    synchronized List<MetricsCollection.Event> annotations() {
        List<MetricsCollection.Event> annotations = new ArrayList<>();
        for (Activation activation : activations) {
            Impairment impairment = activation.impairment;
            String what = impairment.description() + ", " + impairment.endpoints();
            if (activation.applied != null) {
                annotations.add(new MetricsCollection.Event("network-" + impairment.name(),
                        (impairment.kind() == Kind.RESET ? "Network reset: " : "Network impairment applied: ")
                                + impairment.name() + " (" + what + ")", activation.applied));
            }
            if (activation.removed != null) {
                annotations.add(new MetricsCollection.Event("network-" + impairment.name() + "-removed",
                        "Network impairment removed: " + impairment.name(), activation.removed));
            }
        }
        return annotations;
    }

    /**
     * Stops the timeline, removes every filter and qdisc that was installed, restores the offloads, writes the
     * summary for the run report, and stops the sidecar. Each step runs also when an earlier one fails; the first
     * failure is thrown at the end.
     */
    @Override
    public void close() throws IOException {
        stopTimeline();
        List<Exception> failures = new ArrayList<>();
        // Failures to read the final counters, which the summary lacks, but which leave nothing behind
        List<Exception> observationFailures = new ArrayList<>();
        Map<Activation, ObjectNode> finalStatistics = new LinkedHashMap<>();
        synchronized (this) {
            closed = true;
            for (Activation activation : activations) {
                if (activation.status == Status.PENDING) {
                    activation.status = Status.MISSED;
                    record("missed", activation, node -> { });
                }
                try {
                    finalStatistics.put(activation, statistics(activation));
                } catch (IOException | RuntimeException e) {
                    observationFailures.add(e);
                }
            }
            for (Target target : targets.values()) {
                try {
                    if (running(target)) {
                        tcpAtEnd.put(target.name(), tcpCounters(target));
                    }
                } catch (IOException | RuntimeException e) {
                    observationFailures.add(e);
                }
            }
            for (Device device : devices.values()) {
                try {
                    if (device.installed && running(device.owner)) {
                        exec(device.hostSide ? null : device.owner, TcCommands.deleteRoot(device.name()));
                    }
                } catch (IOException | RuntimeException e) {
                    failures.add(e);
                }
            }
            for (Map.Entry<Target, boolean[]> entry : offloads.entrySet()) {
                Target target = entry.getKey();
                try {
                    if (running(target)) {
                        exec(target, TcCommands.setOffloads(target.containerInterface(), entry.getValue()[0],
                                entry.getValue()[1]));
                    }
                } catch (IOException | RuntimeException e) {
                    failures.add(e);
                }
            }
            record("cleaned-up", null, node -> {
                ArrayNode list = node.putArray("failures");
                failures.forEach(failure -> list.add(String.valueOf(failure.getMessage())));
                ArrayNode observations = node.putArray("observationFailures");
                observationFailures.forEach(failure -> observations.add(String.valueOf(failure.getMessage())));
            });
            if (!observationFailures.isEmpty()) {
                status.accept("Network: couldn't read the final counters of the impairments: "
                        + observationFailures.get(0).getMessage());
            }
            try {
                writeSummary(finalStatistics);
            } catch (IOException | RuntimeException e) {
                failures.add(e);
            }
        }
        try {
            events.close();
        } catch (IOException e) {
            failures.add(e);
        }
        host.close();
        if (!failures.isEmpty()) {
            IOException failure = new IOException("Cleaning up the network impairments failed: "
                    + failures.get(0).getMessage(), failures.get(0));
            failures.stream().skip(1).forEach(failure::addSuppressed);
            throw failure;
        }
    }

    /** Places every impairment's bands on its devices and gives each its filter preference. */
    private void plan() {
        int index = 0;
        for (Impairment impairment : settings.impairments()) {
            boolean dropping = impairment.kind() == Kind.PARTITION || impairment.kind() == Kind.SILENT_DISCONNECT;
            Activation activation = new Activation(impairment, (dropping
                    ? TcCommands.PARTITION_PREFERENCE : TcCommands.CONDITION_PREFERENCE) + index++);
            activations.add(activation);
            List<Pair> pairs = NetworkSettings.pairs(impairment, topology);
            if (impairment.kind() == Kind.SILENT_DISCONNECT) {
                // A band on both ends' veths, whose filters the disconnection adds for each connection
                Set<String> ends = new LinkedHashSet<>();
                for (Pair pair : pairs) {
                    ends.add(pair.receiver());
                    ends.add(pair.sender());
                    activation.resets.computeIfAbsent(targets.get(pair.sender()), key -> new ArrayList<>())
                            .add(targets.get(pair.receiver()).ip());
                }
                ends.forEach(end -> activation.placements.add(place(activation, targets.get(end), true, List.of())));
                continue;
            }
            if (impairment.kind() == Kind.RESET) {
                for (Pair pair : pairs) {
                    activation.resets.computeIfAbsent(targets.get(pair.sender()), key -> new ArrayList<>())
                            .add(targets.get(pair.receiver()).ip());
                }
                continue;
            }
            if (impairment.pathImpairment()) {
                Map<String, List<String>> sourcesByReceiver = new LinkedHashMap<>();
                for (Pair pair : pairs) {
                    sourcesByReceiver.computeIfAbsent(pair.receiver(), key -> new ArrayList<>())
                            .add(targets.get(pair.sender()).ip());
                }
                sourcesByReceiver.forEach((receiver, sources) ->
                        activation.placements.add(place(activation, targets.get(receiver), true, sources)));
            }
            if (impairment.rateLimited()) {
                Map<String, List<String>> destinationsBySender = new LinkedHashMap<>();
                for (Pair pair : pairs) {
                    destinationsBySender.computeIfAbsent(pair.sender(), key -> new ArrayList<>())
                            .add(targets.get(pair.receiver()).ip());
                }
                destinationsBySender.forEach((sender, destinations) ->
                        activation.placements.add(place(activation, targets.get(sender), false, destinations)));
            }
        }
    }

    private Placement place(Activation activation, Target owner, boolean hostSide, List<String> addresses) {
        Device device = devices.computeIfAbsent((hostSide ? "host:" : "container:") + owner.name(),
                key -> new Device(owner, hostSide));
        device.slots.add(activation);
        return new Placement(device, device.slots.size() - 1, List.copyOf(addresses));
    }

    /** Turns off the segmentation offloads where the settings need it, before any traffic is impaired. */
    private void setOffloads() throws IOException {
        Set<String> senders = new LinkedHashSet<>();
        switch (settings.offloads()) {
            case ON -> {
                return;
            }
            case OFF -> senders.addAll(targets.keySet());
            default -> {
                for (Impairment impairment : settings.impairments()) {
                    if (impairment.needsSegmentOffloadsOff()) {
                        NetworkSettings.pairs(impairment, topology).forEach(pair -> senders.add(pair.sender()));
                    }
                }
            }
        }
        ObjectNode recorded = mapper.createObjectNode();
        for (String sender : senders) {
            Target target = targets.get(sender);
            String device = target.containerInterface();
            String before = exec(target, TcCommands.showOffloads(device));
            offloads.put(target, new boolean[] {TcCommands.featureOn(before, "tcp-segmentation-offload"),
                    TcCommands.featureOn(before, "generic-segmentation-offload")});
            exec(target, TcCommands.setOffloads(device, false, false));
            String after = exec(target, TcCommands.showOffloads(device));
            if (TcCommands.featureOn(after, "tcp-segmentation-offload")
                    || TcCommands.featureOn(after, "generic-segmentation-offload")) {
                throw new IllegalStateException("Couldn't turn off the segmentation offloads of " + sender);
            }
            recorded.putObject(sender).put("tsoBefore", offloads.get(target)[0])
                    .put("gsoBefore", offloads.get(target)[1])
                    .put("groAfter", TcCommands.featureOn(after, "generic-receive-offload"));
        }
        if (!senders.isEmpty()) {
            status.accept("Network: turned off the TCP segmentation offloads of " + String.join(", ", senders));
            record("offloads", null, node -> node.set("off", recorded));
        }
    }

    private boolean running(Target target) {
        return host.running(target);
    }

    /** The TCP counters of a target's namespace: the segments sent and retransmitted. */
    private long[] tcpCounters(Target target) throws IOException {
        List<String> tcp = exec(target, "grep '^Tcp:' /proc/net/snmp").lines().toList();
        if (tcp.size() < 2) {
            throw new IllegalStateException("No TCP counters in the namespace of " + target.name());
        }
        List<String> names = List.of(tcp.get(0).split("\\s+"));
        String[] values = tcp.get(1).split("\\s+");
        return new long[] {Long.parseLong(values[names.indexOf("OutSegs")]),
                Long.parseLong(values[names.indexOf("RetransSegs")])};
    }

    /**
     * The counters of an impairment's netem qdiscs on its devices, by container, side and device. A device whose
     * container has exited keeps its last counters.
     */
    private ObjectNode statistics(Activation activation) throws IOException {
        return statistics(activation, false);
    }

    private ObjectNode statistics(Activation activation, boolean workloadsOnly) throws IOException {
        ObjectNode statistics = mapper.createObjectNode();
        for (Placement placement : activation.placements) {
            Device device = placement.device();
            if (workloadsOnly && !isWorkload(device.owner)) {
                continue;
            }
            String key = device.owner.name() + " " + (device.hostSide ? "receiver " : "sender ") + device.name();
            if (!device.installed || !running(device.owner)) {
                ObjectNode last = lastStatistics.get(placement);
                if (last != null) {
                    statistics.set(key, last);
                }
                continue;
            }
            JsonNode qdiscs = mapper.readTree(exec(device.hostSide ? null : device.owner,
                    TcCommands.statistics(device.name())));
            String handle = TcCommands.netemHandle(TcCommands.band(placement.slot()));
            for (JsonNode qdisc : qdiscs) {
                if ("netem".equals(qdisc.path("kind").asText()) && handle.equals(qdisc.path("handle").asText())) {
                    lastStatistics.put(placement, statistics.putObject(key).put("container", device.owner.name())
                            .put("side", device.hostSide ? "receiver" : "sender")
                            .put("packets", qdisc.path("packets").asLong())
                            .put("bytes", qdisc.path("bytes").asLong())
                            .put("drops", qdisc.path("drops").asLong())
                            .put("backlogPackets", qdisc.path("qlen").asLong()));
                }
            }
        }
        return statistics;
    }

    private void writeSummary(Map<Activation, ObjectNode> finalStatistics) throws IOException {
        ObjectNode summary = mapper.createObjectNode();
        summary.put("offloads", settings.offloads().name().toLowerCase(Locale.ROOT));
        ArrayNode offloadsOff = summary.putArray("offloadsOff");
        offloads.keySet().forEach(target -> offloadsOff.add(target.name()));
        if (measurementStart != null) {
            summary.put("measurementStartEpochMs", measurementStart.toEpochMilli());
        }
        ArrayNode impairments = summary.putArray("impairments");
        for (Activation activation : activations) {
            Impairment impairment = activation.impairment;
            ObjectNode node = impairments.addObject()
                    .put("name", impairment.name())
                    .put("kind", impairment.kind().name().toLowerCase(Locale.ROOT))
                    .put("description", impairment.description())
                    .put("endpoints", impairment.endpoints())
                    .put("anchor", impairment.wholeRun() ? "whole run"
                            : impairment.anchor().name().toLowerCase(Locale.ROOT))
                    .put("status", activation.status.name().toLowerCase(Locale.ROOT));
            if (impairment.start() != null) {
                node.put("startMillis", impairment.start().toMillis());
            }
            if (impairment.duration() != null) {
                node.put("durationMillis", impairment.duration().toMillis());
            }
            if (activation.applied != null) {
                node.put("appliedEpochMs", activation.applied.toEpochMilli());
            }
            if (activation.removed != null) {
                node.put("removedEpochMs", activation.removed.toEpochMilli());
            }
            if (impairment.kind() == Kind.RESET || impairment.kind() == Kind.SILENT_DISCONNECT) {
                node.put("resetConnections", activation.resetSockets);
            }
            if (impairment.kind() == Kind.CONDITION) {
                node.put("lossPercent", impairment.lossPercent());
            }
            if (activation.note != null) {
                node.put("note", activation.note);
            }
            ObjectNode statistics = finalStatistics.get(activation);
            if (statistics != null) {
                node.set("statistics", statistics);
            }
        }
        ArrayNode tcp = summary.putArray("tcp");
        tcpAtStart.forEach((name, start) -> {
            long[] end = tcpAtEnd.get(name);
            if (end != null) {
                tcp.addObject().put("container", name).put("outSegments", end[0] - start[0])
                        .put("retransmittedSegments", end[1] - start[1]);
            }
        });
        mapper.writerWithDefaultPrettyPrinter().writeValue(
                runDirectory.resolve(RunReport.NETWORK_SUMMARY_FILE).toFile(), summary);
    }

    /** Appends an event to {@value #EVENTS_FILE}, flushed, so that a failed run keeps it. */
    private void record(String event, Activation activation, Consumer<ObjectNode> details) {
        ObjectNode node = mapper.createObjectNode();
        node.put("epochMs", System.currentTimeMillis()).put("event", event);
        if (activation != null) {
            node.put("impairment", activation.impairment.name());
            if (activation.note != null) {
                node.put("note", activation.note);
            }
        }
        details.accept(node);
        try {
            events.write(mapper.writeValueAsString(node).replace("\n", ""));
            events.newLine();
            events.flush();
        } catch (IOException e) {
            status.accept("Network: couldn't record an event: " + e.getMessage());
        }
    }

    private String exec(Target target, String script) throws IOException {
        return host.exec(target, script);
    }

    /** The privileged sidecar in the Docker engine host's network and PID namespaces. */
    private static final class DockerHost implements Host {
        private final GenericContainer<?> sidecar;
        private final AtomicLong scripts = new AtomicLong();

        DockerHost(GenericContainer<?> sidecar) {
            this.sidecar = sidecar;
        }

        @Override
        public Target resolve(String name, GenericContainer<?> container, String networkId) throws IOException {
            InspectContainerResponse info = container.getCurrentContainerInfo();
            Long pid = info.getState().getPidLong();
            if (!Boolean.TRUE.equals(info.getState().getRunning()) || pid == null || pid <= 0) {
                throw new IllegalStateException(name + " isn't running, so its network can't be impaired");
            }
            String ip = null;
            for (ContainerNetwork network : info.getNetworkSettings().getNetworks().values()) {
                if (networkId.equals(network.getNetworkID())) {
                    ip = network.getIpAddress();
                }
            }
            if (ip == null || ip.isEmpty()) {
                throw new IllegalStateException(name + " has no IPv4 address on the cluster's network; the network"
                        + " impairments support IPv4 only");
            }
            Target partial = new Target(name, container, pid, ip, null, null);
            String containerInterface = TcCommands.interfaceWithAddress(exec(partial, "ip -o -4 addr show"), ip);
            String containerLink = exec(partial, "ip -o link show " + TcCommands.device(containerInterface));
            String hostInterface = TcCommands.interfaceWithIndex(exec(null, "ip -o link"),
                    TcCommands.peerIndex(containerLink), TcCommands.linkIndex(containerLink));
            return new Target(name, container, pid, ip, containerInterface, hostInterface);
        }

        /**
         * Fails when Docker can't tell, after a few attempts, so that an impairment isn't skipped, or left in place,
         * for a container that may run.
         */
        @Override
        public boolean running(Target target) {
            RuntimeException failure = null;
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    InspectContainerResponse info = target.container().getCurrentContainerInfo();
                    return Boolean.TRUE.equals(info.getState().getRunning())
                            && Long.valueOf(target.pid()).equals(info.getState().getPidLong());
                } catch (NotFoundException e) {
                    // Removed
                    return false;
                } catch (RuntimeException e) {
                    failure = e;
                    // a transient error of the Docker API, which a moment later usually answers
                    try {
                        Thread.sleep(200L * (attempt + 1));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            throw new IllegalStateException("Docker can't tell whether " + target.name() + " runs: "
                    + failure.getMessage(), failure);
        }

        /**
         * Runs a shell script in the sidecar: in the engine host's network namespace, or in a target's, and returns its
         * output; fails when it fails or takes longer than {@value NetworkImpairments#COMMAND_TIMEOUT_SECONDS} s.
         */
        @Override
        public String exec(Target target, String script) throws IOException {
            List<String> command = new ArrayList<>(List.of("timeout", Integer.toString(COMMAND_TIMEOUT_SECONDS)));
            if (target != null) {
                command.addAll(List.of("nsenter", "-t", Long.toString(target.pid()), "-n"));
            }
            String body = "set -e\n" + script;
            if (body.length() > MAX_INLINE_SCRIPT) {
                // Linux takes at most 128 KB in one argument, which the filters of hundreds of connections exceed; a
                // file in the sidecar works in any network namespace, since nsenter -n keeps the sidecar's file system
                String file = "/tmp/network-impairments-" + scripts.incrementAndGet() + ".sh";
                sidecar.copyFileToContainer(Transferable.of(body.getBytes(StandardCharsets.UTF_8)), file);
                command.addAll(List.of("sh", file));
            } else {
                command.addAll(List.of("sh", "-c", body));
            }
            try {
                Container.ExecResult result = sidecar.execInContainer(command.toArray(String[]::new));
                if (result.getExitCode() != 0) {
                    String shown = script.replace("\n", "; ");
                    throw new IOException("`" + (shown.length() > 300 ? shown.substring(0, 300) + " ..." : shown) + "`"
                            + (target != null ? " in " + target.name() : "") + " failed with status "
                            + result.getExitCode() + ": " + (result.getStderr() + result.getStdout()).strip());
                }
                return result.getStdout();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while running " + script, e);
            }
        }

        @Override
        public void close() {
            sidecar.stop();
        }
    }
}

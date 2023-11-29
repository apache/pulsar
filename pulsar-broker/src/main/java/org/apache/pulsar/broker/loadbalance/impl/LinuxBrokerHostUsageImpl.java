/**
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
package org.apache.pulsar.broker.loadbalance.impl;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.base.Charsets;
import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;


/**
 * Class that will return the broker host usage.
 */
@Slf4j
public class LinuxBrokerHostUsageImpl implements BrokerHostUsage {
    private long lastCollection;
    private double lastTotalNicUsageTx;
    private double lastTotalNicUsageRx;
    private double lastCpuUsage;
    private double lastCpuTotalTime;
    private OperatingSystemMXBean systemBean;
    private SystemResourceUsage usage;

    private final Optional<Double> overrideBrokerNicSpeedGbps;
    private final boolean isCGroupsEnabled;

    private static final String CGROUPS_CPU_USAGE_PATH = "/sys/fs/cgroup/cpu/cpuacct.usage";
    private static final String CGROUPS_CPU_LIMIT_QUOTA_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
    private static final String CGROUPS_CPU_LIMIT_PERIOD_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

    public LinuxBrokerHostUsageImpl(PulsarService pulsar) {
        this(
            pulsar.getConfiguration().getLoadBalancerHostUsageCheckIntervalMinutes(),
            pulsar.getConfiguration().getLoadBalancerOverrideBrokerNicSpeedGbps(),
            pulsar.getLoadManagerExecutor()
        );
    }

    public LinuxBrokerHostUsageImpl(int hostUsageCheckIntervalMin,
                                    Optional<Double> overrideBrokerNicSpeedGbps,
                                    ScheduledExecutorService executorService) {
        this.systemBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        this.lastCollection = 0L;
        this.usage = new SystemResourceUsage();
        this.overrideBrokerNicSpeedGbps = overrideBrokerNicSpeedGbps;

        boolean isCGroupsEnabled = false;
        try {
             isCGroupsEnabled = Files.exists(Paths.get(CGROUPS_CPU_USAGE_PATH));
        } catch (Exception e) {
            log.warn("Failed to check cgroup CPU usage file: {}", e.getMessage());
        }
        this.isCGroupsEnabled = isCGroupsEnabled;

        // Call now to initialize values before the constructor returns
        calculateBrokerHostUsage();
        executorService.scheduleWithFixedDelay(catchingAndLoggingThrowables(this::calculateBrokerHostUsage),
                hostUsageCheckIntervalMin,
                hostUsageCheckIntervalMin, TimeUnit.MINUTES);
    }

    @Override
    public SystemResourceUsage getBrokerHostUsage() {
        return usage;
    }

    @Override
    public void calculateBrokerHostUsage() {
        List<String> nics = getNics();
        double totalNicLimit = getTotalNicLimitKbps(nics);
        double totalNicUsageTx = getTotalNicUsageTxKb(nics);
        double totalNicUsageRx = getTotalNicUsageRxKb(nics);
        double totalCpuLimit = getTotalCpuLimit();

        long now = System.currentTimeMillis();
        double elapsedSeconds = (now - lastCollection) / 1000d;
        if (elapsedSeconds <= 0) {
            log.warn("elapsedSeconds {} is not expected, skip this round of calculateBrokerHostUsage", elapsedSeconds);
            return;
        }

        SystemResourceUsage usage = new SystemResourceUsage();
        double cpuUsage = getTotalCpuUsage(elapsedSeconds);

        if (lastCollection == 0L) {
            usage.setMemory(getMemUsage());
            usage.setBandwidthIn(new ResourceUsage(0d, totalNicLimit));
            usage.setBandwidthOut(new ResourceUsage(0d, totalNicLimit));
        } else {
            double nicUsageTx = (totalNicUsageTx - lastTotalNicUsageTx) / elapsedSeconds;
            double nicUsageRx = (totalNicUsageRx - lastTotalNicUsageRx) / elapsedSeconds;

            usage.setMemory(getMemUsage());
            usage.setBandwidthIn(new ResourceUsage(nicUsageRx, totalNicLimit));
            usage.setBandwidthOut(new ResourceUsage(nicUsageTx, totalNicLimit));
        }

        lastTotalNicUsageTx = totalNicUsageTx;
        lastTotalNicUsageRx = totalNicUsageRx;
        lastCollection = System.currentTimeMillis();
        this.usage = usage;
        usage.setCpu(new ResourceUsage(cpuUsage, totalCpuLimit));
    }

    private double getTotalCpuLimit() {
        if (isCGroupsEnabled) {
            try {
                long quota = readLongFromFile(CGROUPS_CPU_LIMIT_QUOTA_PATH);
                long period = readLongFromFile(CGROUPS_CPU_LIMIT_PERIOD_PATH);
                if (quota > 0) {
                    return 100.0 * quota / period;
                }
            } catch (IOException e) {
                log.warn("Failed to read CPU quotas from cgroups", e);
                // Fallback to availableProcessors
            }
        }

        // Fallback to JVM reported CPU quota
        return 100 * Runtime.getRuntime().availableProcessors();
    }

    private double getTotalCpuUsage(double elapsedTimeSeconds) {
        if (isCGroupsEnabled) {
            return getTotalCpuUsageForCGroup(elapsedTimeSeconds);
        } else {
            return getTotalCpuUsageForEntireHost();
        }
    }

    /**
     * Reads first line of /proc/stat to get total cpu usage.
     *
     * <pre>
     *     cpu  user   nice system idle    iowait irq softirq steal guest guest_nice
     *     cpu  317808 128  58637  2503692 7634   0   13472   0     0     0
     * </pre>
     *
     * Line is split in "words", filtering the first. The sum of all numbers give the amount of cpu cycles used this
     * far. Real CPU usage should equal the sum substracting the idle cycles, this would include iowait, irq and steal.
     */
    private double getTotalCpuUsageForEntireHost() {
        try (Stream<String> stream = Files.lines(Paths.get("/proc/stat"))) {
            String[] words = stream.findFirst().get().split("\\s+");

            long total = Arrays.stream(words).filter(s -> !s.contains("cpu")).mapToLong(Long::parseLong).sum();
            long idle = Long.parseLong(words[4]);
            long usage = total - idle;

            double currentUsage = (usage - lastCpuUsage)  / (total - lastCpuTotalTime) * getTotalCpuLimit();

            lastCpuUsage = usage;
            lastCpuTotalTime = total;

            return currentUsage;
        } catch (IOException e) {
            log.error("Failed to read CPU usage from /proc/stat", e);
            return -1;
        }
    }

    private double getTotalCpuUsageForCGroup(double elapsedTimeSeconds) {
        try {
            long usage = readLongFromFile(CGROUPS_CPU_USAGE_PATH);
            double currentUsage = usage - lastCpuUsage;
            lastCpuUsage = usage;

            return 100 * currentUsage / elapsedTimeSeconds / TimeUnit.SECONDS.toNanos(1);
        } catch (IOException e) {
            log.error("Failed to read CPU usage from {}", CGROUPS_CPU_USAGE_PATH, e);
            return -1;
        }
    }

    private ResourceUsage getMemUsage() {
        double total = ((double) systemBean.getTotalPhysicalMemorySize()) / (1024 * 1024);
        double free = ((double) systemBean.getFreePhysicalMemorySize()) / (1024 * 1024);
        return new ResourceUsage(total - free, total);
    }

    private List<String> getNics() {
        try (Stream<Path> stream = Files.list(Paths.get("/sys/class/net/"))) {
            return stream.filter(this::isPhysicalNic).map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("Failed to find NICs", e);
            return Collections.emptyList();
        }
    }

    public int getNicCount() {
        return getNics().size();
    }

    private boolean isPhysicalNic(Path path) {
        try {
            if (path.toString().contains("/virtual/")) {
                return false;
            }
            // Check the type to make sure it's ethernet (type "1")
            String type = new String(Files.readAllBytes(path.resolve("type")), StandardCharsets.UTF_8).trim();
            // wireless NICs don't report speed, ignore them.
            return Integer.parseInt(type) == 1;
        } catch (Exception e) {
            // Read type got error.
            return false;
        }
    }

    private Path getNicSpeedPath(String nic) {
        return Paths.get(String.format("/sys/class/net/%s/speed", nic));
    }

    private double getTotalNicLimitKbps(List<String> nics) {
        // Use the override value as configured. Return the total max speed across all available NICs, converted
        // from Gbps into Kbps
        return overrideBrokerNicSpeedGbps.map(aDouble -> aDouble * nics.size() * 1000 * 1000)
                .orElseGet(() -> nics.stream().mapToDouble(nicPath -> {
                    // Nic speed is in Mbits/s, return kbits/s
                    try {
                        return Double.parseDouble(new String(Files.readAllBytes(getNicSpeedPath(nicPath))));
                    } catch (IOException e) {
                        log.error(String.format("Failed to read speed for nic %s, maybe you can set broker"
                                + " config [loadBalancerOverrideBrokerNicSpeedGbps] to override it.", nicPath), e);
                        return 0d;
                    }
                }).sum() * 1000);
    }

    private Path getNicTxPath(String nic) {
        return Paths.get(String.format("/sys/class/net/%s/statistics/tx_bytes", nic));
    }

    private Path getNicRxPath(String nic) {
        return Paths.get(String.format("/sys/class/net/%s/statistics/rx_bytes", nic));
    }

    private double getTotalNicUsageRxKb(List<String> nics) {
        return nics.stream().mapToDouble(s -> {
            try {
                return Double.parseDouble(new String(Files.readAllBytes(getNicRxPath(s))));
            } catch (IOException e) {
                log.error("Failed to read rx_bytes for NIC " + s, e);
                return 0d;
            }
        }).sum() * 8d / 1000;
    }

    private double getTotalNicUsageTxKb(List<String> nics) {
        return nics.stream().mapToDouble(s -> {
            try {
                return Double.parseDouble(new String(Files.readAllBytes(getNicTxPath(s))));
            } catch (IOException e) {
                log.error("Failed to read tx_bytes for NIC " + s, e);
                return 0d;
            }
        }).sum() * 8d / 1000;
    }

    private static long readLongFromFile(String path) throws IOException {
        return Long.parseLong(new String(Files.readAllBytes(Paths.get(path)), Charsets.UTF_8).trim());
    }
}

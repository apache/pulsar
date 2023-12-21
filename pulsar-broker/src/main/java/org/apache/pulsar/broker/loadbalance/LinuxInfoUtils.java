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
package org.apache.pulsar.broker.loadbalance;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.BitRateUnit;
import oshi.hardware.NetworkIF;

@Slf4j
public class LinuxInfoUtils {

    // CGROUP
    private static final String CGROUPS_CPU_USAGE_PATH = "/sys/fs/cgroup/cpu/cpuacct.usage";
    private static final String CGROUPS_CPU_LIMIT_QUOTA_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
    private static final String CGROUPS_CPU_LIMIT_PERIOD_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

    // proc states
    private static final String PROC_STAT_PATH = "/proc/stat";

    private static Object /*jdk.internal.platform.Metrics*/ metrics;
    private static Method getMetricsProviderMethod;
    private static Method getCpuQuotaMethod;
    private static Method getCpuPeriodMethod;
    private static Method getCpuUsageMethod;

    static {
        try {
            metrics = Class.forName("jdk.internal.platform.Container").getMethod("metrics")
                    .invoke(null);
            if (metrics != null) {
                getMetricsProviderMethod = metrics.getClass().getMethod("getProvider");
                getMetricsProviderMethod.setAccessible(true);
                getCpuQuotaMethod = metrics.getClass().getMethod("getCpuQuota");
                getCpuQuotaMethod.setAccessible(true);
                getCpuPeriodMethod = metrics.getClass().getMethod("getCpuPeriod");
                getCpuPeriodMethod.setAccessible(true);
                getCpuUsageMethod = metrics.getClass().getMethod("getCpuUsage");
                getCpuUsageMethod.setAccessible(true);
            }
        } catch (Throwable e) {
            log.warn("Failed to get runtime metrics", e);
        }
    }

    private LinuxInfoUtils() {
    }

    /**
     * Determine whether the OS is the linux kernel.
     * @return Whether the OS is the linux kernel
     */
    public static boolean isLinux() {
        return SystemUtils.IS_OS_LINUX;
    }

    /**
     * Determine whether the OS enable CG Group.
     */
    public static boolean isCGroupEnabled() {
        try {
            if (metrics == null) {
                return Files.exists(Paths.get(CGROUPS_CPU_USAGE_PATH));
            }
            String provider = (String) getMetricsProviderMethod.invoke(metrics);
            log.info("[LinuxInfo] The system metrics provider is: {}", provider);
            return provider.contains("cgroup");
        } catch (Exception e) {
            log.warn("[LinuxInfo] Failed to check cgroup CPU: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Get total cpu limit.
     * @param isCGroupsEnabled Whether CGroup is enabled
     * @return Total cpu limit
     */
    public static double getTotalCpuLimit(boolean isCGroupsEnabled) {
        if (isCGroupsEnabled) {
            try {
                long quota;
                long period;
                if (metrics != null && getCpuQuotaMethod != null && getCpuPeriodMethod != null) {
                    quota = (long) getCpuQuotaMethod.invoke(metrics);
                    period = (long) getCpuPeriodMethod.invoke(metrics);
                } else {
                    quota = readLongFromFile(Paths.get(CGROUPS_CPU_LIMIT_QUOTA_PATH));
                    period = readLongFromFile(Paths.get(CGROUPS_CPU_LIMIT_PERIOD_PATH));
                }

                if (quota > 0) {
                    return 100.0 * quota / period;
                }
            } catch (Exception e) {
                log.warn("[LinuxInfo] Failed to read CPU quotas from cgroup", e);
                // Fallback to availableProcessors
            }
        }
        // Fallback to JVM reported CPU quota
        return 100 * Runtime.getRuntime().availableProcessors();
    }

    /**
     * Get CGroup cpu usage.
     * @return Cpu usage
     */
    public static long getCpuUsageForCGroup() {
        try {
            if (metrics != null && getCpuUsageMethod != null) {
                return (long) getCpuUsageMethod.invoke(metrics);
            }
            return readLongFromFile(Paths.get(CGROUPS_CPU_USAGE_PATH));
        } catch (Exception e) {
            log.error("[LinuxInfo] Failed to read CPU usage from cgroup", e);
            return -1;
        }
    }


    /**
     * Reads first line of /proc/stat to get total cpu usage.
     *
     * <pre>
     *     cpu  user   nice system idle    iowait irq softirq steal guest guest_nice
     *     cpu  317808 128  58637  2503692 7634   0   13472   0     0     0
     * </pre>
     * <p>
     * Line is split in "words", filtering the first. The sum of all numbers give the amount of cpu cycles used this
     * far. Real CPU usage should equal the sum substracting the idle cycles(that is idle+iowait), this would include
     * cpu, user, nice, system, irq, softirq, steal, guest and guest_nice.
     */
    public static ResourceUsage getCpuUsageForEntireHost() {
        try (Stream<String> stream = Files.lines(Paths.get(PROC_STAT_PATH))) {
            Optional<String> first = stream.findFirst();
            if (!first.isPresent()) {
                log.error("[LinuxInfo] Failed to read CPU usage from /proc/stat, because of empty values.");
                return ResourceUsage.empty();
            }
            String[] words = first.get().split("\\s+");
            long total = Arrays.stream(words)
                    .filter(s -> !s.contains("cpu"))
                    .mapToLong(Long::parseLong)
                    .sum();
            long idle = Long.parseLong(words[4]) + Long.parseLong(words[5]);
            return ResourceUsage.builder()
                    .usage(total - idle)
                    .idle(idle)
                    .total(total).build();
        } catch (IOException e) {
            log.error("[LinuxInfo] Failed to read CPU usage from /proc/stat", e);
            return ResourceUsage.empty();
        }
    }

    /**
     * Get all physical nic limit.
     * @param nics All nic path
     * @param bitRateUnit Bit rate unit
     * @return Total nic limit
     */
    public static double getTotalNicLimit(List<String> nics, BitRateUnit bitRateUnit) {
        List<NetworkIF> networkIFs = OshiUtil.getNetworkIFs();
        return bitRateUnit.convert(networkIFs.stream()
                .filter(intf -> nics.contains(intf.getName()))
                .mapToDouble(iface -> (double) iface.getSpeed()).sum(), BitRateUnit.Megabit);
    }


    /**
     * Get all physical nic usage.
     * @param nics All nic path
     * @param type Nic's usage type:  transport, receive
     * @param bitRateUnit Bit rate unit
     * @return Total nic usage
     */
    public static double getTotalNicUsage(List<String> nics, NICUsageType type, BitRateUnit bitRateUnit) {
        double totalUsage = 0;
        BitRateUnit byteUnit = BitRateUnit.Byte;
        List<NetworkIF> networkIFs = OshiUtil.getNetworkIFs().stream()
                .filter(intf -> nics.contains(intf.getName())).toList();
        for (NetworkIF networkIF : networkIFs) {
            if (networkIF != null) {
                switch (type) {
                    case TX:
                        totalUsage += networkIF.getBytesSent();
                        break;
                    case RX:
                        totalUsage += networkIF.getBytesRecv();
                        break;
                    default:
                        // Handle unsupported NICUsageType
                        break;
                }
            }
        }
        return bitRateUnit.convert(totalUsage, byteUnit);
    }


    /**
     * Get paths of all usable physical nic.
     *
     * @return All usable physical nic paths.
     */
    public static List<String> getUsablePhysicalNICs() {
        List<NetworkIF> networkIFs = OshiUtil.getNetworkIFs();
        if (networkIFs != null) {
            return networkIFs.stream()
                    .filter(intf -> {
                        try {
                            return (intf.queryNetworkInterface().isUp())
                                    && !intf.queryNetworkInterface().isLoopback()
                                    && !intf.queryNetworkInterface().isVirtual()
                                    && !intf.queryNetworkInterface().isPointToPoint();
                        } catch (SocketException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .map(NetworkIF::getName)
                    .collect(Collectors.toList());
        } else {
            log.error("Failed to find NICs");
            return Collections.emptyList();
        }
    }

    /**
     * Check this VM has nic speed.
     * @return Whether the VM has nic speed
     */
    public static boolean checkHasNicSpeeds() {
        List<String> physicalNICs = getUsablePhysicalNICs();
        if (CollectionUtils.isEmpty(physicalNICs)) {
            return false;
        }
        double totalNicLimit = getTotalNicLimit(physicalNICs, BitRateUnit.Kilobit);
        return totalNicLimit > 0;
    }

    private static String readTrimStringFromFile(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8).trim();
    }

    private static long readLongFromFile(Path path) throws IOException {
        return Long.parseLong(readTrimStringFromFile(path));
    }

    @VisibleForTesting
    public static Object getMetrics() {
        return metrics;
    }

    @AllArgsConstructor
    public enum NICUsageType {
        // transport
        TX("/sys/class/net/%s/statistics/tx_bytes"),
        // receive
        RX("/sys/class/net/%s/statistics/rx_bytes");
        private final String template;
    }

    @Data
    @Builder
    public static class ResourceUsage {
        private final long total;
        private final long idle;
        private final long usage;

        public static ResourceUsage empty() {
            return ResourceUsage.builder()
                    .total(-1)
                    .idle(-1)
                    .usage(-1).build();
        }

        public boolean isEmpty() {
            return this.total == -1 && idle == -1 && usage == -1;
        }
    }
}

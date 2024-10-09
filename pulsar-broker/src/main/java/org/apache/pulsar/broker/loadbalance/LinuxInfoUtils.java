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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.BitRateUnit;

@Slf4j
public class LinuxInfoUtils {

    // CGROUP
    private static final String CGROUPS_CPU_USAGE_PATH = "/sys/fs/cgroup/cpu/cpuacct.usage";
    private static final String CGROUPS_CPU_LIMIT_QUOTA_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
    private static final String CGROUPS_CPU_LIMIT_PERIOD_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

    // proc states
    private static final String PROC_STAT_PATH = "/proc/stat";
    private static final String NIC_PATH = "/sys/class/net/";
    // NIC type
    private static final int ARPHRD_ETHER = 1;
    private static final String NIC_SPEED_TEMPLATE = "/sys/class/net/%s/speed";
    private static final long errLogPrintedFrequencyInReadingNicLimits = 1000;
    private static final AtomicLong failedCounterInReadingNicLimits = new AtomicLong(0);
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
     * Determine whether the VM has physical nic.
     * @param nicPath Nic path
     * @return whether The VM has physical nic.
     */
    private static boolean isPhysicalNic(Path nicPath) {
        try {
            if (nicPath.toString().contains("/virtual/")) {
                return false;
            }
            // Check the type to make sure it's ethernet (type "1")
            final Path nicTypePath = nicPath.resolve("type");
            if (!Files.exists(nicTypePath)) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to read NIC type, the expected linux type file does not exist."
                              + " nic_type_path={}", nicTypePath);
                }
               return false;
            }
            // wireless NICs don't report speed, ignore them.
            return Integer.parseInt(readTrimStringFromFile(nicTypePath)) == ARPHRD_ETHER;
        } catch (Exception ex) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to read NIC type. nic_path={}", nicPath, ex);
            }
            return false;
        }
    }

    /**
     * Determine whether nic is usable.
     * @param nicPath Nic path
     * @return whether nic is usable.
     */
    private static boolean isUsable(Path nicPath) {
        try {
            String operstate = readTrimStringFromFile(nicPath.resolve("operstate"));
            Operstate operState = Operstate.valueOf(operstate.toUpperCase(Locale.ROOT));
            switch (operState) {
                case UP:
                case UNKNOWN:
                case DORMANT:
                    return true;
                default:
                    return false;
            }
        } catch (Exception e) {
            log.warn("[LinuxInfo] Failed to read {} NIC operstate, the detail is: {}", nicPath, e.getMessage());
            // Read operstate got error.
            return false;
        }
    }

    /**
     * Get all physical nic limit.
     * @param nics All nic path
     * @param bitRateUnit Bit rate unit
     * @return Total nic limit
     */
    public static double getTotalNicLimit(List<String> nics, BitRateUnit bitRateUnit) {
        return bitRateUnit.convert(nics.stream().mapToDouble(nicPath -> {
            try {
                return readDoubleFromFile(getReplacedNICPath(NIC_SPEED_TEMPLATE, nicPath));
            } catch (IOException e) {
                // ERROR-level logs about NIC rate limiting reading failures are periodically printed but not
                // continuously printed
                if (failedCounterInReadingNicLimits.getAndIncrement() % errLogPrintedFrequencyInReadingNicLimits == 0) {
                    log.error("[LinuxInfo] Failed to get the nic limit of {}.", nicPath, e);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[LinuxInfo] Failed to get the nic limit of {}.", nicPath, e);
                    }
                }
                return 0d;
            }
        }).sum(), BitRateUnit.Megabit);
    }

    /**
     * Get all physical nic usage.
     * @param nics All nic path
     * @param type Nic's usage type:  transport, receive
     * @param bitRateUnit Bit rate unit
     * @return Total nic usage
     */
    public static double getTotalNicUsage(List<String> nics, NICUsageType type, BitRateUnit bitRateUnit) {
        return bitRateUnit.convert(nics.stream().mapToDouble(nic -> {
            try {
                return readDoubleFromFile(getReplacedNICPath(type.template, nic));
            } catch (IOException e) {
                log.error("[LinuxInfo] Failed to read {} bytes for NIC {} ", type, nic, e);
                return 0d;
            }
        }).sum(), BitRateUnit.Byte);
    }

    /**
     * Get paths of all usable physical nic.
     * @return All usable physical nic paths.
     */
    public static List<String> getUsablePhysicalNICs() {
        try (Stream<Path> stream = Files.list(Paths.get(NIC_PATH))) {
            return stream.filter(LinuxInfoUtils::isPhysicalNic)
                    .filter(LinuxInfoUtils::isUsable)
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("[LinuxInfo] Failed to find NICs", e);
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

    private static Path getReplacedNICPath(String template, String nic) {
        return Paths.get(String.format(template, nic));
    }

    private static String readTrimStringFromFile(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8).trim();
    }

    private static long readLongFromFile(Path path) throws IOException {
        return Long.parseLong(readTrimStringFromFile(path));
    }

    private static double readDoubleFromFile(Path path) throws IOException {
        return Double.parseDouble(readTrimStringFromFile(path));
    }

    /**
     * TLV IFLA_OPERSTATE
     * contains RFC2863 state of the interface in numeric representation:
     * See <a href="https://www.kernel.org/doc/Documentation/networking/operstates.txt">...</a>
     */
    enum Operstate {
        // Interface is in unknown state, neither driver nor userspace has set
        // operational state. Interface must be considered for user data as
        // setting operational state has not been implemented in every driver.
        UNKNOWN,
        // Interface is unable to transfer data on L1, f.e. ethernet is not
        // plugged or interface is ADMIN down.
        DOWN,
        // Interfaces stacked on an interface that is IF_OPER_DOWN show this
        // state (f.e. VLAN).
        LOWERLAYERDOWN,
        // Interface is L1 up, but waiting for an external event, f.e. for a
        // protocol to establish. (802.1X)
        DORMANT,
        // Interface is operational up and can be used.
        UP
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

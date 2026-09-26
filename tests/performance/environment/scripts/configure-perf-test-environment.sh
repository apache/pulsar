#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Configures the host for performance testing and restores it afterwards.
#
#   install  installs TuneD and the performance-testing profile (Debian based distros),
#            disables TuneD's dynamic tuning, limits the size of Docker's container logs
#            and leaves the TuneD daemon stopped and disabled
#   start    checks that the host is on AC power and has disk space, stops daemons that
#            would throttle or retune the host, activates the performance-testing TuneD
#            profile and skips the Gradle task that applies the same kernel settings in
#            ~/.gradle/gradle.properties of the user running sudo
#   stop     switches TuneD to a balanced profile that allows power saving, stops TuneD,
#            starts the daemons stopped by "start" again and removes the Gradle property
set -euo pipefail

PERF_PROFILE="performance-testing"
PERF_PROFILE_DIR="/etc/tuned/${PERF_PROFILE}"
RESTORE_PROFILE="${RESTORE_PROFILE:-balanced}"
TUNED_SERVICE="tuned.service"
TUNED_MAIN_CONFIG="/etc/tuned/tuned-main.conf"
THERMALD_SERVICE="thermald.service"
SYSTEM76_POWER_SERVICE="com.system76.PowerDaemon.service"
DOCKER_SERVICE="docker.service"
DOCKER_DAEMON_CONFIG="/etc/docker/daemon.json"
# Applied unless daemon.json already sets them
DOCKER_LOG_OPTIONS='{"max-size": "100m", "max-file": "3"}'
# BookKeeper's diskUsageWarnThreshold, bookies switch to read-only mode at 95 % by default
DISK_USAGE_WARNING_PERCENT=90
# The performance-testing profile applies the kernel settings of the
# :tests:integration:tuneKernelPerfEvents task, so "start" skips the task in the Gradle
# properties of the user who runs the tests
GRADLE_SKIP_PROPERTY="inttest.asyncprofiler.skipPerfEventTuning"
GRADLE_PROPERTIES_BEGIN="# BEGIN added by configure-perf-test-environment.sh start, removed by stop"
GRADLE_PROPERTIES_END="# END added by configure-perf-test-environment.sh start"

usage() {
    echo "Usage: sudo $0 install|start|stop" >&2
}

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: Run this script as root: sudo $0 ${*}" >&2
    exit 1
fi

os_id() {
    (
        # shellcheck disable=SC1091
        . /etc/os-release
        echo "${ID:-}"
    )
}

is_debian_based() {
    (
        # shellcheck disable=SC1091
        . /etc/os-release
        [[ " ${ID:-} ${ID_LIKE:-} " == *" debian "* ]]
    )
}

service_exists() {
    [[ -n "$(systemctl list-unit-files --no-legend "$1" 2>/dev/null)" ]]
}

stop_service_if_exists() {
    if service_exists "$1"; then
        echo "Stopping $1"
        systemctl stop "$1"
    fi
}

start_service_if_exists() {
    if service_exists "$1"; then
        echo "Starting $1"
        systemctl start "$1"
    fi
}

stop_distro_specific_services() {
    case "$(os_id)" in
        pop)
            # system76-power manages CPU governors and power profiles and would
            # override the TuneD settings
            stop_service_if_exists "${SYSTEM76_POWER_SERVICE}"
            ;;
    esac
}

start_distro_specific_services() {
    case "$(os_id)" in
        pop)
            start_service_if_exists "${SYSTEM76_POWER_SERVICE}"
            ;;
    esac
}

start_tuned_if_not_running() {
    if ! systemctl is-active --quiet "${TUNED_SERVICE}"; then
        echo "Starting ${TUNED_SERVICE}"
        systemctl start "${TUNED_SERVICE}"
    fi
}

verify_tuned_profile() {
    local active
    active="$(tuned-adm active)"
    echo "${active}"
    if [[ "${active}" != "Current active profile: ${PERF_PROFILE}" ]]; then
        echo "ERROR: Expected TuneD profile ${PERF_PROFILE} to be active." >&2
        exit 1
    fi
    if ! tuned-adm verify; then
        echo "ERROR: TuneD profile verification failed." >&2
        echo "Recent TuneD log:" >&2
        tail -n 50 /var/log/tuned/tuned.log >&2 || true
        exit 1
    fi
}

# tuned-adm verify doesn't check the intel_pstate settings
verify_turbo_disabled() {
    local no_turbo="/sys/devices/system/cpu/intel_pstate/no_turbo"
    if [[ -e "${no_turbo}" && "$(cat "${no_turbo}")" != "1" ]]; then
        echo "ERROR: Turbo is enabled (${no_turbo} is $(cat "${no_turbo}"))." >&2
        exit 1
    fi
}

disable_tuned_dynamic_tuning() {
    # Dynamic tuning changes settings based on the load while tests run. Newer TuneD
    # versions disable it by default.
    echo "Disabling TuneD dynamic tuning in ${TUNED_MAIN_CONFIG}"
    if grep -q -E '^[[:space:]]*#?[[:space:]]*dynamic_tuning[[:space:]]*=' "${TUNED_MAIN_CONFIG}"; then
        sed -i -E 's/^[[:space:]]*#?[[:space:]]*dynamic_tuning[[:space:]]*=.*/dynamic_tuning = 0/' \
            "${TUNED_MAIN_CONFIG}"
    else
        echo "dynamic_tuning = 0" >>"${TUNED_MAIN_CONFIG}"
    fi
}

# Limits the size of the container logs, keeping the other settings of daemon.json. The
# log driver has to support "docker logs", since the launcher saves the container logs
# through the Docker API.
configure_docker_logging() {
    if ! service_exists "${DOCKER_SERVICE}"; then
        echo "Docker isn't installed, skipping its logging configuration"
        return
    fi

    local current updated driver tmp validation
    if [[ -s "${DOCKER_DAEMON_CONFIG}" ]]; then
        current="$(cat "${DOCKER_DAEMON_CONFIG}")"
    else
        current='{}'
    fi
    if ! driver="$(jq -r '."log-driver" // "json-file"' <<<"${current}")"; then
        echo "ERROR: ${DOCKER_DAEMON_CONFIG} isn't valid JSON." >&2
        exit 1
    fi
    case "${driver}" in
        json-file | local) ;;
        *)
            echo "WARNING: ${DOCKER_DAEMON_CONFIG} uses the ${driver} log driver, not limiting the log size." >&2
            return
            ;;
    esac
    # Settings already in daemon.json take precedence
    updated="$(jq --argjson options "${DOCKER_LOG_OPTIONS}" '{"log-opts": $options} * .' <<<"${current}")"
    if [[ -e "${DOCKER_DAEMON_CONFIG}" && "$(jq -S . <<<"${current}")" == "$(jq -S . <<<"${updated}")" ]]; then
        echo "Docker logging is already configured in ${DOCKER_DAEMON_CONFIG}"
        return
    fi

    echo "Limiting the size of Docker's container logs in ${DOCKER_DAEMON_CONFIG}"
    mkdir -p "$(dirname "${DOCKER_DAEMON_CONFIG}")"
    tmp="$(mktemp "${DOCKER_DAEMON_CONFIG}.XXXXXX")"
    echo "${updated}" >"${tmp}"
    if dockerd --help 2>/dev/null | grep -q -- '--validate' \
        && ! validation="$(dockerd --validate --config-file "${tmp}" 2>&1)"; then
        rm -f "${tmp}"
        echo "${validation}" >&2
        echo "ERROR: Docker rejected the updated configuration, ${DOCKER_DAEMON_CONFIG} wasn't changed." >&2
        exit 1
    fi
    if [[ -e "${DOCKER_DAEMON_CONFIG}" ]]; then
        chmod --reference="${DOCKER_DAEMON_CONFIG}" "${tmp}"
        chown --reference="${DOCKER_DAEMON_CONFIG}" "${tmp}"
    else
        chmod 644 "${tmp}"
    fi
    mv "${tmp}" "${DOCKER_DAEMON_CONFIG}"
    jq 'with_entries(select(.key | startswith("log-")))' "${DOCKER_DAEMON_CONFIG}"

    # The log settings apply to containers created after Docker has been restarted
    if systemctl is-active --quiet "${DOCKER_SERVICE}"; then
        if [[ -z "$(docker ps -q)" ]]; then
            echo "Restarting Docker to apply the logging configuration"
            systemctl restart "${DOCKER_SERVICE}"
        else
            echo "WARNING: Containers are running, so Docker wasn't restarted. Apply the logging" \
                "configuration with: sudo systemctl restart docker" >&2
        fi
    fi
}

# Writes the performance-testing TuneD profile, which includes latency-performance
write_perf_profile() {
    echo "Creating TuneD profile ${PERF_PROFILE} in ${PERF_PROFILE_DIR}"
    mkdir -p "${PERF_PROFILE_DIR}"

    cat >"${PERF_PROFILE_DIR}/tuned.conf" <<'EOF'
[main]
include=latency-performance

# latency-performance keeps turbo enabled. Turbo frequencies depend on the CPU's
# temperature and power budget, which vary between runs, so run at the base
# frequency. With min_perf_pct=100 from latency-performance, the frequency is
# fixed at the base frequency. no_turbo applies to intel_pstate, the boost file
# below to the other cpufreq drivers.
[cpu]
no_turbo=1

[sysctl]
vm.swappiness=1
kernel.numa_balancing=0

# Kyber throttles requests to meet its latency targets, which adds a feedback
# loop on top of the fsync'd BookKeeper journal writes
[disk]
elevator=none

# Missing files are skipped
[sysfs]
/sys/devices/system/cpu/cpufreq/boost=0
# Fans and power limits of the platform firmware
/sys/firmware/acpi/platform_profile=performance
# Disable NVMe Autonomous Power State Transitions, which add the exit latency of a
# power state to the first request after the drive has been idle
/sys/class/nvme/nvme*/power/pm_qos_latency_tolerance_us=0

[script]
script=${i:PROFILE_DIR}/dirty-pages.sh

# Profiling and Transparent Huge Pages settings, left in place when the profile
# is deactivated
[profiling_and_thp]
type=script
script=${i:PROFILE_DIR}/profiling-and-thp.sh
EOF

    cat >"${PERF_PROFILE_DIR}/dirty-pages.sh" <<'EOF'
#!/bin/sh
set -eu

case "${1:-}" in
    start|reload)
        # Some Linux distros configure fixed dirty-page limits, which TuneD applies
        # again from the system sysctl config after the latency-performance
        # sysctls. Setting the ratios makes the kernel reset the byte limits to 0
        # (writing 0 to the byte limits directly is rejected with EINVAL).
        sysctl -q -w vm.dirty_ratio=10
        sysctl -q -w vm.dirty_background_ratio=3
        ;;
esac
EOF

    cat >"${PERF_PROFILE_DIR}/profiling-and-thp.sh" <<'EOF'
#!/bin/sh
set -eu

case "${1:-}" in
    start|reload)
        # Allow profiling with perf events (for example async-profiler) and
        # resolving kernel symbols
        echo 1 >/proc/sys/kernel/perf_event_paranoid
        echo 0 >/proc/sys/kernel/kptr_restrict
        echo 1024 >/proc/sys/kernel/perf_event_max_stack
        echo 2048 >/proc/sys/kernel/perf_event_mlock_kb
        # The NMI watchdog takes up a hardware performance counter
        echo 0 >/proc/sys/kernel/nmi_watchdog
        # The BPF syscall gate that jonoffcpu's off-CPU collector needs. The write
        # fails when the value is 1, which can't be changed until the next boot.
        { echo 0 >/proc/sys/kernel/unprivileged_bpf_disabled; } 2>/dev/null || true

        # Optimize for -XX:+UseTransparentHugePages. With defrag=madvise, the
        # madvised JVM heap is compacted into huge pages when it's touched, so that
        # -XX:+AlwaysPreTouch gets huge pages at startup. With defer, it would depend
        # on the memory fragmentation, and khugepaged would collapse them later.
        echo madvise >/sys/kernel/mm/transparent_hugepage/enabled
        echo advise >/sys/kernel/mm/transparent_hugepage/shmem_enabled
        echo madvise >/sys/kernel/mm/transparent_hugepage/defrag
        echo 1 >/sys/kernel/mm/transparent_hugepage/khugepaged/defrag
        ;;
esac
EOF

    chmod 755 "${PERF_PROFILE_DIR}/dirty-pages.sh" "${PERF_PROFILE_DIR}/profiling-and-thp.sh"
}

print_perf_settings() {
    echo
    echo "Kernel settings:"
    sysctl \
        vm.swappiness \
        vm.dirty_ratio \
        vm.dirty_bytes \
        vm.dirty_background_ratio \
        vm.dirty_background_bytes \
        kernel.numa_balancing \
        kernel.nmi_watchdog \
        kernel.unprivileged_bpf_disabled \
        kernel.perf_event_paranoid \
        kernel.kptr_restrict \
        kernel.perf_event_max_stack \
        kernel.perf_event_mlock_kb

    echo
    echo "CPU frequency settings:"
    local f
    for f in /sys/devices/system/cpu/intel_pstate/no_turbo /sys/devices/system/cpu/intel_pstate/min_perf_pct \
        /sys/devices/system/cpu/cpufreq/boost /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq \
        /sys/firmware/acpi/platform_profile; do
        if [[ -e "${f}" ]]; then
            echo "${f}: $(cat "${f}")"
        fi
    done

    echo
    echo "Transparent Huge Pages settings:"
    for f in enabled shmem_enabled defrag khugepaged/defrag; do
        echo "${f}: $(cat "/sys/kernel/mm/transparent_hugepage/${f}")"
    done
    echo
}

install_environment() {
    if ! is_debian_based; then
        echo "ERROR: install supports only Debian based Linux distributions." >&2
        exit 1
    fi

    echo "Installing TuneD"
    apt-get update
    apt-get install -y tuned jq

    configure_docker_logging
    disable_tuned_dynamic_tuning
    # Restart so that TuneD reads its main configuration again
    systemctl restart "${TUNED_SERVICE}"
    write_perf_profile
    activate_perf_profile

    echo
    echo "Switching TuneD to the ${RESTORE_PROFILE} profile and disabling the daemon"
    tuned-adm profile "${RESTORE_PROFILE}"
    systemctl disable --now "${TUNED_SERVICE}"

    echo
    echo "Installation completed. Run \"sudo $0 start\" before performance testing."
}

check_perf_profile_installed() {
    if ! command -v tuned-adm >/dev/null || [[ ! -d "/etc/tuned/${PERF_PROFILE}" ]]; then
        echo "ERROR: TuneD or the ${PERF_PROFILE} profile is missing. Run \"sudo $0 install\" first." >&2
        exit 1
    fi
}

check_ac_power() {
    # Laptops run with lower power limits on battery
    local supply has_battery=false on_ac=false
    for supply in /sys/class/power_supply/*; do
        case "$(cat "${supply}/type" 2>/dev/null)" in
            Battery) has_battery=true ;;
            Mains | USB)
                if [[ "$(cat "${supply}/online" 2>/dev/null)" == "1" ]]; then
                    on_ac=true
                fi
                ;;
        esac
    done
    if [[ "${has_battery}" == true && "${on_ac}" == false ]]; then
        echo "ERROR: The host is running on battery. Connect it to AC power." >&2
        exit 1
    fi
}

check_disk_space() {
    local docker_root usage
    docker_root="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)"
    docker_root="${docker_root:-/var/lib/docker}"
    if [[ ! -d "${docker_root}" ]]; then
        return
    fi
    usage="$(df --output=pcent "${docker_root}" | tail -n 1 | tr -d ' %')"
    if ((usage >= DISK_USAGE_WARNING_PERCENT)); then
        echo "WARNING: The disk of ${docker_root} is ${usage} % full. BookKeeper bookies switch to" \
            "read-only mode when the disk is 95 % full." >&2
    fi
}

stop_thermald() {
    # thermald throttles the CPU before it reaches its thermal limits
    stop_service_if_exists "${THERMALD_SERVICE}"
}

# gradle.properties in the default Gradle user home of the user who runs this script with sudo
gradle_properties_file() {
    local user="${SUDO_USER:-}"
    if [[ -z "${user}" || "${user}" == "root" ]]; then
        return 1
    fi
    echo "$(getent passwd "${user}" | cut -d: -f6)/.gradle/gradle.properties"
}

delete_gradle_properties_block() {
    # --follow-symlinks keeps a gradle.properties that is a symlink to a dotfiles repository
    sed -i --follow-symlinks "/^${GRADLE_PROPERTIES_BEGIN}\$/,/^${GRADLE_PROPERTIES_END}\$/d" "$1"
}

# The property is appended to the end of the file: the last value of a property in the file
# takes effect, so this overrides a value set earlier in the file, and the earlier value takes
# effect again when "stop" removes the block
add_gradle_properties() {
    local file user group
    if ! file="$(gradle_properties_file)"; then
        echo "WARNING: Not run with sudo by a user, so ${GRADLE_SKIP_PROPERTY} isn't set in the" \
            "user's Gradle properties. Pass -P${GRADLE_SKIP_PROPERTY}=true to Gradle." >&2
        return
    fi
    user="${SUDO_USER}"
    group="$(id -gn "${user}")"
    if [[ -f "${file}" ]]; then
        delete_gradle_properties_block "${file}"
        if [[ -s "${file}" && -n "$(tail -c 1 "${file}")" ]]; then
            echo >>"${file}"
        fi
    else
        if [[ ! -d "$(dirname "${file}")" ]]; then
            command install -d -o "${user}" -g "${group}" "$(dirname "${file}")"
        fi
        command install -m 644 -o "${user}" -g "${group}" /dev/null "${file}"
    fi
    echo "Setting ${GRADLE_SKIP_PROPERTY}=true in ${file}"
    cat >>"${file}" <<EOF
${GRADLE_PROPERTIES_BEGIN}
${GRADLE_SKIP_PROPERTY}=true
${GRADLE_PROPERTIES_END}
EOF
}

remove_gradle_properties() {
    local file
    if ! file="$(gradle_properties_file)"; then
        return
    fi
    if [[ -f "${file}" ]] && grep -q -F -x "${GRADLE_PROPERTIES_BEGIN}" "${file}"; then
        echo "Removing ${GRADLE_SKIP_PROPERTY} from ${file}"
        delete_gradle_properties_block "${file}"
    fi
}

activate_perf_profile() {
    start_tuned_if_not_running
    echo "Activating TuneD profile ${PERF_PROFILE}"
    tuned-adm profile "${PERF_PROFILE}"
    print_perf_settings
    verify_tuned_profile
    verify_turbo_disabled
}

start() {
    check_perf_profile_installed
    check_ac_power
    check_disk_space
    stop_thermald
    stop_distro_specific_services
    activate_perf_profile
    add_gradle_properties

    echo
    echo "Performance testing environment is active. Run \"sudo $0 stop\" when done."
}

restore_profile_and_stop_tuned() {
    if ! command -v tuned-adm >/dev/null; then
        return
    fi
    # TuneD has to be running to switch profiles. Switching also makes sure that
    # the performance-testing profile won't be applied when TuneD starts next time.
    start_tuned_if_not_running
    echo "Activating TuneD profile ${RESTORE_PROFILE}"
    tuned-adm profile "${RESTORE_PROFILE}"
    tuned-adm active
    echo "Stopping ${TUNED_SERVICE}"
    systemctl stop "${TUNED_SERVICE}"
}

start_thermald() {
    start_service_if_exists "${THERMALD_SERVICE}"
}

stop() {
    restore_profile_and_stop_tuned
    start_distro_specific_services
    start_thermald
    remove_gradle_properties

    echo
    echo "Performance testing environment settings are no longer enforced."
}

case "${1:-}" in
    install) install_environment ;;
    start) start ;;
    stop) stop ;;
    *)
        usage
        exit 1
        ;;
esac

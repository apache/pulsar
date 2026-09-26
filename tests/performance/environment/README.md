<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Performance testing environment

Results of a run depend on the state of the host: a CPU running at turbo frequencies slows down as it heats up and
throttles, and daemons that manage power change CPU and device settings during a run. The scripts in `scripts/`
configure a Linux host for consistent results across runs with a [TuneD](https://tuned-project.org/) profile, and
restore a configuration that allows power saving afterwards. The TuneD daemon only runs between `start` and `stop`.

| Command | What it does |
|---|---|
| `install` | Installs TuneD and `jq` (Debian based distributions), disables TuneD's dynamic tuning, installs the `performance-testing` TuneD profile, limits the size of Docker's container logs and leaves the TuneD daemon disabled. Run once, and again after the profile changes. |
| `start` | Checks that the host is on AC power and warns when Docker's disk is 90 % full, stops `thermald` (and `com.system76.PowerDaemon.service` on Pop!_OS), activates and verifies the `performance-testing` profile and skips the `:tests:integration:tuneKernelPerfEvents` task in `~/.gradle/gradle.properties`. |
| `stop` | Switches TuneD to the `balanced` profile, stops TuneD, starts the stopped daemons again and removes the Gradle property. |

All commands run as root: `sudo scripts/configure-perf-test-environment.sh start`.

## What the profile changes

The `performance-testing` profile includes TuneD's `latency-performance` profile (performance CPU governor, CPU idle
states limited to C1, `min_perf_pct=100`) and adds:

- **Turbo disabled**, so that the CPU runs at a fixed base frequency. Turbo frequencies depend on the temperature and
  the power budget of the CPU, which vary between runs and within a run. Absolute throughput is lower than with
  turbo, but comparisons between runs are more reliable, and cooling down between runs matters much less.
- `vm.swappiness=1`, NUMA balancing disabled and the dirty page limits of `latency-performance` (10 % and 3 %).
- The `none` I/O scheduler, the `performance` ACPI platform profile (fans and power limits of laptops) and NVMe
  Autonomous Power State Transitions disabled.
- Settings for profiling and for `-XX:+UseTransparentHugePages`: the perf event and BPF limits, the NMI watchdog
  disabled and Transparent Huge Pages in `madvise` mode with `defrag=madvise`.

`stop` restores the previous values, except the profiling and Transparent Huge Pages settings, which stay in place.

## Setup

Install the profile from this directory:

```sh
sudo scripts/configure-perf-test-environment.sh install
```

`install` restarts Docker to apply the log size limit when no containers are running. Otherwise it prints how to
restart Docker.

### Running start and stop without a password

To run `start` and `stop` from scripts, allow them in sudoers. Copy the script to a location that only root can
write first: a sudoers rule for a script that your user can edit lets anything running as your user run any command
as root.

```sh
sudo install -o root -g root -m 755 scripts/configure-perf-test-environment.sh /usr/local/sbin/
```

Create the sudoers rule. `visudo -cf` only checks the syntax, a sudoers file with a syntax error can stop `sudo`
from working. `install` sets the ownership and mode that sudo requires.

```sh
rule="$USER ALL=(root) NOPASSWD: /usr/local/sbin/configure-perf-test-environment.sh start, \
/usr/local/sbin/configure-perf-test-environment.sh stop"
tmp="$(mktemp)"
echo "$rule" >"$tmp"
visudo -cf "$tmp" && sudo install -o root -g root -m 0440 "$tmp" /etc/sudoers.d/perf-test-environment
rm "$tmp"
```

The rule takes effect immediately. Check it:

```sh
sudo -k
sudo -l
sudo -n /usr/local/sbin/configure-perf-test-environment.sh start
```

`sudo -n` fails instead of asking for a password, so the last command only works when the rule matches.

Notes:

- sudo ignores files in `/etc/sudoers.d` whose names contain a `.` or end with `~`.
- Copy the script to `/usr/local/sbin` again after it changes. `install` still asks for a password:
  `sudo /usr/local/sbin/configure-perf-test-environment.sh install`.
- sudo resets the environment, so `RESTORE_PROFILE` doesn't pass through, and `stop` switches to `balanced`.

To remove the rule:

```sh
sudo rm /etc/sudoers.d/perf-test-environment /usr/local/sbin/configure-perf-test-environment.sh
```

## Running tests

From the root of the repository:

```sh
# Before running tests
sudo /usr/local/sbin/configure-perf-test-environment.sh start

# Then run the tests
./gradlew :tests:performance:launcher:run \
  --args='--config tests/performance/scenarios/iot-telemetry-local.yaml'

# After running tests
sudo /usr/local/sbin/configure-perf-test-environment.sh stop
```

Without the sudoers rule, run `tests/performance/environment/scripts/configure-perf-test-environment.sh` instead.
See [the performance testing guide](../README.md) for the scenarios and the `profile` task.

Before a run:

- Keep the disk that holds Docker's data less than 90 % full. BookKeeper bookies switch to read-only mode when the
  disk is 95 % full.
- Close applications that use the CPU, such as browsers and IDEs.

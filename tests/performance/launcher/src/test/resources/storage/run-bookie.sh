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

set -euo pipefail

if [[ "${STORAGE_FAULTS:-false}" != "true" ]]; then
    exec /pulsar/bin/run-bookie-original.sh "$@"
fi

# This wrapper runs once at container startup, not at supervisor bookie restarts.
# It requires only this container's /dev/fuse and SYS_ADMIN, never privileged mode.
if [[ "$(id -u)" != "0" ]]; then
    echo 'Storage faults require container root for isolated mounts' >&2
    exit 1
fi
for path in /storage /storage/backing /storage/journal /storage/ledgers /storage/control \
        /storage/control/mode /storage/control/events /storage/control/fuse.log; do
    if [[ -L "$path" ]]; then
        echo "Refusing symlink at fixed storage path: $path" >&2
        exit 1
    fi
done
mkdir -p /storage/backing /storage/journal /storage/ledgers /storage/control

# All changes are in the container's private mount namespace. Make propagation
# private before creating mounts; never bind, remount, freeze, or fill host paths.
mount --make-rprivate /
for path in /storage/backing /storage/journal /storage/ledgers; do
    if mountpoint -q "$path"; then
        echo "Refusing an existing mount at fixed storage path: $path" >&2
        exit 1
    fi
done
mount -t tmpfs -o size=128m,mode=0777,nosuid,nodev,noexec tmpfs /storage/backing
mount -t tmpfs -o size=128m,mode=0777,nosuid,nodev,noexec tmpfs /storage/ledgers
printf 'healthy\n' > /storage/control/mode
: > /storage/control/events
chmod 0755 /storage/control
chmod 0644 /storage/control/mode /storage/control/events

/usr/local/bin/journal-faultfs > /storage/control/fuse.log 2>&1 &
fuse_pid=$!
# A failed startup must not leave the daemon running. After exec, container
# lifecycle owns it and supervisor may freely stop/start only the bookie.
trap 'kill "$fuse_pid" 2>/dev/null || true' EXIT
# Bound even an unresponsive FUSE getattr, not just the retry count.
if ! timeout 10 bash -c '
    while kill -0 "$1" 2>/dev/null; do
        if mountpoint -q /storage/journal && stat /storage/journal > /dev/null 2>&1; then
            exit 0
        fi
        sleep 0.1
    done
    exit 1
' storage-mount-ready "$fuse_pid"; then
    echo 'Journal FUSE mount failed or was not ready within 10 seconds; inspect /storage/control/fuse.log' >&2
    exit 1
fi

# The test supplies BookKeeper journal/ledger settings through the existing
# entrypoint's environment configuration; unrelated containers use its defaults.
trap - EXIT
exec /pulsar/bin/run-bookie-original.sh "$@"

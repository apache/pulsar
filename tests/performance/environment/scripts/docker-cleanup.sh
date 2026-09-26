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

# Frees the Docker disk space that test runs and image builds use up:
#
# - removes stopped containers, unused networks and dangling images
# - removes the Pulsar images (apachepulsar/pulsar, apachepulsar/java-test-image and
#   apachepulsar/pulsar-test-latest-version), which the Gradle tasks build again from the
#   build cache. Each -Pdocker.tag leaves images of about 750 MB behind, which pruning
#   doesn't remove.
# - removes images without a tag, such as the previous version of a base image after a newer
#   one has been pulled (amazoncorretto:<none>). Pruning doesn't remove them, since they
#   still have a registry digest.
# - removes build cache beyond 5 GB
set -euo pipefail

DOCKER_ORGANIZATION="${DOCKER_ORGANIZATION:-apachepulsar}"
PULSAR_IMAGE_REPOSITORIES=(pulsar java-test-image pulsar-test-latest-version)
BUILD_CACHE_RESERVED_SPACE="5GB"

usage() {
    echo "Usage: $0 [--dry-run]" >&2
    echo "  --dry-run  print what would be removed without removing anything" >&2
}

dry_run=false
for arg in "$@"; do
    case "${arg}" in
        --dry-run) dry_run=true ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done

run() {
    echo "+ $*"
    if [[ "${dry_run}" == false ]]; then
        "$@"
    fi
}

pulsar_images() {
    local repository
    for repository in "${PULSAR_IMAGE_REPOSITORIES[@]}"; do
        docker images --format '{{.Repository}}:{{.Tag}}' "${DOCKER_ORGANIZATION}/${repository}"
    done | grep -v ':<none>$' || true
}

# Images of a repository whose tag has moved to a newer image
untagged_images() {
    docker images --format '{{.ID}} {{.Repository}}:{{.Tag}}' | awk '$2 ~ /:<none>$/ && $2 != "<none>:<none>"'
}

# Removes the images given as "description" lines on stdin, the first word being the image
remove_images() {
    local description="$1" lines
    mapfile -t lines
    if ((${#lines[@]} == 0)); then
        echo "No ${description} to remove"
        return
    fi
    echo "Removing ${#lines[@]} ${description}:"
    printf '  %s\n' "${lines[@]}"
    if [[ "${dry_run}" == false ]] \
        && ! docker rmi "${lines[@]%% *}" >/dev/null; then
        echo "WARNING: Some ${description} weren't removed, since containers or other images use them." >&2
    fi
}

main() {
    if ! docker info >/dev/null 2>&1; then
        echo "ERROR: Can't connect to Docker. Add your user to the docker group or run with sudo." >&2
        exit 1
    fi
    docker system df
    echo
    run docker system prune --force
    pulsar_images | remove_images "Pulsar images"
    untagged_images | remove_images "untagged images"
    run docker builder prune --force --reserved-space "${BUILD_CACHE_RESERVED_SPACE}"
    echo
    docker system df
}

main

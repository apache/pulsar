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

if [ -z "$PULSAR_CHART_HOME" ]; then
    echo "error: PULSAR_CHART_HOME should be initialized"
    exit 1
fi

OUTPUT=${PULSAR_CHART_HOME}/output
OUTPUT_BIN=${OUTPUT}/bin
KUBECTL_VERSION=1.14.3
KUBECTL_BIN=$OUTPUT_BIN/kubectl
HELM_BIN=$OUTPUT_BIN/helm
HELM_VERSION=3.0.1
KIND_VERSION=0.6.1
KIND_BIN=$OUTPUT_BIN/kind
CR_BIN=$OUTPUT_BIN/cr
CR_VERSION=1.0.0-beta.1

test -d "$OUTPUT_BIN" || mkdir -p "$OUTPUT_BIN"

ARCH=""
hack::discoverArch() {
  ARCH=$(uname -m)
  case $ARCH in
    x86) ARCH="386";;
    x86_64) ARCH="amd64";;
    i686) ARCH="386";;
    i386) ARCH="386";;
  esac
}

hack::discoverArch
OS=$(echo `uname`|tr '[:upper:]' '[:lower:]')

function hack::verify_kubectl() {
    if test -x "$KUBECTL_BIN"; then
        [[ "$($KUBECTL_BIN version --client --short | grep -o -E '[0-9]+\.[0-9]+\.[0-9]+')" == "$KUBECTL_VERSION" ]]
        return
    fi
    return 1
}

function hack::ensure_kubectl() {
    if hack::verify_kubectl; then
        return 0
    fi
    echo "Installing kubectl v$KUBECTL_VERSION..."
    tmpfile=$(mktemp)
    trap "test -f $tmpfile && rm $tmpfile" RETURN
    curl --retry 10 -L -o $tmpfile https://storage.googleapis.com/kubernetes-release/release/v${KUBECTL_VERSION}/bin/${OS}/${ARCH}/kubectl
    mv $tmpfile $KUBECTL_BIN
    chmod +x $KUBECTL_BIN
}

function hack::verify_helm() {
    if test -x "$HELM_BIN"; then
        local v=$($HELM_BIN version --short --client | grep -o -E '[0-9]+\.[0-9]+\.[0-9]+')
        [[ "$v" == "$HELM_VERSION" ]]
        return
    fi
    return 1
}

function hack::ensure_helm() {
    if hack::verify_helm; then
        return 0
    fi
    local HELM_URL=https://get.helm.sh/helm-v${HELM_VERSION}-${OS}-${ARCH}.tar.gz
    curl --retry 10 -L -s "$HELM_URL" | tar --strip-components 1 -C $OUTPUT_BIN -zxf - ${OS}-${ARCH}/helm
}

function hack::verify_kind() {
    if test -x "$KIND_BIN"; then
        [[ "$($KIND_BIN --version 2>&1 | cut -d ' ' -f 3)" == "$KIND_VERSION" ]]
        return
    fi
    return 1
}

function hack::ensure_kind() {
    if hack::verify_kind; then
        return 0
    fi
    echo "Installing kind v$KIND_VERSION..."
    tmpfile=$(mktemp)
    trap "test -f $tmpfile && rm $tmpfile" RETURN
    curl --retry 10 -L -o $tmpfile https://github.com/kubernetes-sigs/kind/releases/download/v${KIND_VERSION}/kind-$(uname)-amd64
    mv $tmpfile $KIND_BIN
    chmod +x $KIND_BIN
}

# hack::version_ge "$v1" "$v2" checks whether "v1" is greater or equal to "v2"
function hack::version_ge() {
    [ "$(printf '%s\n' "$1" "$2" | sort -V | head -n1)" = "$2" ]
}

function hack::verify_cr() {
    if test -x "$CR_BIN"; then
        return
    fi
    return 1
}

function hack::ensure_cr() {
    if hack::verify_cr; then
        $CR_BIN version
        return 0
    fi
    echo "Installing chart-releaser ${CR_VERSION} ..."
    tmpfile=$(mktemp)
    trap "test -f $tmpfile && rm $tmpfile" RETURN
    echo curl --retry 10 -L -o $tmpfile https://github.com/helm/chart-releaser/releases/download/v${CR_VERSION}/chart-releaser_${CR_VERSION}_${OS}_${ARCH}.tar.gz
    curl --retry 10 -L -o $tmpfile https://github.com/helm/chart-releaser/releases/download/v${CR_VERSION}/chart-releaser_${CR_VERSION}_${OS}_${ARCH}.tar.gz
    mv $tmpfile $CR_BIN
    chmod +x $CR_BIN
    $CR_BIN version
}

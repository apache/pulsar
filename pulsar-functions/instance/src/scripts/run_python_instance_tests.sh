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

set -o errexit
set -o nounset
set -o pipefail

CUR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PULSAR_HOME="$( cd "$CUR_DIR/../../../../" >/dev/null && pwd )"

PYTHON_BIN=${PYTHON_BIN:-python3}

# Keep these aligned with the Python dependencies installed in docker/pulsar/Dockerfile: the tests
# should run against the same versions the Python instance runs with in production. pulsar-client's
# "all" extra brings in apache-bookkeeper-client, fastavro, prometheus_client and ratelimit, which
# the instance imports.
PULSAR_CLIENT_PYTHON_VERSION=${PULSAR_CLIENT_PYTHON_VERSION:-3.13.0}
PYTHON_GRPCIO_VERSION=${PYTHON_GRPCIO_VERSION:-1.78.0}
PYTHON_PROTOBUF_VERSION=${PYTHON_PROTOBUF_VERSION:-6.33.6}

# Set SKIP_PYTHON_DEPS=true to run against an environment you have prepared yourself. Otherwise the
# dependencies are installed into whatever ${PYTHON_BIN} resolves to, so run this inside a
# virtualenv unless you want them on your system interpreter.
if [[ "${SKIP_PYTHON_DEPS:-false}" != "true" ]]; then
  ${PYTHON_BIN} -m pip install \
    mock \
    "pulsar-client[all]==${PULSAR_CLIENT_PYTHON_VERSION}" \
    "grpcio==${PYTHON_GRPCIO_VERSION}" \
    "protobuf==${PYTHON_PROTOBUF_VERSION}"
fi

TEST_DIR="${PULSAR_HOME}/pulsar-functions/instance/src/test/python"
export PULSAR_HOME
export PYTHONPATH="${PULSAR_HOME}/pulsar-functions/instance/src/main/python"

# Each test module runs in its own interpreter. They cannot share one: test_python_instance replaces
# prometheus_client with a mock in sys.modules, which breaks test_python_instance_main when it later
# imports the real one, and the two modules also register the same Prometheus metrics, so a shared
# registry rejects the duplicates.
failed_modules=()
for test_file in "${TEST_DIR}"/test_*.py; do
  module_name="$(basename "${test_file}" .py)"
  echo "=== Running ${module_name} ==="
  if ! (cd "${TEST_DIR}" && ${PYTHON_BIN} -m unittest -v "${module_name}"); then
    failed_modules+=("${module_name}")
  fi
done

if [[ ${#failed_modules[@]} -gt 0 ]]; then
  echo "Failed test modules: ${failed_modules[*]}" >&2
  exit 1
fi

echo "All Python instance test modules passed"

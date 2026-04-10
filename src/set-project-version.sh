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

set -e

if [ $# -eq 0 ]; then
    echo "Required argument with new project version"
    exit 1
fi

NEW_VERSION=$1

# Go to top level project directory
SRC_DIR=$(dirname "$0")
ROOT_DIR=$(cd "${SRC_DIR}/.."; pwd)
TERRAFORM_DIR=${ROOT_DIR}/deployment/terraform-ansible
pushd ${ROOT_DIR}

# Get the current version from the version catalog
OLD_VERSION=$(grep -oP '^pulsar = "\K[^"]+' gradle/libs.versions.toml)

echo "Changing version from $OLD_VERSION to $NEW_VERSION"

# Update the version catalog (single source of truth)
sed -i "s/^pulsar = \"${OLD_VERSION}\"/pulsar = \"${NEW_VERSION}\"/" gradle/libs.versions.toml

# Set terraform ansible deployment pulsar version
if [ -f "${TERRAFORM_DIR}/deploy-pulsar.yaml" ]; then
    sed -i -e "s/${OLD_VERSION}/${NEW_VERSION}/g" ${TERRAFORM_DIR}/deploy-pulsar.yaml
fi

popd

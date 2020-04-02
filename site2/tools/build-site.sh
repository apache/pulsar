#!/bin/bash
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

ROOT_DIR=$(git rev-parse --show-toplevel)
VERSION=`${ROOT_DIR}/src/get-project-version.py`

set -x -e

${ROOT_DIR}/site2/tools/generate-api-docs.sh
cd ${ROOT_DIR}/site2/website
yarn
yarn write-translations
yarn run crowdin-upload
yarn run crowdin-download

# TODO: remove this after figuring out why crowdin removed code tab when generating translated files
# https://github.com/apache/pulsar/issues/5816
cp versioned_docs/version-2.4.2/functions-develop.md translated_docs/zh-CN/version-2.4.2/functions-develop.md
cp versioned_docs/version-2.5.0/functions-develop.md translated_docs/zh-CN/version-2.5.0/functions-develop.md
cp versioned_docs/version-2.5.0/io-overview.md translated_docs/zh-CN/version-2.5.0/io-overview.md
cp versioned_docs/version-2.4.2/functions-develop.md translated_docs/ja/version-2.4.2/functions-develop.md
cp versioned_docs/version-2.5.0/functions-develop.md translated_docs/ja/version-2.5.0/functions-develop.md
cp versioned_docs/version-2.5.0/io-overview.md translated_docs/ja/version-2.5.0/io-overview.md
cp versioned_docs/version-2.4.2/functions-develop.md translated_docs/fr/version-2.4.2/functions-develop.md
cp versioned_docs/version-2.5.0/functions-develop.md translated_docs/fr/version-2.5.0/functions-develop.md
cp versioned_docs/version-2.5.0/io-overview.md translated_docs/fr/version-2.5.0/io-overview.md
cp versioned_docs/version-2.4.2/functions-develop.md translated_docs/zh-TW/version-2.4.2/functions-develop.md
cp versioned_docs/version-2.5.0/functions-develop.md translated_docs/zh-TW/version-2.5.0/functions-develop.md
cp versioned_docs/version-2.5.0/io-overview.md translated_docs/zh-TW/version-2.5.0/io-overview.md

yarn build

node ./scripts/replace.js
node ./scripts/split-swagger-by-version.js

# Generate document for command line tools.
${ROOT_DIR}/site2/tools/pulsar-admin-doc-gen.sh
cd ${ROOT_DIR}/site2/website

rm -rf ${ROOT_DIR}/generated-site/content
mkdir -p ${ROOT_DIR}/generated-site/content
cp -R ${ROOT_DIR}/generated-site/api ${ROOT_DIR}/generated-site/content
cp -R ./build/pulsar/* ${ROOT_DIR}/generated-site/content
cp -R ${ROOT_DIR}/generated-site/tools ${ROOT_DIR}/generated-site/content

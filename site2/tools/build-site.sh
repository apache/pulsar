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

ROOT_DIR=$(git rev-parse --show-toplevel)
VERSION=$(${ROOT_DIR}/src/get-project-version.py)

function workaround_crowdin_problem_by_copying_files() {
  # TODO: remove this after figuring out why crowdin removed code tab when generating translated files
  # https://github.com/apache/pulsar/issues/5816
  node scripts/fix-tab.js 
}


set -x -e

export NODE_OPTIONS="--max-old-space-size=4096" #increase to 4GB, default is 512MB
${ROOT_DIR}/site2/tools/generate-api-docs.sh
cd ${ROOT_DIR}/site2/website
yarn
yarn write-translations

# The crowdin upload and download take a long time to run, and have resulted in timeouts. In order to ensure that the
# website is still able to get published, we only run the download and upload if current hour is 0-5.
# This leads to executing crowdin-upload and crowdin-download once per day when website build is scheduled
# to run with cron expression '0 */6 * * *'
CURRENT_HOUR=$(date +%H)
if [[ "$CROWDIN_DOCUSAURUS_API_KEY" != "UNSET" || $CURRENT_HOUR -lt 6 ]]; then
  # upload only if environment variable CROWDIN_UPLOAD=1 is set
  if [[ "$CROWDIN_UPLOAD" == "1" ]]; then
    yarn run crowdin-upload
  fi
  yarn run crowdin-download

  workaround_crowdin_problem_by_copying_files
else
  # set English as the only language to build in this case
  cat > languages.js <<'EOF'
const languages = [
{
  enabled: true,
  name: 'English',
  tag: 'en',
}];
module.exports = languages;
EOF
fi

yarn build

node ./scripts/replace.js
node ./scripts/split-swagger-by-version.js

# Generate document for command line tools.
${ROOT_DIR}/site2/tools/pulsar-admin-doc-gen.sh
${ROOT_DIR}/site2/tools/pulsar-client-doc-gen.sh
${ROOT_DIR}/site2/tools/pulsar-perf-doc-gen.sh
${ROOT_DIR}/site2/tools/pulsar-doc-gen.sh
cd ${ROOT_DIR}/site2/website

rm -rf ${ROOT_DIR}/generated-site/content
mkdir -p ${ROOT_DIR}/generated-site/content
cp -R ${ROOT_DIR}/generated-site/api ${ROOT_DIR}/generated-site/content
cp -R ./build/pulsar/* ${ROOT_DIR}/generated-site/content
cp -R ${ROOT_DIR}/generated-site/tools ${ROOT_DIR}/generated-site/content
cp -R ${ROOT_DIR}/site2/website/static/swagger/* ${ROOT_DIR}/generated-site/content/swagger/

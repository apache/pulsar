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

if [ "$TRAVIS_BRANCH" != "master" ]
then
  echo "This commit was made against the $TRAVIS_BRANCH and not the master! No deploy!"
  exit 0
fi

ROOT_DIR=$(git rev-parse --show-toplevel)

ORIGIN_REPO=$(git remote show origin | grep 'Push  URL' | awk -F// '{print $NF}')
echo "ORIGIN_REPO: $ORIGIN_REPO"

GENERATED_SITE_DIR=$ROOT_DIR/generated-site

(
  cd $ROOT_DIR

  REVISION=$(git rev-parse --short HEAD)

  cd $GENERATED_SITE_DIR

  git init
  git config user.name "Pulsar Site Updater"
  git config user.email "dev@pulsar.incubator.apache.org"

  git remote add upstream "https://$GH_TOKEN@$ORIGIN_REPO"
  git fetch upstream
  git reset upstream/asf-site

  touch .

  git add -A .
  git commit -m "Updated site at revision $REVISION"
  git push -q upstream HEAD:asf-site
)

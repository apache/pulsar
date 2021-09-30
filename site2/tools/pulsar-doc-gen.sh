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
VERSION=`${ROOT_DIR}/src/get-project-version.py`
DEST_DIR=$ROOT_DIR/generated-site

cd $ROOT_DIR

mkdir -p $DEST_DIR/tools/pulsar/${VERSION}
mkdir -p $DEST_DIR/tools/pulsar/${VERSION}/node_modules
mkdir -p $ROOT_DIR/site2/website/brodocs/documents

$ROOT_DIR/bin/pulsar broker -g > $ROOT_DIR/site2/website/brodocs/documents/broker.md
$ROOT_DIR/bin/pulsar broker-tool gen-doc > $ROOT_DIR/site2/website/brodocs/documents/broker-tool.md
$ROOT_DIR/bin/pulsar compact-topic -t tmp -g > $ROOT_DIR/site2/website/brodocs/documents/compact-topic.md
$ROOT_DIR/bin/pulsar tokens gen-doc > $ROOT_DIR/site2/website/brodocs/documents/tokens.md
$ROOT_DIR/bin/pulsar proxy -g > $ROOT_DIR/site2/website/brodocs/documents/proxy.md
$ROOT_DIR/bin/pulsar functions-worker -g > $ROOT_DIR/site2/website/brodocs/documents/functions-worker.md
$ROOT_DIR/bin/pulsar standalone -g > $ROOT_DIR/site2/website/brodocs/documents/standalone.md
$ROOT_DIR/bin/pulsar initialize-cluster-metadata -cs cs -uw uw -zk zk -c c -g > $ROOT_DIR/site2/website/brodocs/documents/initialize-cluster-metadata.md
$ROOT_DIR/bin/pulsar delete-cluster-metadata -zk zk -g > $ROOT_DIR/site2/website/brodocs/documents/delete-cluster-metadata.md
$ROOT_DIR/bin/pulsar initialize-transaction-coordinator-metadata -cs cs -c c -g > $ROOT_DIR/site2/website/brodocs/documents/initialize-transaction-coordinator-metadata.md
$ROOT_DIR/bin/pulsar initialize-namespace -cs cs -c c -g demo > $ROOT_DIR/site2/website/brodocs/documents/initialize-namespace.md
$ROOT_DIR/bin/pulsar version -g > $ROOT_DIR/site2/website/brodocs/documents/version.md
$ROOT_DIR/bin/pulsar discovery -g > $ROOT_DIR/site2/website/brodocs/documents/discovery.md
$ROOT_DIR/bin/pulsar websocket -g > $ROOT_DIR/site2/website/brodocs/documents/websocket.md

cd $ROOT_DIR/site2/website/brodocs
cp pulsar-manifest.json manifest.json
node brodoc.js

cp index.html $DEST_DIR/tools/pulsar/${VERSION}/
cp navData.js stylesheet.css $DEST_DIR/tools/pulsar/${VERSION}/
cp scroll.js tabvisibility.js $DEST_DIR/tools/pulsar/${VERSION}/
cp favicon.ico $DEST_DIR/tools/pulsar/${VERSION}/
mkdir -p $DEST_DIR/tools/pulsar/${VERSION}/node_modules/bootstrap/dist/css
cp -r $ROOT_DIR/site2/website/node_modules/bootstrap/dist/css/bootstrap.min.css $DEST_DIR/tools/pulsar/${VERSION}/node_modules/bootstrap/dist/css
mkdir -p $DEST_DIR/tools/pulsar/${VERSION}/node_modules/font-awesome/css
cp -r $ROOT_DIR/site2/website/node_modules/font-awesome/css/font-awesome.min.css $DEST_DIR/tools/pulsar/${VERSION}/node_modules/font-awesome/css
mkdir -p $DEST_DIR/tools/pulsar/${VERSION}/node_modules/highlight.js/styles
cp -r $ROOT_DIR/site2/website/node_modules/highlight.js/styles/default.css $DEST_DIR/tools/pulsar/${VERSION}/node_modules/highlight.js/styles
mkdir -p $DEST_DIR/tools/pulsar/${VERSION}/node_modules/jquery/dist
cp -r $ROOT_DIR/site2/website/node_modules/jquery/dist/jquery.min.js $DEST_DIR/tools/pulsar/${VERSION}/node_modules/jquery/dist/
mkdir -p $DEST_DIR/tools/pulsar/${VERSION}/node_modules/jquery.scrollto
cp -r $ROOT_DIR/site2/website/node_modules/jquery.scrollto/jquery.scrollTo.min.js $DEST_DIR/tools/pulsar/${VERSION}/node_modules/jquery.scrollto



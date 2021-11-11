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

mkdir -p $DEST_DIR/tools/pulsar-admin/${VERSION}
mkdir -p $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules
mkdir -p $ROOT_DIR/site2/website/brodocs/documents

$ROOT_DIR/bin/pulsar-admin documents generate broker-stats > $ROOT_DIR/site2/website/brodocs/documents/broker-stats.md
$ROOT_DIR/bin/pulsar-admin documents generate brokers > $ROOT_DIR/site2/website/brodocs/documents/brokers.md
$ROOT_DIR/bin/pulsar-admin documents generate clusters > $ROOT_DIR/site2/website/brodocs/documents/clusters.md
$ROOT_DIR/bin/pulsar-admin documents generate functions > $ROOT_DIR/site2/website/brodocs/documents/functions.md
$ROOT_DIR/bin/pulsar-admin documents generate functions-worker > $ROOT_DIR/site2/website/brodocs/documents/functions-worker.md
$ROOT_DIR/bin/pulsar-admin documents generate namespaces > $ROOT_DIR/site2/website/brodocs/documents/namespaces.md
$ROOT_DIR/bin/pulsar-admin documents generate ns-isolation-policy > $ROOT_DIR/site2/website/brodocs/documents/ns-isolation-policy.md
$ROOT_DIR/bin/pulsar-admin documents generate sources > $ROOT_DIR/site2/website/brodocs/documents/sources.md
$ROOT_DIR/bin/pulsar-admin documents generate sinks > $ROOT_DIR/site2/website/brodocs/documents/sinks.md
$ROOT_DIR/bin/pulsar-admin documents generate topics > $ROOT_DIR/site2/website/brodocs/documents/topics.md
$ROOT_DIR/bin/pulsar-admin documents generate proxy-stats > $ROOT_DIR/site2/website/brodocs/documents/proxy-stats.md
$ROOT_DIR/bin/pulsar-admin documents generate resourcegroups > $ROOT_DIR/site2/website/brodocs/documents/resourcegroups.md
$ROOT_DIR/bin/pulsar-admin documents generate transactions > $ROOT_DIR/site2/website/brodocs/documents/transactions.md
$ROOT_DIR/bin/pulsar-admin documents generate tenants > $ROOT_DIR/site2/website/brodocs/documents/tenants.md
$ROOT_DIR/bin/pulsar-admin documents generate resource-quotas > $ROOT_DIR/site2/website/brodocs/documents/resource-quotas.md
$ROOT_DIR/bin/pulsar-admin documents generate schemas > $ROOT_DIR/site2/website/brodocs/documents/schemas.md
$ROOT_DIR/bin/pulsar-admin documents generate packages > $ROOT_DIR/site2/website/brodocs/documents/packages.md

cd $ROOT_DIR/site2/website/brodocs
cp pulsar-admin-manifest.json manifest.json
node brodoc.js

cp index.html $DEST_DIR/tools/pulsar-admin/${VERSION}/
cp navData.js stylesheet.css $DEST_DIR/tools/pulsar-admin/${VERSION}/
cp scroll.js tabvisibility.js $DEST_DIR/tools/pulsar-admin/${VERSION}/
cp favicon.ico $DEST_DIR/tools/pulsar-admin/${VERSION}/
mkdir -p $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules/bootstrap/dist/css
cp -r $ROOT_DIR/site2/website/node_modules/bootstrap/dist/css/bootstrap.min.css $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules/bootstrap/dist/css
mkdir -p $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules/font-awesome/css
cp -r $ROOT_DIR/site2/website/node_modules/font-awesome/css/font-awesome.min.css $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules/font-awesome/css
mkdir -p $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules/highlight.js/styles
cp -r $ROOT_DIR/site2/website/node_modules/highlight.js/styles/default.css $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules/highlight.js/styles
mkdir -p $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules/jquery/dist
cp -r $ROOT_DIR/site2/website/node_modules/jquery/dist/jquery.min.js $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules/jquery/dist/
mkdir -p $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules/jquery.scrollto
cp -r $ROOT_DIR/site2/website/node_modules/jquery.scrollto/jquery.scrollTo.min.js $DEST_DIR/tools/pulsar-admin/${VERSION}/node_modules/jquery.scrollto



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

# Sets Grafana up in its volume, /var/lib/grafana, when the volume is empty: the VictoriaMetrics data source and the
# dashboards of the Apache Pulsar Helm chart, the list of its values.yaml at
# https://github.com/apache/pulsar-helm-chart/blob/6053d295e037f050cbd41675a53b7b8ce66baf2d/charts/pulsar/values.yaml#L2154-L2192
# pinned to a commit of the dashboards' repository. Each dashboard gets an annotation query that shows the annotations
# that the runs add, such as the end of the warmup. A volume that is set up already is left as it is; remove the
# volume to set Grafana up again.
set -eu

GRAFANA=/var/lib/grafana
MARKER="$GRAFANA/.pulsar-performance-setup"
DASHBOARDS_URL=https://raw.githubusercontent.com/lhotari/pulsar-grafana-dashboards/75a9067965a5e8c326d8a9cfb5a3a0ae9775e00f/pulsar
DASHBOARDS="bookkeeper-compaction bookkeeper broker-cache-by-broker broker-cache connector-sink connector-source
functions jvm load-balancing messaging namespace node offloader overview-by-broker overview proxy sockets topic
zookeeper"
# The annotations of the runs, which have the tag pulsar-performance, with the run's cluster and what happened in their
# text; the dashboards have no annotations of their own
ANNOTATIONS='"annotations":{"list":[{"datasource":{"type":"grafana","uid":"-- Grafana --"},"enable":true,'\
'"iconColor":"orange","name":"Performance test runs","target":{"type":"tags","tags":["pulsar-performance"],'\
'"matchAny":false,"limit":100}}]},'

if [ -f "$MARKER" ]; then
  echo "Grafana is set up already in its volume"
  exit 0
fi

echo "Setting Grafana up in its volume"
downloads=$(mktemp -d)
for dashboard in $DASHBOARDS; do
  file="$downloads/$dashboard.json"
  curl -fsSL --retry 3 -o "$file" "$DASHBOARDS_URL/$dashboard.json"
  if grep -q '"annotations"' "$file"; then
    echo "The dashboard $dashboard has annotations of its own, so it doesn't show the runs' annotations"
  elif [ "$(head -c1 "$file")" = "{" ]; then
    # Inserted as the dashboard object's first member
    { printf '{%s' "$ANNOTATIONS"; tail -c +2 "$file"; } > "$file.tmp"
    mv "$file.tmp" "$file"
  else
    echo "The dashboard $dashboard isn't a JSON object" >&2
    exit 1
  fi
done
mkdir -p "$GRAFANA/provisioning/datasources" "$GRAFANA/provisioning/dashboards" "$GRAFANA/provisioning/plugins" \
  "$GRAFANA/provisioning/alerting" "$GRAFANA/dashboards/pulsar"
cp /setup/provisioning/datasources/*.yaml "$GRAFANA/provisioning/datasources/"
cp /setup/provisioning/dashboards/*.yaml "$GRAFANA/provisioning/dashboards/"
mv "$downloads"/*.json "$GRAFANA/dashboards/pulsar/"
rmdir "$downloads"
# Grafana runs as the user grafana, 472
chown -R 472:0 "$GRAFANA"
# Last, so that a setup that failed runs again on the next start
touch "$MARKER"
echo "Installed $(echo "$DASHBOARDS" | wc -w) dashboards"

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Metrics

During a run, VictoriaMetrics scrapes the metrics of the cluster's brokers, bookies and ZooKeeper, and Grafana shows
them on the dashboards of the [Apache Pulsar Helm chart](https://github.com/apache/pulsar-helm-chart), with the run's
events marked. Their data stays in Docker volumes across runs, so that the runs can be analyzed and compared afterwards.
At the end of a run, the launcher renders panels of the dashboards as PNG images into the run report.

Docker Compose runs both in Docker containers from [`metrics/compose.yaml`](../metrics/compose.yaml), as the project
`pulsar-performance-metrics`, so the host needs the Docker Compose plugin. Docker Desktop has it; on Linux, install
Docker's `docker-compose-plugin` package, and check it with `docker compose version`.

## Running the metrics stack

Start VictoriaMetrics and Grafana, with Grafana's image renderer, in the background:

```bash
./gradlew :tests:performance:metrics:up
```

```
Starting VictoriaMetrics and Grafana; the first start pulls the images, and sets up Grafana
...
Grafana: http://127.0.0.1:3000/ (log in as admin, password pulsar-performance, to edit; viewing needs no login)
VictoriaMetrics: http://127.0.0.1:8428/vmui/?#/metrics (browse the metrics), http://127.0.0.1:8428/vmui (PromQL queries)
Runs send their metrics here. It keeps running, also across restarts of Docker, until ./gradlew :tests:performance:metrics:down stops it; the metrics and Grafana's settings stay in the Docker volumes pulsar-performance-victoriametrics-data and pulsar-performance-grafana-data
```

It keeps running while you run scenarios, and to analyze the runs afterwards, until you stop it:

```bash
./gradlew :tests:performance:metrics:down
```

The tasks run `docker compose` with the project name and the compose file, which you can run yourself too, once the
network and the volumes exist, which `up` creates:

```bash
docker compose --file tests/performance/metrics/compose.yaml --project-name pulsar-performance-metrics up --detach --wait
docker compose --file tests/performance/metrics/compose.yaml --project-name pulsar-performance-metrics down
```

Grafana's user is `admin` with the password `pulsar-performance`, and viewing the dashboards needs no login.

Beyond Grafana's dashboards, VictoriaMetrics' web UI, vmui, shows any metric that it has collected:
<http://127.0.0.1:8428/vmui/?#/metrics> browses the metrics by name, and <http://127.0.0.1:8428/vmui> runs PromQL
queries and graphs their results.

When the stack runs on another machine, reach Grafana and VictoriaMetrics from your own machine through the SSH tunnel
that [Browsing the reports over HTTP](run-reports.md#browsing-the-reports-over-http) describes, which forwards their
ports together with the reports server's.

The first start pulls the images, about 1 GB, most of it the image renderer's browser, and sets Grafana up in its empty
volume: the VictoriaMetrics data source, the Prometheus data source plugin, which the slim Grafana image leaves out,
and the 19 dashboards of the Helm chart's `pulsar` dashboards, in the folder Pulsar, as
[`metrics/grafana/setup.sh`](../metrics/grafana/setup.sh) lists them, pinned to a commit of their repository. Each
dashboard gets an annotation query, Performance test runs, which shows the runs' events. A later start leaves the volume
as it is, including dashboards that you changed; remove the volume to set Grafana up again.

## What a run collects

A run collects metrics by default. When the metrics stack runs, the run uses it. When it doesn't, the run starts the
stack for itself, as `up` does, and stops it when the run ends; start the stack later to see the run in Grafana. `--no-metrics`, or `-Pperformance.metrics=false`, collects
none. A run whose metrics can't be collected, such as without the Docker Compose plugin, says so and goes on without
them.

The scenario's `metrics.intervalSeconds`, 5 by default, sets how often VictoriaMetrics scrapes, and the periods of the
broker's stats, which have to match it: the broker computes the stats in its metrics over each period and starts over,
so a scrape interval that is longer than the period misses periods, and a shorter one sees the same stats more than
once. The launcher sets these broker settings to the interval, unless the scenario's broker `env` sets them itself:

| Broker setting | What starts over at each period |
|---|---|
| `statsUpdateFrequencyInSecs` | The topic and namespace rates, and the latency summaries, which the stats update rotates |
| `statsUpdateInitialDelayInSecs` | The delay of the first stats update |
| `managedLedgerStatsPeriodSeconds` | The managed ledgers' stats, including their latency and entry size buckets |
| `managedLedgerPrometheusStatsLatencyRolloverSeconds` | The BookKeeper client's latency stats, which the broker exposes with `bookkeeperClientExposeStatsToPrometheus` |

The bookies' latency stats start over at the same kind of period, `prometheusStatsLatencyRolloverSeconds`, which the
launcher sets to the interval too, with `PULSAR_PREFIX_prometheusStatsLatencyRolloverSeconds` in the bookies'
environment, since their `bookkeeper.conf` doesn't have the setting.

The broker and the bookies use these periods also when no metrics are collected, so that runs with and without metrics are alike.
Scraping costs the broker some CPU time to write its metrics every interval, so compare runs that were made in the same
way.

VictoriaMetrics scrapes each broker's `/metrics` on port 8080, and each bookie's and ZooKeeper's on port 8000; the
configuration store serves no metrics. The containers join the network `pulsar-performance-metrics` while the run runs,
which VictoriaMetrics is on, and the run's scrape configuration is added to VictoriaMetrics and removed at the end,
after one more scrape. Every metric of the run has these labels, which the dashboards filter on:

| Label | Value |
|---|---|
| `cluster` | The run: the run directory's path in the reports root, such as `2026-09-27/master/iot-telemetry/09-27-12-00-00`, or its name when it's outside the reports root. The dashboards' cluster variable chooses the run |
| `exported_cluster` | The Pulsar cluster's name, `iot-<pid>`, which the metrics have as `cluster` |
| `job` | `broker`, `bookie` or `zookeeper` |
| `kubernetes_pod_name` | The container, such as `pulsar-broker-0`, which the dashboards call the instance or pod |
| `run_id` | The run's ID, as in `run-id.txt` |

## Events of a run

At the end of a run, the launcher adds its events to Grafana as annotations, which every dashboard shows as markers:

| Event | When |
|---|---|
| `gateways-started` | The gateways started: the producers publish, and the warmup starts |
| `warmup-finished` | The warmup finished, and the measurement starts |
| `gateways-finished` | The gateways finished: the producers have published every message |
| `applications-finished` | The applications finished: the consumers have received every message |

An annotation has the tags `pulsar-performance`, the run's `cluster` label and the event, and its text says the run
and the event. Grafana keeps the annotations in its database, in its volume, and never removes those added through its
API, so they stay with the metrics.

## Panels in the run report

The launcher renders these panels of the run, over the time that it was scraped and with its events marked, as PNG
images into `grafana-panels/` in the run directory, and the run report's Metrics section shows them. Grafana's image
renderer renders PNG only, not SVG. A panel's image links to the panel in Grafana, and the line below it links to the
panel and to the dashboard that it is on, both with the run's cluster and time range:

| Dashboard | Panels |
|---|---|
| Pulsar / Messaging | Publish rate, delivery rate, backlog, storage write latency |
| Pulsar / JVM, of the brokers | Heap memory, GC time, CPU |
| Pulsar / BookKeeper | Add entry latency, 99th percentile |

Rendering takes about 2 seconds a panel, four at a time. A panel that doesn't render is left out, and the run goes on
without the annotations and the panels when Grafana doesn't answer.

## Finding a run in Grafana

The launcher prints the link to the run on the Pulsar / Messaging dashboard, over the time that the run was scraped:

```
18:13:52 Metrics in Grafana: http://127.0.0.1:3000/d/EetmjdhnA/pulsar-messaging?orgId=1&var-cluster=2026-09-27%2Fmaster%2Fiot-telemetry%2F09-27-18-12-09&from=1790522548000&to=1790522632000
```

The run report's Metrics section has the same link. On the other dashboards, such as Pulsar / Topic, Pulsar / JVM and
Pulsar / BookKeeper, choose the run as the cluster, and its time range. The links use Grafana's base URL,
`performance.metrics.grafanaUrl`, so that they open where you reach Grafana.

## metrics.json

`metrics.json` in the run directory describes the run's metrics, for scripts and agents that query VictoriaMetrics
and Grafana directly:

| Key | Value |
|---|---|
| `cluster`, `selector` | The run's `cluster` label, and the label selector of its metrics, such as `{cluster="2026-09-27/master/iot-telemetry/09-27-12-00-00"}` |
| `runId`, `intervalSeconds` | The run's ID, and the scrape interval |
| `startEpochMs`, `endEpochMs` | The time that the run was scraped, the time range of its queries |
| `jobs`, `labels` | Each job's instances, such as `"broker": ["pulsar-broker-0"]`, and what each label of the metrics is |
| `events` | The run's events, each with its `name`, `description` and `epochMs` |
| `victoriaMetrics` | Its `url`, its Prometheus API, `prometheusApi`, its URL on the host that ran the run, `localUrl`, and its UI, `ui` |
| `grafana` | Its `url`, its URL on the host that ran the run, `localUrl`, the `user` and `password` of its API, its VictoriaMetrics data source, `dataSourceUid`, and the tag of the runs' annotations, `annotationTag` |
| `grafanaDashboard`, `dashboards`, `panels` | The link to the run on the Pulsar / Messaging dashboard, the dashboards of the report's panels over the run, and the rendered panels, each with its `title`, `file` and `url`, and the `dashboard` that it is on with its `dashboardUrl` over the run |

For example, the broker's publish rate over the run, with VictoriaMetrics' Prometheus API:

```bash
curl --get "$(jq -r .victoriaMetrics.prometheusApi metrics.json)query_range" \
  --data-urlencode "query=sum(pulsar_rate_in$(jq -r .selector metrics.json))" \
  --data-urlencode "start=$(jq -r '.startEpochMs / 1000' metrics.json)" \
  --data-urlencode "end=$(jq -r '.endEpochMs / 1000' metrics.json)" --data-urlencode step=5
```

Some panels stay empty, since they show what the scenarios don't run, such as proxies, functions and
connectors, or metrics of Kubernetes and of the node exporter.

## Rendering panels as images

Grafana renders a panel as a PNG image with its image renderer, which the stack runs, at `/render/d-solo/<dashboard
UID>/<slug>` with the panel's ID and the dashboard's variables:

```bash
curl -u admin:pulsar-performance -o publish-rate.png \
  'http://127.0.0.1:3000/render/d-solo/EetmjdhnA/pulsar-messaging?orgId=1&panelId=16&var-cluster=<cluster>&from=<start>&to=<end>&width=1000&height=400&tz=UTC'
```

`<cluster>` is the run's `cluster` label, URL-encoded, and `<start>` and `<end>` are the epoch milliseconds in
`metrics.json`. A panel's ID is in the dashboard's JSON, `GET /api/dashboards/uid/<dashboard UID>`. Grafana renders
PNG only, not SVG. `theme=light` renders the light theme, as the run report's panels are.

## Settings

These Gradle properties, on the command line with `-P` or in `~/.gradle/gradle.properties`, configure the stack and
the runs:

| Property | Default | Sets |
|---|---|---|
| `performance.metrics` | `true` | Whether runs collect metrics |
| `performance.metrics.bindAddress` | `127.0.0.1` | The address that VictoriaMetrics and Grafana are published on, as for the [reports server](run-reports.md#browsing-the-reports-over-http). `0.0.0.0` makes them available on the network, where anyone can view the dashboards and anyone who knows the fixed password can edit them, so do that only on a trusted network |
| `performance.metrics.grafanaUrl` | Grafana's port on the bind address | The URL that Grafana is reached at, which the links use, such as `http://192.168.1.123:3000/`. With the bind address `0.0.0.0`, the default is the address of the host's first network interface with an IPv4 address |

VictoriaMetrics keeps the metrics for a year, and its queries see a scrape 2 seconds after it, rather than its default
30 seconds, so that the panels rendered right after a run show its end. The stack uses the latest images of VictoriaMetrics, the slim Grafana
image and Grafana's image renderer; `docker pull` them to update them.

## Removing the data

The metrics, the dashboards and Grafana's settings are in the Docker volumes `pulsar-performance-victoriametrics-data`
and `pulsar-performance-grafana-data`. With the stack stopped, remove them to start over:

```bash
docker volume rm pulsar-performance-victoriametrics-data pulsar-performance-grafana-data
```

[`docker-cleanup.sh`](../environment/scripts/docker-cleanup.sh) leaves the volumes alone. It removes the network
`pulsar-performance-metrics` when no container uses it, which the stack or a run creates again when it starts.

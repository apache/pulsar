---
id: deploy-kubernetes
title: Overview
sidebar_label: "Kubernetes"
---

The Apache Pulsar [Helm chart](https://github.com/apache/pulsar-helm-chart) provides one of the most convenient ways to deploy and operate Pulsar on Kubernetes. With all the required components, the Helm chart is scalable and thus suitable for large-scale deployments.

The Apache Pulsar Helm chart contains all components to support the features and functions that Pulsar delivers. You can install and configure these components separately.

- Pulsar core components
  - ZooKeeper
  - Bookies
  - Brokers
  - Function workers
  - Proxies
- Control center
  - Pulsar Manager
  - Prometheus
  - Grafana


## What's Next?

* To get up and running with Pulsar Helm chart as fast as possible for Proof of Concept (PoC) in a **non-production** use case, see the [quick start guide](getting-started-helm.md). 

* To get up and running with Pulsar Helm chart in production under sustained load, see the [preparations](helm-prepare.md) and [deployment guide](helm-deploy.md).

* To upgrade your existing Pulsar Helm chart, see the [upgrade guide](helm-upgrade.md).

:::tip

* Templating in Helm is done through Golang's [text/template](https://golang.org/pkg/text/template/) and [sprig](https://godoc.org/github.com/Masterminds/sprig). For more information about how all the inner workings behave, check these documents:
  - [Functions and Pipelines](https://helm.sh/docs/chart_template_guide/functions_and_pipelines/)
  - [Subcharts and Globals](https://helm.sh/docs/chart_template_guide/subcharts_and_globals/)

* For additional information on developing with Helm, check [tips and tricks](https://helm.sh/docs/howto/charts_tips_and_tricks/) in the Helm repository.

:::
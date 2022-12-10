---
id: deploy-kubernetes
title: Overview
sidebar_label: "Kubernetes"
---

The Apache Pulsar Helm Chart provides one of the most convenient ways to deploy and operate Pulsar on Kubernetes. With all the required components, the Helm Chart is scalable and thus suitable for large-scale deployments.

The Apache Pulsar Helm Chart contains all components to support the features and functions that Pulsar delivers. You can install and configure these components separately. See [README](https://github.com/apache/pulsar-helm-chart#readme) for more details.

## What's Next?

* To get up and running with Pulsar Helm Chart as fast as possible for Proof of Concept (PoC) in a **non-production** use case, see the [Quick Start Guide](getting-started-helm.md). 

* To get up and running with Pulsar Helm Chart in production under sustained load, see the [preparations](helm-prepare.md) and [Deployment Guide](helm-deploy.md).

* To upgrade your existing Pulsar Helm Chart, see the [Upgrade Guide](helm-upgrade.md).

:::tip

* Templating in Helm is done through Golang's [text/template](https://golang.org/pkg/text/template/) and [sprig](https://godoc.org/github.com/Masterminds/sprig). For more information about how all the inner workings behave, check these documents:
  - [Functions and Pipelines](https://helm.sh/docs/chart_template_guide/functions_and_pipelines/)
  - [Subcharts and Globals](https://helm.sh/docs/chart_template_guide/subcharts_and_globals/)

* For additional information on developing with Helm, check [tips and tricks](https://helm.sh/docs/howto/charts_tips_and_tricks/) in the Helm repository.

:::
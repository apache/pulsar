---
id: version-2.6.3-helm-tools
title: Required tools for deploying Pulsar Helm Chart
sidebar_label: Required Tools
original_id: helm-tools
---

Before deploying Pulsar to your Kubernetes cluster, there are some tools you must have installed locally.

## kubectl

kubectl is the tool that talks to the Kubernetes API. kubectl 1.14 or higher is required and it needs to be compatible with your cluster ([+/- 1 minor release from your cluster](https://kubernetes.io/docs/tasks/tools/install-kubectl/#before-you-begin)).

To Install kubectl locally, follow the [Kubernetes documentation](https://kubernetes.io/docs/tasks/tools/install-kubectl/#install-kubectl).

The server version of kubectl cannot be obtained until we connect to a cluster.

## Helm

Helm is the package manager for Kubernetes. The Apache Pulsar Helm Chart is tested and supported with Helm v3.

### Get Helm

You can get Helm from the project's [releases page](https://github.com/helm/helm/releases), or follow other options under the official documentation of [installing Helm](https://helm.sh/docs/intro/install/).

### Next steps

Once kubectl and Helm are configured, you can configure your [Kubernetes cluster](helm-prepare.md).

## Additional information

### Templates

Templating in Helm is done through Golang's [text/template](https://golang.org/pkg/text/template/) and [sprig](https://godoc.org/github.com/Masterminds/sprig).

For more information about how all the inner workings behave, check these documents:

- [Functions and Pipelines](https://helm.sh/docs/chart_template_guide/functions_and_pipelines/)
- [Subcharts and Globals](https://helm.sh/docs/chart_template_guide/subcharts_and_globals/)

### Tips and tricks

For additional information on developing with Helm, check [tips and tricks section](https://helm.sh/docs/howto/charts_tips_and_tricks/) in the Helm repository.
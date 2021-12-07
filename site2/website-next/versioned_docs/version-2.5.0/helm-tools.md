---
id: helm-tools
title: Required tools for deploying Pulsar Helm Chart
sidebar_label: "Required Tools"
original_id: helm-tools
---

Before deploying Pulsar to your Kubernetes cluster, there are some tools you must have installed locally.

## kubectl

kubectl is the tool that talks to the Kubernetes API. kubectl 1.14 or higher is required and it needs to be compatible with your cluster ([+/- 1 minor release from your cluster](https://kubernetes.io/docs/tasks/tools/install-kubectl/#before-you-begin)).

[Install kubectl locally by following the Kubernetes documentation](https://kubernetes.io/docs/tasks/tools/install-kubectl/#install-kubectl).

The server version of kubectl cannot be obtained until we connect to a cluster. Proceed with setting up Helm.

## Helm

Helm is the package manager for Kubernetes. The Apache Pulsar Helm Chart is tested and supported with Helm v3.

### Get Helm

You can get Helm from the project's [releases page](https://github.com/helm/helm/releases), or follow other options under the official documentation of [installing Helm](https://helm.sh/docs/intro/install/).

### Next steps

Once kubectl and Helm are configured, you can continue to configuring your [Kubernetes cluster](helm-prepare).

## Additional information

### Templates

Templating in Helm is done via golang's [text/template](https://golang.org/pkg/text/template/) and [sprig](https://godoc.org/github.com/Masterminds/sprig).

Some information on how all the inner workings behave:

- [Functions and Pipelines](https://helm.sh/docs/chart_template_guide/functions_and_pipelines/)
- [Subcharts and Globals](https://helm.sh/docs/chart_template_guide/subcharts_and_globals/)

### Tips and tricks

Helm repository has some additional information on developing with Helm in it's [tips and tricks section](https://helm.sh/docs/howto/charts_tips_and_tricks/).
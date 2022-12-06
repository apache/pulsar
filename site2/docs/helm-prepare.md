---
id: helm-prepare
title: Prepare Kubernetes resources
sidebar_label: "Prepare"
---

For a fully functional Pulsar cluster, you need a few resources before deploying the Apache Pulsar Helm Chart. This section provides the information about the preparations you need to do before deploying the Pulsar Helm Chart.

## Prerequisites

Set up your environment by installing the required tools.

### Install kubectl

`kubectl` 1.18 or higher is the required tool that talks to the Kubernetes API. It needs to be compatible with your cluster ([+/- 1 minor release from your cluster](https://kubernetes.io/docs/tasks/tools/install-kubectl/#before-you-begin)). The server version of `kubectl` cannot be obtained until you connect to a cluster.

For the installation instructions, see the [Kubernetes documentation](https://kubernetes.io/docs/tasks/tools/install-kubectl/#install-kubectl).

### Install Helm

Helm is the package manager for Kubernetes. The Apache Pulsar Helm Chart is supported with Helm v3 (3.0.2 or higher).

You can get the Helm from the project's [releases page](https://github.com/helm/helm/releases), or follow other options under the official documentation of [installing Helm](https://helm.sh/docs/intro/install/).

## Create Kubernetes cluster

A Kubernetes cluster version 1.18 or higher is required for continuing the deployment. For details about how to create a Kubernetes cluster, see [Kubernetes documentation](https://kubernetes.io/docs/setup/production-environment/tools/).
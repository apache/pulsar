---
id: helm-install
title: Install Apache Pulsar using Helm
sidebar_label: "Install "
original_id: helm-install
---

Install Apache Pulsar on Kubernetes with the official Pulsar Helm chart.

## Requirements

In order to deploy Apache Pulsar on Kubernetes, the following are required.

1. kubectl 1.14 or higher, compatible with your cluster ([+/- 1 minor release from your cluster](https://kubernetes.io/docs/tasks/tools/install-kubectl/#before-you-begin))
2. Helm v3 (3.0.2 or higher)
3. A Kubernetes cluster, version 1.14 or higher.

## Environment setup

Before proceeding to deploying Pulsar, you need to prepare your environment.

### Tools

`helm` and `kubectl` need to be [installed on your computer](helm-tools).

## Cloud cluster preparation

> NOTE: Kubernetes 1.14 or higher is required, due to the usage of certain Kubernetes features.

Follow the instructions to create and connect to the Kubernetes cluster of your choice:

- [Google Kubernetes Engine](helm-prepare.md#google-kubernetes-engine)

## Deploy Pulsar

With the environment set up and configuration generated, you can now proceed to the [deployment of Pulsar](helm-deploy).

## Upgrade Pulsar

If you are upgrading an existing Kubernetes installation, follow the [upgrade documentation](helm-upgrade) instead.

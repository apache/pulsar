---
id: version-2.7.1-helm-install
title: Install Apache Pulsar using Helm
sidebar_label: Install
original_id: helm-install
---

Install Apache Pulsar on Kubernetes with the official Pulsar Helm chart.

## Requirements

To deploy Apache Pulsar on Kubernetes, the followings are required.

- kubectl 1.14 or higher, compatible with your cluster ([+/- 1 minor release from your cluster](https://kubernetes.io/docs/tasks/tools/install-kubectl/#before-you-begin))
- Helm v3 (3.0.2 or higher)
- A Kubernetes cluster, version 1.14 or higher

## Environment setup

Before deploying Pulsar, you need to prepare your environment.

### Tools

Install [`helm`](helm-tools.md) and [`kubectl`](helm-tools.md) on your computer.

## Cloud cluster preparation

> #### Note 
> Kubernetes 1.14 or higher is required.

To create and connect to the Kubernetes cluster, follow the instructions:

- [Google Kubernetes Engine](helm-prepare.md#google-kubernetes-engine)

## Pulsar deployment

Once the environment is set up and configuration is generated, you can now proceed to the [deployment of Pulsar](helm-deploy.md).

## Pulsar upgrade

To upgrade an existing Kubernetes installation, follow the [upgrade documentation](helm-upgrade.md).

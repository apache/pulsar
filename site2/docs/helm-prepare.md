---
id: helm-prepare
title: Prepare Kubernetes resources
sidebar_label: "Prepare"
---

For a fully functional Pulsar cluster, you need a few resources before deploying the Apache Pulsar Helm chart. This guide provides instructions to prepare the Kubernetes cluster before deploying the Pulsar Helm chart.

## Prerequisites

Set up your environment by installing the required tools.

### Install kubectl

`kubectl` 1.14 or higher is the required tool that talks to the Kubernetes API. It needs to be compatible with your cluster ([+/- 1 minor release from your cluster](https://kubernetes.io/docs/tasks/tools/install-kubectl/#before-you-begin)). The server version of `kubectl` cannot be obtained until you connect to a cluster.

For the installation instructions, see the [Kubernetes documentation](https://kubernetes.io/docs/tasks/tools/install-kubectl/#install-kubectl).

### Install Helm

Helm is the package manager for Kubernetes. The Apache Pulsar Helm Chart is supported with Helm v3 (3.0.2 or higher).

You can get the Helm from the project's [releases page](https://github.com/helm/helm/releases), or follow other options under the official documentation of [installing Helm](https://helm.sh/docs/intro/install/).


## Create Kubernetes cluster

A Kubernetes cluster version 1.14 or higher is required for continuing the deployment. To create a cluster, you can either use a script to automate the process or manually do it.

### Create a cluster manually 

To provision a Kubernetes cluster manually, follow the [GKE instructions](https://cloud.google.com/kubernetes-engine/docs/how-to/creating-a-cluster).

### Create a cluster using a bootstrap script

A [bootstrap script](https://github.com/streamnative/charts/tree/master/scripts/pulsar/gke_bootstrap_script.sh) has been created to automate much of the setup process for users on GCP/GKE.

The script can:
1. Create a new GKE cluster.
2. Allow the cluster to modify DNS (Domain Name Server) records.
3. Set up `kubectl`, and connect it to the cluster.


The script reads various parameters from environment variables and an argument `up` or `down` for bootstrap and clean-up respectively. Google Cloud SDK is a dependency of this script, so ensure it is [set up correctly](#connect-to-a-gke-cluster) for the script to work.

The following table describes all variables.

| **Variable** | **Description** | **Default value** |
| ------------ | --------------- | ----------------- |
| PROJECT      | ID of your GCP project | No default value. It requires to be set. |
| CLUSTER_NAME | Name of the GKE cluster | `pulsar-dev` |
| CONFDIR | Configuration directory to store Kubernetes configuration | ${HOME}/.config/streamnative |
| INT_NETWORK | IP space to use within this cluster | `default` |
| LOCAL_SSD_COUNT | Number of local SSD counts | 4 |
| MACHINE_TYPE | Type of machine to use for nodes | `n1-standard-4` |
| NUM_NODES | Number of nodes to be created in each of the cluster's zones | 4 |
| PREEMPTIBLE | Create nodes using preemptible VM instances in the new cluster. | false |
| REGION | Compute region for the cluster | `us-east1` |
| USE_LOCAL_SSD | Flag to create a cluster with local SSDs | false |
| ZONE | Compute zone for the cluster | `us-east1-b` |
| ZONE_EXTENSION | The extension (`a`, `b`, `c`) of the zone name of the cluster | `b` |
| EXTRA_CREATE_ARGS | Extra arguments passed to create command | |

Run the script, by passing in your desired parameters. It can work with the default parameters except for `PROJECT` which is required:

```bash
PROJECT=<gcloud project id> scripts/pulsar/gke_bootstrap_script.sh up
```

The script can also be used to clean up the created GKE resources.

```bash
PROJECT=<gcloud project id> scripts/pulsar/gke_bootstrap_script.sh down
```

#### Create a cluster with local SSDs

To install the Pulsar Helm chart using local persistent volumes, you need to create a GKE cluster with local SSDs by specifying `USE_LOCAL_SSD` to be `true` in the following command.

```bash
PROJECT=<gcloud project id> USE_LOCAL_SSD=true LOCAL_SSD_COUNT=<local-ssd-count> scripts/pulsar/gke_bootstrap_script.sh up
```
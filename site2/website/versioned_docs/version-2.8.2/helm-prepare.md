---
id: version-2.8.2-helm-prepare
title: Prepare Kubernetes resources
sidebar_label: Prepare
original_id: helm-prepare
---

For a fully functional Pulsar cluster, you need a few resources before deploying the Apache Pulsar Helm chart. The following provides instructions to prepare the Kubernetes cluster before deploying the Pulsar Helm chart.

- [Google Kubernetes Engine](#google-kubernetes-engine)
  - [Manual cluster creation](#manual-cluster-creation)
  - [Scripted cluster creation](#scripted-cluster-creation)
    - [Create cluster with local SSDs](#create-cluster-with-local-ssds)
- [Next Steps](#next-steps)

## Google Kubernetes Engine

To get started easier, a script is provided to create the cluster automatically. Alternatively, a cluster can be created manually as well.

- [Google Kubernetes Engine](#google-kubernetes-engine)
  - [Manual cluster creation](#manual-cluster-creation)
  - [Scripted cluster creation](#scripted-cluster-creation)
    - [Create cluster with local SSDs](#create-cluster-with-local-ssds)
- [Next Steps](#next-steps)

### Manual cluster creation

To provision a Kubernetes cluster manually, follow the [GKE instructions](https://cloud.google.com/kubernetes-engine/docs/how-to/creating-a-cluster).

Alternatively, you can use the [instructions](#scripted-cluster-creation) below to provision a GKE cluster as needed.

### Scripted cluster creation

A [bootstrap script](https://github.com/streamnative/charts/tree/master/scripts/pulsar/gke_bootstrap_script.sh) has been created to automate much of the setup process for users on GCP/GKE.

The script can:

1. Create a new GKE cluster.
2. Allow the cluster to modify DNS (Domain Name Server) records.
3. Setup `kubectl`, and connect it to the cluster.

Google Cloud SDK is a dependency of this script, so ensure it is [set up correctly](helm-tools.md#connect-to-a-gke-cluster) for the script to work.

The script reads various parameters from environment variables and an argument `up` or `down` for bootstrap and clean-up respectively.

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

#### Create cluster with local SSDs

To install a Pulsar Helm chart using local persistent volumes, you need to create a GKE cluster with local SSDs. You can do so Specifying the `USE_LOCAL_SSD` to be `true` in the following command to create a Pulsar cluster with local SSDs.

```
PROJECT=<gcloud project id> USE_LOCAL_SSD=true LOCAL_SSD_COUNT=<local-ssd-count> scripts/pulsar/gke_bootstrap_script.sh up
```
## Next Steps

Continue with the [installation of the chart](helm-deploy.md) once you have the cluster up and running.

---
id: version-2.5.0-helm-prepare
title: Preparing Kubernetes resources
sidebar_label: Prepare
original_id: helm-prepare
---

For a fully functional Pulsar cluster, you will need a few resources before deploying the Apache Pulsar Helm chart. The following provides instructions to prepare the Kubernetes cluster before deploying the Pulsar Helm chart.

- [Google Kubernetes Engine](#google-kubernetes-engine)

## Google Kubernetes Engine

To get started easier, a script is provided to automate the cluster creation. Alternatively, a cluster can be created manually as well.

- [Manual cluster creation](#manual-cluster-creation)
- [Scripted cluster creation](#scripted-cluster-creation)

### Manual cluster creation

To provision a Kubernetes cluster manually, follow the [GKE instructions](https://cloud.google.com/kubernetes-engine/docs/how-to/creating-a-cluster).

Alternatively you can use the [instructions](#scripted-cluster-creation) below to provision a GKE cluster as needed.

### Scripted cluster creation

A [bootstrap script](https://github.com/streamnative/charts/tree/master/scripts/pulsar/gke_bootstrap_script.sh) has been created to automate much of the setup process for users on GCP/GKE.

The script will:

1. Create a new GKE cluster.
2. Allow the cluster to modify DNS records.
3. Setup `kubectl`, and connect it to the cluster.

Google Cloud SDK is a dependency of this script, so make sure it's [set up correctly](helm-tools.md#connect-to-a-gke-cluster) in order for the script to work.

The script reads various parameters from environment variables and an argument `up` or `down` for bootstrap and clean-up respectively.

The table below describes all variables.

| **Variable** | **Description** | **Default value** |
| ------------ | --------------- | ----------------- |
| PROJECT      | The ID of your GCP project | No defaults, required to be set. |
| CLUSTER_NAME | Name of the GKE cluster | `pulsar-dev` |
| CONFDIR | Configuration directory to store kubernetes config | Defaults to ${HOME}/.config/streamnative |
| INT_NETWORK | The IP space to use within this cluster | `default` |
| LOCAL_SSD_COUNT | The number of local SSD counts | Defaults to 4 |
| MACHINE_TYPE | The type of machine to use for nodes | `n1-standard-4` |
| NUM_NODES | The number of nodes to be created in each of the cluster's zones | 4 |
| PREEMPTIBLE | Create nodes using preemptible VM instances in the new cluster. | false |
| REGION | Compute region for the cluster | `us-east1` |
| USE_LOCAL_SSD | Flag to create a cluster with local SSDs | Defaults to false |
| ZONE | Compute zone for the cluster | `us-east1-b` |
| ZONE_EXTENSION | The extension (`a`, `b`, `c`) of the zone name of the cluster | `b` |
| EXTRA_CREATE_ARGS | Extra arguments passed to create command | |

Run the script, by passing in your desired parameters. It can work with the default parameters except for `PROJECT` which is required:

```bash
PROJECT=<gcloud project id> scripts/pulsar/gke_bootstrap_script.sh up
```

The script can also be used to clean up the created GKE resources:

```bash
PROJECT=<gcloud project id> scripts/pulsar/gke_bootstrap_script.sh down
```

#### Create a cluster with local SSDs

If you are planning to install a Pulsar Helm chart using local persistent volumes, you need to create a GKE cluster with local SSDs. You can do so using the provided script by specifying `USE_LOCAL_SSD` to be `true`. A sample command is listed as below:

```
PROJECT=<gcloud project id> USE_LOCAL_SSD=true LOCAL_SSD_COUNT=<local-ssd-count> scripts/pulsar/gke_bootstrap_script.sh up
```
## Next Steps

Continue with the [installation of the chart](helm-deploy.md) once you have the cluster up and running.

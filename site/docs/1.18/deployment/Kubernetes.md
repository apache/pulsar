---
title: Deploying Pulsar on Kubernetes
tags: [kubernetes, google container engine]
---

Pulsar can be easily deployed in [Kubernetes](https://kubernetes.io/) clusters, either in managed clusters on [Google Container Engine](#pulsar-on-google-container-engine) or in [self-deployed clusters](#pulsar-on-a-custom-kubernetes-cluster).

The installation method shown in this guide relies on [YAML](http://yaml.org/) definitions for Kubernetes [resources](https://kubernetes.io/docs/resources-reference/v1.6/). The [`kubernetes`]({{ site.pulsar_repo }}/kubernetes) subdirectory of the [Pulsar repo]({{ site.pulsar_repo }}) holds resource definitions for:

* {% popover BookKeeper %}
* {% popover ZooKeeper %}
* A three-{% popover broker %} Pulsar {% popover cluster %}
* [Prometheus]()
* A [pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/) from which you can run commands from the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool

## Pulsar on Google Container Engine

[Google Container Engine](https://cloud.google.com/container-engine) (GKE) automates the creation of Kubernetes clusters in the [Google Compute Engine](https://cloud.google.com/compute/) (GCE) environment.

### Prerequisites

To get started, you'll need:

* A Google Cloud Platform account, which you can sign up for at [cloud.google.com](https://cloud.google.com)
* An existing Cloud Platform project
* The [Google Cloud SDK](https://cloud.google.com/sdk/downloads) (in particular the [`gcloud`](https://cloud.google.com/sdk/gcloud/) and [`kubectl`]() tools).

### Create a new Kubernetes cluster

You can create a new GKE cluster using the [`container clusters create`](https://cloud.google.com/sdk/gcloud/reference/container/clusters/create) command, which enables you to specify the number of nodes in the cluster, the machine types of those nodes, and more.

As an example, we'll create a new GKE cluster with three VMs, each using two locally attached SSDs and running on [n1-standard-8](https://cloud.google.com/compute/docs/machine-types) machines. These SSDs will be used by {% popover bookie %} instances, one for the [journal](../../getting-started/ConceptsAndArchitecture#journal-storage) and the other for storing the data.

```bash
$ gcloud container clusters create pulsar-us-cent \
  --zone=us-central1-a \
  --machine-type=n1-standard-8 \
  --num-nodes=3 \
  --local-ssd-count=2 \
  --cluster-version=1.6.4
```

By default, the bookies will be running on all the machines that have locally attached SSD disks. In this example, all the machines will have 2 SSDs, but different types of machines can be added to the cluster later. You can control which machines host bookie servers using labels.

Follow the "Connect to the cluster" instructions to open the Kubernetes dashboard in the browser.

## Pulsar on a custom Kubernetes cluster

Pulsar can be deployed on a local Kubernetes cluster as well. You can find detailed documentation on how to choose a method to install Kubernetes at https://kubernetes.io/docs/setup/pick-right-solution/.

To install a mini local cluster for testing purposes, running in VMs in the local machines you can either:

1. Use minikube to have a single-node Kubernetes cluster
1. Create a local cluster running on multiple VMs on the same machine

For the 2nd option, follow the instructions at https://github.com/pires/kubernetes-vagrant-coreos-cluster.

In short, make sure you have Vagrant and VirtualBox installed, and then:

```bash
$ git clone https://github.com/pires/kubernetes-vagrant-coreos-cluster
$ cd kubernetes-vagrant-coreos-cluster

# Start a 3 VMs cluster
$ NODES=3 USE_KUBE_UI=true vagrant up
```

Create the SSD disks mount points on the VMs. Bookies expect to have 2 logical devices to mount for journal and storage. In this VM exercise, we will just create 2 directories on each VM:

```bash
$ for vm in node-01 node-02 node-03; do
    NODES=3 vagrant ssh $vm -c "sudo mkdir -p /mnt/disks/ssd0"
    NODES=3 vagrant ssh $vm -c "sudo mkdir -p /mnt/disks/ssd1"
  done
```

Once the cluster is up, you can verify that `kubectl` can access it:

```bash
$ kubectl get nodes
NAME           STATUS                     AGE       VERSION
172.17.8.101   Ready,SchedulingDisabled   10m       v1.6.4
172.17.8.102   Ready                      8m        v1.6.4
172.17.8.103   Ready                      6m        v1.6.4
172.17.8.104   Ready                      5m        v1.6.4
```

Then use the proxy to access the web interface:

```bash
$ kubectl proxy
```

and open [http://localhost:8001/ui](http://localhost:8001/ui).

## Deploying Pulsar components

The YAML resource definitions for Pulsar components can be found in the `kubernetes` folder of the [Pulsar repository](https://github.com/yahoo/pulsar).

There are 2 versions, one for Google Container Engine (GKE) and the other for the generic Kubernetes deployment.

### ZooKeeper

```bash
$ kubectl apply -f zookeeper.yaml
```

Wait until all 3 ZooKeeper server pods are up and running. The first take will take a bit longer since it will be downloading the Docker image on the VMs.

#### Initialize cluster metadata

This step is needed to initialize the Pulsar and BookKeeper metadata within ZooKeeper.

```bash
$ kubectl exec -it zk-0 -- \
    bin/pulsar initialize-cluster-metadata \
      --cluster us-central \
      --zookeeper zookeeper \
      --global-zookeeper zookeeper \
      --web-service-url http://broker.default.svc.cluster.local:8080/ \
      --broker-service-url pulsar://broker.default.svc.cluster.local:6650/
```

#### Deploy the rest of the components

```bash
$ kubectl apply -f bookie.yaml
$ kubectl apply -f broker.yaml
$ kubectl apply -f monitoring.yaml
```

These command will deploy 3 brokers and 3 bookies (one bookie per VM), plus the monitoring infrastructure (Prometheus, Grafana and Pulsar-Dashboard).

#### Set up property and namespaces

This step is not strictly required if Pulsar authentication and authorization is turned on, though it allows later to change the policies for each of the namespaces.

Connect to the `pulsar-admin` pod that is already configured to act as a client in the newly created cluster.

```bash
$ kubectl exec pulsar-admin -it -- bash
```

From there we can issue all admin commands:

```bash
export MY_PROPERTY=prop
export MY_NAMESPACE=prop/us-central/ns

# Provision a new Pulsar property (as in "tenant")
$ bin/pulsar-admin properties create $MY_PROPERTY \
  --admin-roles admin \
  --allowed-clusters us-central

# Create a namespace can be spread on up to 16 brokers (initially)
$ bin/pulsar-admin namespaces create $MY_NAMESPACE --bundles 16
```

#### From CLI tools to test

From the same `pulsar-admin` pod, we can start a test producer to send 10K messages/s:

```bash
$ bin/pulsar-perf produce persistent://prop/us-central/ns/my-topic \
  --rate 10000
```

Similarly, we can start a consumer to subscribe and receive all the messages.

```bash
$ bin/pulsar-perf consume persistent://prop/us-central/ns/my-topic \
  --subscriber-name my-subscription-name
```

To get the stats for the topic on the CLI:

```bash
$ bin/pulsar-admin persistent stats persistent://prop/us-central/ns/my-topic
```

### Monitoring

Pulsar provides

#### Prometheus

All the metrics are being collected by a Prometheus instance running inside the Kubernetes cluster. Typically there is no need to access directly Prometheus, but rather just use the Grafana interface that is exposing the data stored in Prometheus.

#### Grafana dashboards

There are dashboards for Pulsar namespaces (rates/latency/storage), JVM stats, ZooKeeper and BookKeeper.

Get access to the pod serving Grafana:

```bash
$ kubectl port-forward $(kubectl get pods | grep grafana | awk '{print $1}') 3000
```

Then open the browser at [http://localhost:3000](http://localhost:3000).

#### Pulsar dashboard

While Grafana and Prometheus are used to provide graphs with historical data, Pulsar dashboard reports more detailed current data for the single topics.

For example, you can have sortable tables with all namespaces, topics and broker stats, with details on the IP address for consumers, since when they were connected and much more.

Get access to the pod serving the Pulsar dashboard:

```bash
$ kubectl port-forward $(kubectl get pods | grep pulsar-dashboard | awk '{print $1}') 8080:80
```

Then open the browser at [http://localhost:8080](http://localhost:8080).

---
title: Deploying Pulsar on Kubernetes
tags: [Kubernetes, Google Container Engine]
---

Pulsar can be easily deployed in [Kubernetes](https://kubernetes.io/) clusters, either in managed clusters on [Google Container Engine](#pulsar-on-google-container-engine) or [Amazon Web Services](https://aws.amazon.com/) or in [custom clusters](#pulsar-on-a-custom-kubernetes-cluster).

The deployment method shown in this guide relies on [YAML](http://yaml.org/) definitions for Kubernetes [resources](https://kubernetes.io/docs/resources-reference/v1.6/). The [`kubernetes`]({{ site.pulsar_repo }}/kubernetes) subdirectory of the [Pulsar package](/downloads) holds resource definitions for:

* A two-{% popover bookie %} {% popover BookKeeper %} cluster
* A three-node {% popover ZooKeeper %} cluster
* A three-{% popover broker %} Pulsar {% popover cluster %}
* A [monitoring stack]() consisting of [Prometheus](https://prometheus.io/), [Grafana](https://grafana.com), and the [Pulsar dashboard](../../admin/Dashboard)
* A [pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/) from which you can run administrative commands using the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool

## Setup

To get started, install a source package from the [downloads page](/downloads).

{% include admonition.html type='warning' content="Please note that the Pulsar binary package will *not* contain the necessary YAML resources to deploy Pulsar on Kubernetes." %}

If you'd like to change the number of bookies, brokers, or ZooKeeper nodes in your Pulsar cluster, modify the `replicas` parameter in the `spec` section of the appropriate [`Deployment`](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) or [`StatefulSet`](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) resource.

## Pulsar on Google Container Engine

[Google Container Engine](https://cloud.google.com/container-engine) (GKE) automates the creation and management of Kubernetes clusters in [Google Compute Engine](https://cloud.google.com/compute/) (GCE).

### Prerequisites

To get started, you'll need:

* A Google Cloud Platform account, which you can sign up for at [cloud.google.com](https://cloud.google.com)
* An existing Cloud Platform project
* The [Google Cloud SDK](https://cloud.google.com/sdk/downloads) (in particular the [`gcloud`](https://cloud.google.com/sdk/gcloud/) and [`kubectl`]() tools).

### Create a new Kubernetes cluster

You can create a new GKE cluster using the [`container clusters create`](https://cloud.google.com/sdk/gcloud/reference/container/clusters/create) command for `gcloud`. This command enables you to specify the number of nodes in the cluster, the machine types of those nodes, and more.

As an example, we'll create a new GKE cluster for Kubernetes version [1.6.4](https://github.com/kubernetes/kubernetes/blob/master/CHANGELOG.md#v164) in the [us-central1-a](https://cloud.google.com/compute/docs/regions-zones/regions-zones#available) zone. The cluster will be named `pulsar-gke-cluster` and will consist of three VMs, each using two locally attached SSDs and running on [n1-standard-8](https://cloud.google.com/compute/docs/machine-types) machines. These SSDs will be used by {% popover bookie %} instances, one for the BookKeeper [journal](../../getting-started/ConceptsAndArchitecture#journal-storage) and the other for storing the actual message data.

```bash
$ gcloud container clusters create pulsar-gke-cluster \
  --zone=us-central1-a \
  --machine-type=n1-standard-8 \
  --num-nodes=3 \
  --local-ssd-count=2 \
  --cluster-version=1.6.4
```

By default, bookies will run on all the machines that have locally attached SSD disks. In this example, all of those machines will have two SSDs, but you can add different types of machines to the cluster later. You can control which machines host bookie servers using [labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels).

### Dashboard

You can observe your cluster in the [Kubernetes Dashboard](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/) by downloading the credentials for your Kubernetes cluster and opening up a proxy to the cluster:

```bash
$ gcloud container clusters get-credentials pulsar-gke-cluster \
  --zone=us-central1-a \
  --project=your-project-name
$ kubectl proxy
```

By default, the proxy will be opened on port 8001. Now you can navigate to [localhost:8001/ui](http://localhost:8001/ui) in your browser to access the dashboard. At first your GKE cluster will be empty, but that will change as you begin deploying Pulsar [components](#deploying-pulsar-components).

## Pulsar on Amazon Web Services

You can run Kubernetes on [Amazon Web Services](https://aws.amazon.com/) (AWS) in a variety of ways. A very simple way that was [recently introduced](https://aws.amazon.com/blogs/compute/kubernetes-clusters-aws-kops/) involves using the [Kubernetes Operations](https://github.com/kubernetes/kops) (kops) tool.

You can find detailed instructions for setting up a Kubernetes cluster on AWS [here](https://github.com/kubernetes/kops/blob/master/docs/aws.md).

When you create a cluster using those instructions, your `kubectl` config in `~/.kube/config` (on MacOS and Linux) will be updated for you, so you probably won't need to change your configuration. Nonetheless, you can ensure that `kubectl` can interact with your cluster by listing the nodes in the cluster:

```bash
$ kubectl get nodes
```

If `kubectl` is working with your cluster, you can proceed to [deploy Pulsar components](#deploying-pulsar-components).

## Pulsar on a custom Kubernetes cluster

Pulsar can be deployed on a custom, non-GKE Kubernetes cluster as well. You can find detailed documentation on how to choose a Kubernetes installation method that suits your needs in the [Picking the Right Solution](https://kubernetes.io/docs/setup/pick-right-solution) guide in the Kubernetes docs.

### Local cluster

The easiest way to run a Kubernetes cluster is to do so locally. To install a mini local cluster for testing purposes, running in local VMs, you can either:

1. Use [minikube](https://kubernetes.io/docs/getting-started-guides/minikube/) to run a single-node Kubernetes cluster
1. Create a local cluster running on multiple VMs on the same machine

For the second option, follow the [instructions](https://github.com/pires/kubernetes-vagrant-coreos-cluster) for running Kubernetes using [CoreOS](https://coreos.com/) on [Vagrant](https://www.vagrantup.com/). We'll provided an abridged version of those instructions here.


First, make sure you have [Vagrant](https://www.vagrantup.com/downloads.html) and [VirtualBox](https://www.virtualbox.org/wiki/Downloads) installed. Then clone the repo and start up the cluster:

```bash
$ git clone https://github.com/pires/kubernetes-vagrant-coreos-cluster
$ cd kubernetes-vagrant-coreos-cluster

# Start a three-VM cluster
$ NODES=3 USE_KUBE_UI=true vagrant up
```

Create SSD disk mount points on the VMs using this script:

```bash
$ for vm in node-01 node-02 node-03; do
    NODES=3 vagrant ssh $vm -c "sudo mkdir -p /mnt/disks/ssd0"
    NODES=3 vagrant ssh $vm -c "sudo mkdir -p /mnt/disks/ssd1"
  done
```

{% popover Bookies %} expect two logical devices to mount for [journal](../../getting-started/ConceptsAndArchitecture#journal-storage) and persistent message storage to be available. In this VM exercise, we created two directories on each VM.

Once the cluster is up, you can verify that `kubectl` can access it:

```bash
$ kubectl get nodes
NAME           STATUS                     AGE       VERSION
172.17.8.101   Ready,SchedulingDisabled   10m       v1.6.4
172.17.8.102   Ready                      8m        v1.6.4
172.17.8.103   Ready                      6m        v1.6.4
172.17.8.104   Ready                      4m        v1.6.4
```

### Dashboard

In order to use the [Kubernetes Dashboard](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/) with your local Kubernetes cluster, first use `kubectl` to create a proxy to the cluster:

```bash
$ kubectl proxy
```

Now you can access the web interface at [localhost:8001/ui](http://localhost:8001/ui). At first your local cluster will be empty, but that will change as you begin deploying Pulsar [components](#deploying-pulsar-components).

## Deploying Pulsar components

Now that you've set up a Kubernetes cluster, either on [Google Container Engine](#pulsar-on-google-container-engine) or on a [custom cluster](#pulsar-on-a-custom-kubernetes-cluster), you can begin deploying the components that make up Pulsar. The YAML resource definitions for Pulsar components can be found in the `kubernetes` folder of the [Pulsar source package](/download).

In that package, there are two sets of resource definitions, one for Google Container Engine (GKE) in the `kubernetes/google-container-engine` folder and one for a custom Kubernetes cluster in the `kubernetes/generic` folder. To begin, `cd` into the appropriate folder.

### ZooKeeper

You *must* deploy {% popover ZooKeeper %} as the first Pulsar component, as it is a dependency for the others.

```bash
$ kubectl apply -f zookeeper.yaml
```

Wait until all three ZooKeeper server pods are up and have the status `Running`. You can check on the status of the ZooKeeper pods at any time:

```bash
$ kubectl get pods -l component=zookeeper
NAME      READY     STATUS             RESTARTS   AGE
zk-0      1/1       Running            0          18m
zk-1      1/1       Running            0          17m
zk-2      0/1       Running            6          15m
```

This step may take several minutes, as Kubernetes needs to download the Docker image on the VMs.

#### Initialize cluster metadata

Once ZooKeeper is running, you need to [initialize the metadata](../InstanceSetup#cluster-metadata-initialization) for the Pulsar cluster in ZooKeeper. This includes system metadata for {% popover BookKeeper %} and Pulsar more broadly.

```bash
$ kubectl exec -it zk-0 -- \
    bin/pulsar initialize-cluster-metadata \
      --cluster us-central \
      --zookeeper zookeeper \
      --global-zookeeper zookeeper \
      --web-service-url http://broker.default.svc.cluster.local:8080/ \
      --broker-service-url pulsar://broker.default.svc.cluster.local:6650/
```

Make sure to modify the metadata values for your own cluster when necessary.

#### Deploy the rest of the components

Once cluster metadata has been successfully initialized, you can then deploy the {% popover bookies %}, {% popover brokers %}, and monitoring stack ([Prometheus](https://prometheus.io), [Grafana](https://grafana.com), and the [Pulsar dashboard](../../admin/Dashboard)).

```bash
$ kubectl apply -f bookie.yaml
$ kubectl apply -f broker.yaml
$ kubectl apply -f monitoring.yaml
```

You can check on the status of the pods for these components either in the Kubernetes Dashboard or using `kubectl`:

```bash
$ kubectl get pods
```

#### Set up properties and namespaces

Once all of the components are up and running, you'll need to create at least one Pulsar {% popover property %} and at least one {% popover namespace %}.

{% include admonition.html type='info' content='
This step is not strictly required if Pulsar [authentication and authorization](../../admin/Authz) is turned on, though it allows you to change [policies](../../admin/PropertiesNamespaces#managing-namespaces) for each of the namespaces later.
' %}

To create properties and namespaces, connect to the `pulsar-admin` pod that has already been configured to act as a client for your newly created Pulsar cluster.

```bash
$ kubectl exec pulsar-admin -it -- bash
```

From there, you can issue all admin commands. Here's an example command that would create a property named `prop` and a namespace within that property named `prop/us-central-ns`.

```bash
export MY_PROPERTY=prop
export MY_NAMESPACE=prop/us-central/ns

# Provision a new Pulsar property
$ bin/pulsar-admin properties create $MY_PROPERTY \
  --admin-roles admin \
  --allowed-clusters us-central

# Create a namespace that can be spread across up to 16 brokers
$ bin/pulsar-admin namespaces create $MY_NAMESPACE --bundles 16
```

#### Experimenting with your cluster

Now that a property and namespace have been created, you can begin experimenting with your running Pulsar cluster. From the same `pulsar-admin` pod, for example, you can use [`pulsar-perf`](../../reference/CliTools#pulsar-perf) to create a test {% popover producer %} to publish 10,000 messages a second on a topic in the {% popover property %} and {% popover namespace %} you created:

```bash
$ bin/pulsar-perf produce persistent://prop/us-central/ns/my-topic \
  --rate 10000
```

Similarly, you can start a {% popover consumer %} to subscribe to and receive all the messages on that topic:

```bash
$ bin/pulsar-perf consume persistent://prop/us-central/ns/my-topic \
  --subscriber-name my-subscription-name
```

You can also view [stats](../../admin/Stats) for the topic using the [`pulsar-admin`](../../reference/CliTools#pulsar-admin-persistent-stats) tool:

```bash
$ bin/pulsar-admin persistent stats persistent://prop/us-central/ns/my-topic
```

### Monitoring

The default monitoring stack for Pulsar on Kubernetes has consists of [Prometheus](#prometheus), [Grafana](#grafana), and the [Pulsar dashbaord](../../admin/Dashboard).

#### Prometheus

All Pulsar metrics in Kubernetes are collected by a [Prometheus](https://prometheus.io) instance running inside the cluster. Typically, there is no need to access Prometheus directly. Instead, you can the [Grafana interface](#grafana) that displays the data stored in Prometheus.

#### Grafana

In your Kubernetes cluster, you can use [Grafana](https://grafana.com) to view dashbaords for Pulsar {% popover namespaces %} (message rates, latency, and storage), JVM stats, {% popover ZooKeeper %}, and {% popover BookKeeper %}. You can get access to the pod serving Grafana using `kubectl`'s [`port-forward`](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster) command:

```bash
$ kubectl port-forward $(kubectl get pods | grep grafana | awk '{print $1}') 3000
```

You can then access the dashboard in your web browser at [localhost:3000](http://localhost:3000).

#### Pulsar dashboard

While Grafana and Prometheus are used to provide graphs with historical data, [Pulsar dashboard](../../admin/Dashbaord) reports more detailed current data for individual {% popover topics %}.

For example, you can have sortable tables showing all namespaces, topics, and broker stats, with details on the IP address for consumers, how long they've been connected, and much more.

You can access to the pod serving the Pulsar dashboard using `kubectl`'s [`port-forward`](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster) command:

```bash
$ kubectl port-forward $(kubectl get pods | grep pulsar-dashboard | awk '{print $1}') 8080:80
```

You can then access the dashboard in your web browser at [localhost:8080](http://localhost:8080).
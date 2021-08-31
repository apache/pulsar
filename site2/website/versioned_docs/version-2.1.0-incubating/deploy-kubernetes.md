---
id: version-2.1.0-incubating-deploy-kubernetes
title: Deploying Pulsar on Kubernetes
sidebar_label: Kubernetes
original_id: deploy-kubernetes
---

> ### Tips
>
> If you want to enable all builtin [Pulsar IO](io-overview.md) connectors in your Pulsar deployment, you can choose to use `apachepulsar/pulsar-all` image instead of
> `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled [all builtin connectors](io-overview.md#working-with-connectors).

Pulsar can be easily deployed in [Kubernetes](https://kubernetes.io/) clusters, either in managed clusters on [Google Kubernetes Engine](#pulsar-on-google-kubernetes-engine) or [Amazon Web Services](https://aws.amazon.com/) or in [custom clusters](#pulsar-on-a-custom-kubernetes-cluster).

The deployment method shown in this guide relies on [YAML](http://yaml.org/) definitions for Kubernetes [resources](https://kubernetes.io/docs/reference/). The {@inject: github:`kubernetes`:/kubernetes} subdirectory of the [Pulsar package](pulsar:download_page_url) holds resource definitions for:

* A two-bookie BookKeeper cluster
* A three-node ZooKeeper cluster
* A three-broker Pulsar cluster
* A [monitoring stack]() consisting of [Prometheus](https://prometheus.io/), [Grafana](https://grafana.com), and the [Pulsar dashboard](administration-dashboard.md)
* A [pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/) from which you can run administrative commands using the [`pulsar-admin`](reference-pulsar-admin.md) CLI tool

## Setup

To get started, install a source package from the [downloads page](pulsar:download_page_url).

> Please note that the Pulsar binary package will *not* contain the necessary YAML resources to deploy Pulsar on Kubernetes.

If you'd like to change the number of bookies, brokers, or ZooKeeper nodes in your Pulsar cluster, modify the `replicas` parameter in the `spec` section of the appropriate [`Deployment`](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) or [`StatefulSet`](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) resource.

## Pulsar on Google Kubernetes Engine

[Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine) (GKE) automates the creation and management of Kubernetes clusters in [Google Compute Engine](https://cloud.google.com/compute/) (GCE).

### Prerequisites

To get started, you'll need:

* A Google Cloud Platform account, which you can sign up for at [cloud.google.com](https://cloud.google.com)
* An existing Cloud Platform project
* The [Google Cloud SDK](https://cloud.google.com/sdk/downloads) (in particular the [`gcloud`](https://cloud.google.com/sdk/gcloud/) and [`kubectl`]() tools).

### Create a new Kubernetes cluster

You can create a new GKE cluster using the [`container clusters create`](https://cloud.google.com/sdk/gcloud/reference/container/clusters/create) command for `gcloud`. This command enables you to specify the number of nodes in the cluster, the machine types of those nodes, and more.

As an example, we'll create a new GKE cluster for Kubernetes version [1.6.4](https://github.com/kubernetes/kubernetes/blob/master/CHANGELOG.md#v164) in the [us-central1-a](https://cloud.google.com/compute/docs/regions-zones/regions-zones#available) zone. The cluster will be named `pulsar-gke-cluster` and will consist of three VMs, each using two locally attached SSDs and running on [n1-standard-8](https://cloud.google.com/compute/docs/machine-types) machines. These SSDs will be used by
[bookie](reference-terminology.md#bookie) instances, one for the BookKeeper [journal](concepts-architecture-overview.md#journal-storage) and the other for storing the actual message data.

```bash
$ gcloud container clusters create pulsar-gke-cluster \
  --zone=us-central1-a \
  --machine-type=n1-standard-8 \
  --num-nodes=3 \
  --local-ssd-count=2 \
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

By default, the proxy will be opened on port 8001. Now you can navigate to [localhost:8001/ui](http://localhost:8001/ui) in your browser to access the dashboard. At first your GKE cluster will be empty, but that will change as you begin deploying Pulsar components using `kubectl` [component by component](#deploying-pulsar-components),
or using [`helm`](#deploying-pulsar-components-helm).

## Pulsar on Amazon Web Services

You can run Kubernetes on [Amazon Web Services](https://aws.amazon.com/) (AWS) in a variety of ways. A very simple way that was [recently introduced](https://aws.amazon.com/blogs/compute/kubernetes-clusters-aws-kops/) involves using the [Kubernetes Operations](https://github.com/kubernetes/kops) (kops) tool.

You can find detailed instructions for setting up a Kubernetes cluster on AWS [here](https://github.com/kubernetes/kops/blob/master/docs/getting_started/aws.md).

When you create a cluster using those instructions, your `kubectl` config in `~/.kube/config` (on MacOS and Linux) will be updated for you, so you probably won't need to change your configuration. Nonetheless, you can ensure that `kubectl` can interact with your cluster by listing the nodes in the cluster:

```bash
$ kubectl get nodes
```

If `kubectl` is working with your cluster, you can proceed to deploy Pulsar components using `kubectl` [component by component](#deploying-pulsar-components),
or using [`helm`](#deploying-pulsar-components-helm).

## Pulsar on a custom Kubernetes cluster

Pulsar can be deployed on a custom, non-GKE Kubernetes cluster as well. You can find detailed documentation on how to choose a Kubernetes installation method that suits your needs in the [Picking the Right Solution](https://kubernetes.io/docs/setup/pick-right-solution) guide in the Kubernetes docs.

The easiest way to run a Kubernetes cluster is to do so locally. To install a mini local cluster for testing purposes, running in local VMs, you can either:

1. Use [minikube](https://minikube.sigs.k8s.io/docs/start/) to run a single-node Kubernetes cluster
1. Create a local cluster running on multiple VMs on the same machine

### Minikube

1. [Install and configure minikube](https://github.com/kubernetes/minikube#installation) with
   a [VM driver](https://github.com/kubernetes/minikube#requirements), e.g. `kvm2` on Linux or `hyperkit` or `VirtualBox` on macOS.
1. Create a kubernetes cluster on Minikube.
    ```shell
    minikube start --memory=8192 --cpus=4 \
        --kubernetes-version=v1.10.5
    ```
1. Set `kubectl` to use Minikube.
    ```shell
    kubectl config use-context minikube
    ```

In order to use the [Kubernetes Dashboard](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/)
with local Kubernetes cluster on Minikube, run:

```bash
$ minikube dashboard
```

The command will automatically trigger open a webpage in your browser. At first your local cluster will be empty,
but that will change as you begin deploying Pulsar components using `kubectl` [component by component](#deploying-pulsar-components),
or using [`helm`](#deploying-pulsar-components-helm).

### Multiple VMs

For the second option, follow the [instructions](https://github.com/pires/kubernetes-vagrant-coreos-cluster) for running Kubernetes using [CoreOS](https://coreos.com/) on [Vagrant](https://www.vagrantup.com/). We'll provide an abridged version of those instructions here.


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

Bookies expect two logical devices to mount for [journal](concepts-architecture-overview.md#journal-storage) and persistent message storage to be available. In this VM exercise, we created two directories on each VM.

Once the cluster is up, you can verify that `kubectl` can access it:

```bash
$ kubectl get nodes
NAME           STATUS                     AGE       VERSION
172.17.8.101   Ready,SchedulingDisabled   10m       v1.6.4
172.17.8.102   Ready                      8m        v1.6.4
172.17.8.103   Ready                      6m        v1.6.4
172.17.8.104   Ready                      4m        v1.6.4
```

In order to use the [Kubernetes Dashboard](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/) with your local Kubernetes cluster, first use `kubectl` to create a proxy to the cluster:

```bash
$ kubectl proxy
```

Now you can access the web interface at [localhost:8001/ui](http://localhost:8001/ui). At first your local cluster will be empty,
but that will change as you begin deploying Pulsar components using `kubectl` [component by component](#deploying-pulsar-components),
or using [`helm`](#deploying-pulsar-components-helm).

## Deploying Pulsar components

Now that you've set up a Kubernetes cluster, either on [Google Kubernetes Engine](#pulsar-on-google-kubernetes-engine) or on a [custom cluster](#pulsar-on-a-custom-kubernetes-cluster), you can begin deploying the components that make up Pulsar. The YAML resource definitions for Pulsar components can be found in the `kubernetes` folder of the [Pulsar source package](pulsar:download_page_url).

In that package, there are different sets of resource definitions for different environments.

- `deployment/kubernetes/google-kubernetes-engine`: for Google Kubernetes Engine (GKE)
- `deployment/kubernetes/aws`: for AWS
- `deployment/kubernetes/generic`: for a custom Kubernetes cluster

To begin, `cd` into the appropriate folder.

### Deploy ZooKeeper

You *must* deploy ZooKeeper as the first Pulsar component, as it is a dependency for the others.

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

### Initialize cluster metadata

Once ZooKeeper is running, you need to [initialize the metadata](#cluster-metadata-initialization) for the Pulsar cluster in ZooKeeper. This includes system metadata for [BookKeeper](reference-terminology.md#bookkeeper) and Pulsar more broadly. There is a Kubernetes [job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/) in the `cluster-metadata.yaml` file that you only need to run once:

```bash
$ kubectl apply -f cluster-metadata.yaml
```

For the sake of reference, that job runs the following command on an ephemeral pod:

```bash
$ bin/pulsar initialize-cluster-metadata \
  --cluster local \
  --zookeeper zookeeper \
  --configuration-store zookeeper \
  --web-service-url http://broker.default.svc.cluster.local:8080/ \
  --broker-service-url pulsar://broker.default.svc.cluster.local:6650/
```

### Deploy the rest of the components

Once cluster metadata has been successfully initialized, you can then deploy the bookies, brokers, monitoring stack ([Prometheus](https://prometheus.io), [Grafana](https://grafana.com), and the [Pulsar dashboard](administration-dashboard.md)), and Pulsar cluster proxy:

```bash
$ kubectl apply -f bookie.yaml
$ kubectl apply -f broker.yaml
$ kubectl apply -f proxy.yaml
$ kubectl apply -f monitoring.yaml
$ kubectl apply -f admin.yaml
```

You can check on the status of the pods for these components either in the Kubernetes Dashboard or using `kubectl`:

```bash
$ kubectl get pods -w -l app=pulsar
```

### Set up properties and namespaces

Once all of the components are up and running, you'll need to create at least one Pulsar tenant and at least one namespace.

> This step is not strictly required if Pulsar [authentication and authorization](security-overview.md) is turned on, though it allows you to change [policies](admin-api-namespaces.md) for each of the namespaces later.

You can create properties and namespaces (and perform any other administrative tasks) using the `pulsar-admin` pod that is already configured to act as an admin client for your newly created Pulsar cluster. One easy way to perform administrative tasks is to create an alias for the [`pulsar-admin`](reference-pulsar-admin.md) tool installed on the admin pod.

```bash
$ alias pulsar-admin='kubectl exec pulsar-admin -it -- bin/pulsar-admin'
```

Now, any time you run `pulsar-admin`, you will be running commands from that pod. This command will create a tenant called `ten`:

```bash
$ pulsar-admin tenants create ten \
  --admin-roles admin \
  --allowed-clusters local
```

This command will create a `ns` namespace under the `ten` tenant:

```bash
$ pulsar-admin namespaces create ten/ns
```

To verify that everything has gone as planned:

```bash
$ pulsar-admin tenants list
public
ten

$ pulsar-admin namespaces list ten
ten/ns
```

Now that you have a namespace and tenant set up, you can move on to [experimenting with your Pulsar cluster](#experimenting-with-your-cluster) from within the cluster or [connecting to the cluster](#client-connections) using a Pulsar client.

### Experimenting with your cluster

Now that a tenant and namespace have been created, you can begin experimenting with your running Pulsar cluster. Using the same `pulsar-admin` pod via an alias, as in the section above, you can use [`pulsar-perf`](reference-cli-tools.md#pulsar-perf) to create a test [producer](reference-terminology.md#producer) to publish 10,000 messages a second on a topic in the [tenant](reference-terminology.md#tenant) and [namespace](reference-terminology.md#namespace) you created.

First, create an alias to use the `pulsar-perf` tool via the admin pod:

```bash
$ alias pulsar-perf='kubectl exec pulsar-admin -it -- bin/pulsar-perf'
```

Now, produce messages:

```bash
$ pulsar-perf produce persistent://public/default/my-topic \
  --rate 10000
```

Similarly, you can start a [consumer](reference-terminology.md#consumer) to subscribe to and receive all the messages on that topic:

```bash
$ pulsar-perf consume persistent://public/default/my-topic \
  --subscriber-name my-subscription-name
```

You can also view [stats](administration-stats.md) for the topic using the [`pulsar-admin`](reference-pulsar-admin.md#persistent-stats) tool:

```bash
$ pulsar-admin persistent stats persistent://public/default/my-topic
```

### Monitoring

The default monitoring stack for Pulsar on Kubernetes has consists of [Prometheus](#prometheus), [Grafana](#grafana), and the [Pulsar dashboard](administration-dashboard.md).

> If you deployed the cluster to Minikube, the following monitoring ports are mapped at the minikube VM:
>
> - Prometheus port: 30003
> - Grafana port: 30004
> - Dashboard port: 30005
>
> You can use `minikube ip` to find the ip address of the minikube VM, and then use their mapped ports
> to access corresponding services. For example, you can access Pulsar dashboard at `http://$(minikube ip):30005`.

#### Prometheus

All Pulsar metrics in Kubernetes are collected by a [Prometheus](https://prometheus.io) instance running inside the cluster. Typically, there is no need to access Prometheus directly. Instead, you can use the [Grafana interface](#grafana) that displays the data stored in Prometheus.

#### Grafana

In your Kubernetes cluster, you can use [Grafana](https://grafana.com) to view dashboards for Pulsar [namespaces](reference-terminology.md#namespace) (message rates, latency, and storage), JVM stats, [ZooKeeper](https://zookeeper.apache.org), and [BookKeeper](reference-terminology.md#bookkeeper). You can get access to the pod serving Grafana using `kubectl`'s [`port-forward`](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster) command:

```bash
$ kubectl port-forward \
  $(kubectl get pods -l component=grafana -o jsonpath='{.items[*].metadata.name}') 3000
```

You can then access the dashboard in your web browser at [localhost:3000](http://localhost:3000).

#### Pulsar dashboard

While Grafana and Prometheus are used to provide graphs with historical data, [Pulsar dashboard](administration-dashboard.md) reports more detailed current data for individual [topics](reference-terminology.md#topic).

For example, you can have sortable tables showing all namespaces, topics, and broker stats, with details on the IP address for consumers, how long they've been connected, and much more.

You can access to the pod serving the Pulsar dashboard using `kubectl`'s [`port-forward`](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster) command:

```bash
$ kubectl port-forward \
  $(kubectl get pods -l component=dashboard -o jsonpath='{.items[*].metadata.name}') 8080:80
```

You can then access the dashboard in your web browser at [localhost:8080](http://localhost:8080).

### Client connections

> If you deployed the cluster to Minikube, the proxy ports are mapped at the minikube VM:
>
> - Http port: 30001
> - Pulsar binary protocol port: 30002
>
> You can use `minikube ip` to find the ip address of the minikube VM, and then use their mapped ports
> to access corresponding services. For example, pulsar webservice url will be at `http://$(minikube ip):30001`.

Once your Pulsar cluster is running on Kubernetes, you can connect to it using a Pulsar client. You can fetch the IP address for the Pulsar proxy running in your Kubernetes cluster using kubectl:

```bash
$ kubectl get service broker-proxy \
  --output=jsonpath='{.status.loadBalancer.ingress[*].ip}'
```

If the IP address for the proxy were, for example, 35.12.13.198, you could connect to Pulsar using `pulsar://35.12.13.198:6650`.

You can find client documentation for:

* [Java](client-libraries-java.md)
* [Python](client-libraries-python.md)
* [C++](client-libraries-cpp.md)


## Deploying Pulsar components (helm)

Pulsar also provides a [Helm](https://docs.helm.sh/) chart for deploying a Pulsar cluster to Kubernetes. Before you start,
make sure you follow [Helm documentation](https://docs.helm.sh/using_helm) to install helm.

> Assume you have cloned pulsar repo under a `PULSAR_HOME` directory.

### Minikube

1. Go to Pulsar helm chart directory
    ```shell
    cd ${PULSAR_HOME}/deployment/kubernetes/helm
    ```
1. Install helm chart to a K8S cluster on Minikube.
    ```shell
    helm install --values pulsar/values-mini.yaml ./pulsar
    ```

Once the helm chart is completed on installation, you can access the cluster via:

- Web service url: `http://$(minikube ip):30001/`
- Pulsar service url: `pulsar://$(minikube ip):30002/`

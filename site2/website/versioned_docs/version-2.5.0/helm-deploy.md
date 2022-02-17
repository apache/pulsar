---
id: helm-deploy
title: Deploying a Pulsar cluster using Helm
sidebar_label: "Deployment"
original_id: helm-deploy
---

Before running `helm install`, you need to make some decisions about how you will run Pulsar.
Options can be specified using Helm's `--set option.name=value` command line option.

## Selecting configuration options

In each section collect the options that will be combined to use with `helm install`.

### Kubernetes Namespace

By default, the chart is installed to a namespace called `pulsar`.

```yaml

namespace: pulsar

```

If you decide to install the chart into a different k8s namespace, you can include this option in your Helm install command:

```bash

--set namespace=<different-k8s-namespace>

```

By default, the chart doesn't create the namespace.

```yaml

namespaceCreate: false

```

If you want the chart to create the k8s namespace automatically, you can include this option in your Helm install command.

```bash

--set namespaceCreate=true

```

### Persistence

By default the chart creates Volume Claims with the expectation that a dynamic provisioner will create the underlying Persistent Volumes.

```yaml

volumes:
  persistence: true
  # configure the components to use local persistent volume
  # the local provisioner should be installed prior to enable local persistent volume
  local_storage: false

```

If you would like to use local persistent volumes as the persistent storage for your Helm release, you can install [local-storage-provisioner](#install-local-storage-provisioner) and include the following option in your Helm install command. 

```bash

--set volumes.local_storage=true

```

> **Important**: After initial installation, making changes to your storage settings requires manually editing Kubernetes objects,
> so it's best to plan ahead before installing your production instance of Pulsar to avoid extra storage migration work.

This chart is designed for production use, To use this chart in a development environment (e.g. minikube), you can disable persistence by including this option in your Helm install command.

```bash

--set volumes.persistence=false

```

### Affinity 

By default `anti-affinity` is turned on to ensure pods of same component can run on different nodes.

```yaml

affinity:
  anti_affinity: true

```

If you are planning to use this chart in a development environment (e.g. minikue), you can disable `anti-affinity` by including this option in your Helm install command.

```bash

--set affinity.anti_affinity=false

```

### Components

This chart is designed for production usage. It deploys a production-ready Pulsar cluster including Pulsar core components and monitoring components.

You can customize the components to deploy by turning on/off individual components.

```yaml

## Components
##
## Control what components of Apache Pulsar to deploy for the cluster
components:
  # zookeeper
  zookeeper: true
  # bookkeeper
  bookkeeper: true
  # bookkeeper - autorecovery
  autorecovery: true
  # broker
  broker: true
  # functions
  functions: true
  # proxy
  proxy: true
  # toolset
  toolset: true
  # pulsar manager
  pulsar_manager: true

## Monitoring Components
##
## Control what components of the monitoring stack to deploy for the cluster
monitoring:
  # monitoring - prometheus
  prometheus: true
  # monitoring - grafana
  grafana: true

```

### Docker Images

This chart is designed to enable controlled upgrades. So it provides the capability to configure independent image versions for components. You can customize the images by setting individual component.

```yaml

## Images
##
## Control what images to use for each component
images:
  zookeeper:
    repository: apachepulsar/pulsar-all
    tag: 2.5.0
    pullPolicy: IfNotPresent
  bookie:
    repository: apachepulsar/pulsar-all
    tag: 2.5.0
    pullPolicy: IfNotPresent
  autorecovery:
    repository: apachepulsar/pulsar-all
    tag: 2.5.0
    pullPolicy: IfNotPresent
  broker:
    repository: apachepulsar/pulsar-all
    tag: 2.5.0
    pullPolicy: IfNotPresent
  proxy:
    repository: apachepulsar/pulsar-all
    tag: 2.5.0
    pullPolicy: IfNotPresent
  functions:
    repository: apachepulsar/pulsar-all
    tag: 2.5.0
  prometheus:
    repository: prom/prometheus
    tag: v1.6.3
    pullPolicy: IfNotPresent
  grafana:
    repository: streamnative/apache-pulsar-grafana-dashboard-k8s
    tag: 0.0.4
    pullPolicy: IfNotPresent
  pulsar_manager:
    repository: apachepulsar/pulsar-manager
    tag: v0.1.0
    pullPolicy: IfNotPresent
    hasCommand: false

```

### TLS

This Pulsar Chart can be configured to enable TLS to protect all the traffic between components. Before you enable TLS, you have to provision TLS certificates
for the components you have configured to enable TLS.

- [Provision TLS certs using `cert-manager`](#provision-tls-certs-using-cert-manager)

#### Provision TLS certs using cert-manager

In order to using `cert-manager` to provision the TLS certificates, you have to install [cert-manager](#install-cert-manager) before installing the Pulsar chart. After
successfully install cert manager, you can then set `certs.internal_issuer.enabled`
to `true`. So the Pulsar chart will use `cert-manager` to generate `selfsigning` TLS
certs for the configured components.

```yaml

certs:
  internal_issuer:
    enabled: false
    component: internal-cert-issuer
    type: selfsigning

```

You can also customize the generated TLS certificates by configuring the fields as the following.

```yaml

tls:
  # common settings for generating certs
  common:
    # 90d
    duration: 2160h
    # 15d
    renewBefore: 360h
    organization:
      - pulsar
    keySize: 4096
    keyAlgorithm: rsa
    keyEncoding: pkcs8

```

#### Enable TLS

After installing `cert-manager`, you can then set `tls.enabled` to `true` to enable TLS encryption for the entire cluster.

```yaml

tls:
  enabled: false

```

You can also control whether to enable TLS encryption for individual component.

```yaml

tls:
  # settings for generating certs for proxy
  proxy:
    enabled: false
    cert_name: tls-proxy
  # settings for generating certs for broker
  broker:
    enabled: false
    cert_name: tls-broker
  # settings for generating certs for bookies
  bookie:
    enabled: false
    cert_name: tls-bookie
  # settings for generating certs for zookeeper
  zookeeper:
    enabled: false
    cert_name: tls-zookeeper
  # settings for generating certs for recovery
  autorecovery:
    cert_name: tls-recovery
  # settings for generating certs for toolset
  toolset:
    cert_name: tls-toolset

```

### Authentication

Authentication is disabled by default. You can set `auth.authentication.enabled` to `true` to turn on authentication.
Currently this chart only supports JWT authentication provider. You can set `auth.authentication.provider` to `jwt` to use JWT authentication provider.

```yaml

# Enable or disable broker authentication and authorization.
auth:
  authentication:
    enabled: false
    provider: "jwt"
    jwt:
      # Enable JWT authentication
      # If the token is generated by a secret key, set the usingSecretKey as true.
      # If the token is generated by a private key, set the usingSecretKey as false.
      usingSecretKey: false
  superUsers:
    # broker to broker communication
    broker: "broker-admin"
    # proxy to broker communication
    proxy: "proxy-admin"
    # pulsar-admin client to broker/proxy communication
    client: "admin"

```

If you decide to enable authentication, you can run [prepare helm release](#prepare-the-helm-release) to generate token secret keys and tokens for three super users specified in `auth.superUsers` field. The generated token keys and super user tokens are uploaded and stored as kubernetes secrets prefixed with `<pulsar-release-name>-token-`. You can use following command to find those secrets.

```bash

kubectl get secrets -n <k8s-namespace>

```

### Authorization

Authorization is disabled by default. Authorization can be enabled
only if Authentication is enabled.

```yaml

auth:
  authorization:
    enabled: false

```

You can include this option to turn on authorization.

```bash

--set auth.authorization.enabled=true

```

### CPU and RAM resource requirements

The resource requests, and number of replicas for the Pulsar components in this Chart are set by default to be adequate for a small production deployment. If you are trying to deploy a non-production instance, you can reduce the defaults in order to fit into a smaller cluster.

Once you have all of your configuration options collected, we need
to install dependent charts before proceeding to install the Pulsar
Chart.

## Install Dependent Charts

### Install Local Storage Provisioner

If you decide to use local persistent volumes as the persistent storage, you need to [install a storage provisioner for local persistent volumes](https://kubernetes.io/blog/2019/04/04/kubernetes-1.14-local-persistent-volumes-ga/).

One of the easiest way to get started is to use the local storage provisioner provided along with the Pulsar Helm chart.

```

helm repo add streamnative https://charts.streamnative.io
helm repo update
helm install pulsar-storage-provisioner streamnative/local-storage-provisioner

```

### Install Cert Manager

The Pulsar Chart uses [cert-manager](https://github.com/jetstack/cert-manager) to automate provisioning and managing TLS certificates. If you decide to enable TLS encryption for brokers or proxies, you need to install cert-manager first.

You can follow the [official instructions](https://cert-manager.io/docs/installation/kubernetes/#installing-with-helm) to install cert-manager.

Alternatively, we provide a bash script [install-cert-manager.sh](https://github.com/apache/pulsar-helm-chart/blob/master/scripts/cert-manager/install-cert-manager.sh) to install a cert-manager release to namespace `cert-manager`.

```bash

git clone https://github.com/apache/pulsar-helm-chart
cd pulsar-helm-chart
./scripts/cert-manager/install-cert-manager.sh

```

## Prepare the Helm Release

Once you have install all the dependent charts and collected all of your configuration options, you can run [prepare_helm_release.sh](https://github.com/apache/pulsar-helm-chart/blob/master/scripts/pulsar/prepare_helm_release.sh) to prepare the helm release.

```bash

git clone https://github.com/apache/pulsar-helm-chart
cd pulsar-helm-chart
./scripts/pulsar/prepare_helm_release.sh -n <k8s-namespace> -k <helm-release-name>

```

The `prepare_helm_release` creates following resources:

- A k8s namespace for installing the Pulsar release
- Create a secret for storing the username and password of control center administrator. The username and password can be passed to `prepare_helm_release.sh` through flags `--control-center-admin` and `--control-center-password`. The username and password is used for logging into Grafana dashboard and Pulsar Manager.
- Create the JWT secret keys and tokens for three superusers: `broker-admin`, `proxy-admin`, and `admin`. By default, it generates asymmetric pubic/private key pair. You can choose to generate symmeric secret key by specifying `--symmetric`.
  - `proxy-admin` role is used for proxies to communicate to brokers.
  - `broker-admin` role is used for inter-broker communications.
  - `admin` role is used by the admin tools.

## Deploy using Helm

Once you have done the following three things, you can proceed to install a Helm release.

- Collect all of your configuration options
- Install dependent charts
- Prepare the Helm release

In this example, we've named our Helm release `pulsar`.

```bash

git clone https://github.com/apache/pulsar-helm-chart
cd pulsar-helm-chart
helm upgrade --install pulsar charts/pulsar \
    --timeout 600 \
    --set [your configuration options]

```

:::note

For the first deployment, add `--set initialize=true` option to initialize bookie and Pulsar cluster metadata.

:::

You can also use `--version <installation version>` option if you would like to install a specific version of Pulsar Helm chart.

## Monitoring the Deployment

This will output the list of resources installed once the deployment finishes which may take 5-10 minutes.

The status of the deployment can be checked by running `helm status pulsar` which can also be done while the deployment is taking place if you run the command in another terminal.

## Accessing the Pulsar Cluster

The default values will create a `ClusterIP` for the following resources you can use to interact with the cluster.

- Proxy: You can use the IP address to produce and consume messages to the installed Pulsar cluster.
- Pulsar Manager: You can access the pulsar manager UI at `http://<pulsar-manager-ip>:9527`.
- Grafana Dashboard: You can access the Grafana dashboard at `http://<grafana-dashboard-ip>:3000`.

To find the IP address of those components use:

```bash

kubectl get service -n <k8s-namespace>

```


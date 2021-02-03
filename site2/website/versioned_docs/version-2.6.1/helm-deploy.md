---
id: version-2.6.1-helm-deploy
title: Deploy Pulsar cluster using Helm
sidebar_label: Deployment
original_id: helm-deploy
---

Before running `helm install`, you need to decide how to run Pulsar.
Options can be specified using Helm's `--set option.name=value` command line option.

## Select configuration options

In each section, collect the options that are combined to use with the `helm install` command.

### Kubernetes namespace

By default, the Pulsar Helm chart is installed to a namespace called `pulsar`.

```yaml
namespace: pulsar
```

To install the Pulsar Helm chart into a different Kubernetes namespace, you can include this option in the `helm install` command.

```bash
--set namespace=<different-k8s-namespace>
```

By default, the Pulsar Helm chart doesn't create the namespace.

```yaml
namespaceCreate: false
```

To use the Pulsar Helm chart to create the Kubernetes namespace automatically, you can include this option in the `helm install` command.

```bash
--set namespaceCreate=true
```

### Persistence

By default, the Pulsar Helm chart creates Volume Claims with the expectation that a dynamic provisioner creates the underlying Persistent Volumes.

```yaml
volumes:
  persistence: true
  # configure the components to use local persistent volume
  # the local provisioner should be installed prior to enable local persistent volume
  local_storage: false
```

To use local persistent volumes as the persistent storage for Helm release, you can install the [local storage provisioner](#install-local-storage-provisioner) and include the following option in the `helm install` command. 

```bash
--set volumes.local_storage=true
```

> #### Note
> 
> Before installing the production instance of Pulsar, ensure to plan the storage settings to avoid extra storage migration work. Because after initial installation, you must edit Kubernetes objects manually if you want to change storage settings.

The Pulsar Helm chart is designed for production use. To use the Pulsar Helm chart in a development environment (such as Minikube), you can disable persistence by including this option in your `helm install` command.

```bash
--set volumes.persistence=false
```

### Affinity 

By default, `anti-affinity` is enabled to ensure pods of the same component can run on different nodes.

```yaml
affinity:
  anti_affinity: true
```

To use the Pulsar Helm chart in a development environment (such as Minikue), you can disable `anti-affinity` by including this option in your `helm install` command.

```bash
--set affinity.anti_affinity=false
```

### Components

The Pulsar Helm chart is designed for production usage. It deploys a production-ready Pulsar cluster, including Pulsar core components and monitoring components.

You can customize the components to be deployed by turning on/off individual components.

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

### Docker images

The Pulsar Helm chart is designed to enable controlled upgrades. So it can configure independent image versions for components. You can customize the images by setting individual component.

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

The Pulsar Helm chart can be configured to enable TLS (Transport Layer Security) to protect all the traffic between components. Before enabling TLS, you have to provision TLS certificates for the required components.

#### Provision TLS certificates using cert-manager

To use the `cert-manager` to provision the TLS certificates, you have to install the [cert-manager](#install-cert-manager) before installing the Pulsar Helm chart. After successfully installing the cert-manager, you can set `certs.internal_issuer.enabled` to `true`. Therefore, the Pulsar Helm chart can use the `cert-manager` to generate `selfsigning` TLS certificates for the configured components.

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

After installing the `cert-manager`, you can set `tls.enabled` to `true` to enable TLS encryption for the entire cluster.

```yaml
tls:
  enabled: false
```

You can also configure whether to enable TLS encryption for individual component.

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

By default, authentication is disabled. You can set `auth.authentication.enabled` to `true` to enable authentication.
Currently, the Pulsar Helm chart only supports JWT authentication provider. You can set `auth.authentication.provider` to `jwt` to use the JWT authentication provider.

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

To enable authentication, you can run [prepare helm release](#prepare-the-helm-release) to generate token secret keys and tokens for three super users specified in the `auth.superUsers` field. The generated token keys and super user tokens are uploaded and stored as Kubernetes secrets prefixed with `<pulsar-release-name>-token-`. You can use the following command to find those secrets.

```bash
kubectl get secrets -n <k8s-namespace>
```

### Authorization

By default, authorization is disabled. Authorization can be enabled only when authentication is enabled.

```yaml
auth:
  authorization:
    enabled: false
```

To enable authorization, you can include this option in the `helm install` command.

```bash
--set auth.authorization.enabled=true
```

### CPU and RAM resource requirements

By default, the resource requests and the number of replicas for the Pulsar components in the Pulsar Helm chart are adequate for a small production deployment. If you deploy a non-production instance, you can reduce the defaults to fit into a smaller cluster.

Once you have all of your configuration options collected, you can install dependent charts before installing the Pulsar Helm chart.

## Install dependent charts

### Install local storage provisioner

To use local persistent volumes as the persistent storage, you need to install a storage provisioner for [local persistent volumes](https://kubernetes.io/blog/2019/04/04/kubernetes-1.14-local-persistent-volumes-ga/).

One of the easiest way to get started is to use the local storage provisioner provided along with the Pulsar Helm chart.

```
helm repo add streamnative https://charts.streamnative.io
helm repo update
helm install pulsar-storage-provisioner streamnative/local-storage-provisioner
```

### Install cert-manager

The Pulsar Helm chart uses the [cert-manager](https://github.com/jetstack/cert-manager) to provision and manage TLS certificates automatically. To enable TLS encryption for brokers or proxies, you need to install the cert-manager in advance.

For details about how to install the cert-manager, follow the [official instructions](https://cert-manager.io/docs/installation/kubernetes/#installing-with-helm).

Alternatively, we provide a bash script [install-cert-manager.sh](https://github.com/apache/pulsar/blob/master/deployment/kubernetes/helm/scripts/cert-manager/install-cert-manager.sh) to install a cert-manager release to the namespace `cert-manager`.

```bash
git clone https://github.com/apache/pulsar
cd pulsar/deployment/kubernetes/helm
./scripts/cert-manager/install-cert-manager.sh
```

## Prepare Helm release

Once you have install all the dependent charts and collected all of your configuration options, you can run [prepare_helm_release.sh](https://github.com/apache/pulsar/blob/master/deployment/kubernetes/helm/scripts/pulsar/prepare_helm_release.sh) to prepare the Helm release.

```bash
git clone https://github.com/apache/pulsar
cd pulsar/deployment/kubernetes/helm
./scripts/pulsar/prepare_helm_release.sh -n <k8s-namespace> -k <helm-release-name>
```

The `prepare_helm_release` creates the following resources:

- A Kubernetes namespace for installing the Pulsar release.
- JWT secret keys and tokens for three super users: `broker-admin`, `proxy-admin`, and `admin`. By default, it generates an asymmetric pubic/private key pair. You can choose to generate a symmetric secret key by specifying `--symmetric`.
    - `proxy-admin` role is used for proxies to communicate to brokers.
    - `broker-admin` role is used for inter-broker communications.
    - `admin` role is used by the admin tools.

## Deploy Pulsar cluster using Helm

Once you have finished the following three things, you can install a Helm release.

- Collect all of your configuration options.
- Install dependent charts.
- Prepare the Helm release.

In this example, we name our Helm release `pulsar`.

```bash
git clone https://github.com/apache/pulsar
cd pulsar/deployment/kubernetes/helm
helm upgrade --install pulsar pulsar \
    --timeout 10m \
    --set [your configuration options]
```
> **Note**
>
> For the first deployment, add `--set initialize=true` option to initialize bookie and Pulsar cluster metadata.

You can also use the `--version <installation version>` option if you want to install a specific version of Pulsar Helm chart.

## Monitor deployment

A list of installed resources are output once the Pulsar cluster is deployed. This may take 5-10 minutes.

The status of the deployment can be checked by running the `helm status pulsar` command, which can also be done while the deployment is taking place if you run the command in another terminal.

## Access Pulsar cluster

The default values will create a `ClusterIP` for the following resources, which you can use to interact with the cluster.

- Proxy: You can use the IP address to produce and consume messages to the installed Pulsar cluster.
- Pulsar Manager: You can access the Pulsar Manager UI at `http://<pulsar-manager-ip>:9527`.
- Grafana Dashboard: You can access the Grafana dashboard at `http://<grafana-dashboard-ip>:3000`.

To find the IP addresses of those components, run the following command:

```bash
kubectl get service -n <k8s-namespace>
```

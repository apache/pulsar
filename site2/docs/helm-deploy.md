---
id: helm-deploy
title: Deploy a Pulsar cluster on Kubernetes
sidebar_label: "Deploy"
---

Before deploying a Pulsar cluster, you need to [prepare Kubernetes resources](helm-prepare.md) and then continue with the following steps.
1. Select configuration options
2. Install dependent charts
3. Prepare Helm release
4. Deploy your Pulsar cluster using Helm

## Select configuration options

Specify how to run Pulsar using Helm's `--set option.name=value` command line option. In each section, collect the options that are combined to use with the `helm install` command.

#### Kubernetes namespace

By default, the Pulsar Helm Chart is installed in a namespace called `pulsar`.

```yaml
namespace: pulsar
```

To install the Pulsar Helm Chart into a different Kubernetes namespace, you can include this option in the `helm install` command.

```bash
--set namespace=<different-k8s-namespace>
```

By default, the Pulsar Helm Chart doesn't create the namespace.

```yaml
namespaceCreate: false
```

To use the Pulsar Helm Chart to create the Kubernetes namespace automatically, you can include this option in the `helm install` command.

```bash
--set namespaceCreate=true
```

#### Persistence

By default, the Pulsar Helm Chart creates Volume Claims with the expectation that a dynamic provisioner creates the underlying Persistent Volumes.

```yaml
volumes:
  persistence: true
```

:::note

Before installing the production instance of Pulsar, ensure to plan the storage settings to avoid extra storage migration work. Because after initial installation, you must edit Kubernetes objects manually if you want to change storage settings.

:::

The Pulsar Helm Chart is designed for production use. To use the Pulsar Helm Chart in a development environment (such as Minikube), you can disable persistence by including this option in your `helm install` command.

```bash
--set volumes.persistence=false
```

#### Affinity 

By default, `anti-affinity` is enabled to ensure pods of the same component can run on different nodes.

```yaml
affinity:
  anti_affinity: true
```

To use the Pulsar Helm Chart in a development environment (such as Minikube), you can disable `anti-affinity` by including this option in your `helm install` command.

```bash
--set affinity.anti_affinity=false
```

#### Components

The Pulsar Helm Chart is designed for production usage. It deploys a production-ready Pulsar cluster, including Pulsar core components and monitoring components.

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

#### Docker images

The Pulsar Helm Chart is designed to enable controlled upgrades. So it can configure independent image versions for components. You can customize the images by setting individual components.

```yaml
## Images
##
## Control what images to use for each component
images:
  zookeeper:
    repository: apachepulsar/pulsar-all
    tag: @pulsar:version@
    pullPolicy: IfNotPresent
  bookie:
    repository: apachepulsar/pulsar-all
    tag: @pulsar:version@
    pullPolicy: IfNotPresent
  autorecovery:
    repository: apachepulsar/pulsar-all
    tag: @pulsar:version@
    pullPolicy: IfNotPresent
  broker:
    repository: apachepulsar/pulsar-all
    tag: @pulsar:version@
    pullPolicy: IfNotPresent
  proxy:
    repository: apachepulsar/pulsar-all
    tag: @pulsar:version@
    pullPolicy: IfNotPresent
  functions:
    repository: apachepulsar/pulsar-all
    tag: @pulsar:version@
  pulsar_manager:
    repository: apachepulsar/pulsar-manager
    tag: v0.3.0
    pullPolicy: IfNotPresent
    hasCommand: false
```

#### TLS

The Pulsar Helm Chart can be configured to enable TLS (Transport Layer Security) to protect all the traffic between components. Before enabling TLS, you have to provision TLS certificates for the required components.

##### Provision TLS certificates using cert-manager

To use the `cert-manager` to provision the TLS certificates, you have to install the [cert-manager](#install-cert-manager) before installing the Pulsar Helm Chart. After successfully installing the cert-manager, you can set `certs.internal_issuer.enabled` to `true`. Therefore, the Pulsar Helm Chart can use the `cert-manager` to generate `selfsigning` TLS certificates for the configured components.

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

##### Enable TLS

After installing the `cert-manager`, you can set `tls.enabled` to `true` to enable TLS encryption for the entire cluster.

```yaml
tls:
  enabled: false
```

You can also configure whether to enable TLS encryption for individual components.

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

#### Authentication

By default, authentication is disabled. You can set `auth.authentication.enabled` to `true` to enable authentication.
Currently, the Pulsar Helm Chart only supports the JWT authentication provider. You can set `auth.authentication.provider` to `jwt` to use the JWT authentication provider.

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

To enable authentication, you can run [prepare helm release](#prepare-helm-release) to generate token secret keys and tokens for three super users specified in the `auth.superUsers` field. The generated token keys and super user tokens are uploaded and stored as Kubernetes secrets prefixed with `<pulsar-release-name>-token-`. You can use the following command to find those secrets.

```bash
kubectl get secrets -n <k8s-namespace>
```

#### Authorization

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

#### CPU and RAM resource requirements

By default, the resource requests and the number of replicas for the Pulsar components in the Pulsar Helm Chart are adequate for small production deployment. If you deploy a non-production instance, you can reduce the defaults to fit into a smaller cluster.

Once you have all of your configuration options collected, you can install dependent charts before installing the Pulsar Helm Chart.

## Install dependent charts

### Install storage provisioner

For more information about storage provisioner, refer to [Kubernetes documentation](https://kubernetes.io/docs/concepts/storage/storage-classes/#provisioner). Note that you need to create a storage class for your Kubernetes cluster and configure the [storage class name](https://github.com/apache/pulsar-helm-chart/blob/master/charts/pulsar/values.yaml) in the Helm Chart.

If you want to use **local** [persistent volumes](#persistence) as the persistent storage, you need to install a local storage provisioner. Here are two options:
* [Local Path Provisioner](https://github.com/rancher/local-path-provisioner)
* [Local Persistence Volume Static Provisioner](https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner)

### Install cert-manager

The Pulsar Helm Chart uses the [cert-manager](https://github.com/jetstack/cert-manager) to provision and manage TLS certificates automatically. To enable TLS encryption for brokers or proxies, you need to install the cert-manager in advance.

For details about how to install the cert-manager, follow the [official instructions](https://cert-manager.io/docs/installation/kubernetes/#installing-with-helm).

Alternatively, we provide a bash script [install-cert-manager.sh](https://github.com/apache/pulsar-helm-chart/blob/master/scripts/cert-manager/install-cert-manager.sh) to install a cert-manager release to the namespace `cert-manager`.

```bash
git clone https://github.com/apache/pulsar-helm-chart
cd pulsar-helm-chart
./scripts/cert-manager/install-cert-manager.sh
```

## Prepare Helm release

Once you have installed all the dependent charts and collected all of your configuration options, you can run [prepare_helm_release.sh](https://github.com/apache/pulsar-helm-chart/blob/master/scripts/pulsar/prepare_helm_release.sh) to prepare the Helm release.

```bash
git clone https://github.com/apache/pulsar-helm-chart
cd pulsar-helm-chart
./scripts/pulsar/prepare_helm_release.sh -n <k8s-namespace> -k <helm-release-name>
```

The `prepare_helm_release` creates the following resources:

- A Kubernetes namespace for installing the Pulsar release.
- JWT secret keys and tokens for three super users: `broker-admin`, `proxy-admin`, and `admin`. By default, it generates an asymmetric public/private key pair. You can choose to generate a symmetric secret key by specifying `--symmetric`.
  - the `broker-admin` role is used for inter-broker communications.
  - the `proxy-admin` role is used for proxies to communicate with brokers.
  - the `admin` role is used by the admin tools.

## Deploy Pulsar cluster using Helm

Once you have finished the above steps, you can install a Helm release.

In this example, the Helm release is named `pulsar`.

```bash
helm repo add apache https://pulsar.apache.org/charts
helm repo update
helm install pulsar apache/pulsar \
    --timeout 10m \
    --set [your configuration options]
```

You can also use the `--version <installation version>` option if you want to install a specific version of Pulsar Helm Chart.

:::tip

A list of installed resources is output once the Pulsar cluster is deployed. This may take 5-10 minutes.

To check the status of the deployment, run the `helm status pulsar` command. It can also be done while the deployment is taking place if you run the command in another terminal.

:::

## Access Pulsar cluster

The default values will create a `ClusterIP` for the following resources, which you can use to interact with the cluster.

- Proxy: You can use the IP address to produce and consume messages to the installed Pulsar cluster.
- Pulsar Manager: You can access the Pulsar Manager UI at `http://<pulsar-manager-ip>:9527`.
- Grafana Dashboard: You can access the Grafana dashboard at `http://<grafana-dashboard-ip>:3000`.

To find the IP addresses of those components, run the following command:

```bash
kubectl get service -n <k8s-namespace>
```

## Troubleshoot

Although we have done our best to make these charts as seamless as possible, troubles do go out of our control occasionally. We have been collecting tips and tricks for troubleshooting common issues. Check it first before raising an [issue](https://github.com/apache/pulsar/issues/new/choose), and feel free to add your solutions by creating a [Pull Request](https://github.com/apache/pulsar/compare).


## Uninstall

To uninstall the Pulsar Helm Chart, run the following command:

```bash
helm uninstall <pulsar-release-name>
```

For the purposes of continuity, some Kubernetes objects in these charts cannot be removed by using the `helm uninstall` command. It is recommended to *consciously* remove these items, as they affect re-deployment.

* PVCs for stateful data: remove these items.
  - ZooKeeper: This is your metadata.
  - BookKeeper: This is your data.
  - Prometheus: This is your metrics data, which can be safely removed.
* Secrets: if the secrets are generated by the [prepared release script](https://github.com/apache/pulsar-helm-chart/blob/master/scripts/pulsar/prepare_helm_release.sh), they contain secret keys and tokens. You can use the [cleanup release script](https://github.com/apache/pulsar-helm-chart/blob/master/scripts/pulsar/cleanup_helm_release.sh) to remove these secrets and tokens as needed.

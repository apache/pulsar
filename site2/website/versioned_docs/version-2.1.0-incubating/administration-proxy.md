---
id: version-2.1.0-incubating-administration-proxy
title: The Pulsar proxy
sidebar_label: Pulsar proxy
original_id: administration-proxy
---

The [Pulsar proxy](concepts-architecture-overview.md#pulsar-proxy) is an optional gateway that you can run over the brokers in a Pulsar cluster. We recommend running a Pulsar proxy in cases when direction connections between clients and Pulsar brokers are either infeasible, undesirable, or both, for example when running Pulsar in a cloud environment or on [Kubernetes](https://kubernetes.io) or an analogous platform.

## Running the proxy

In order to run the Pulsar proxy, you need to have both a local [ZooKeeper](https://zookeeper.apache.org) and configuration store quorum set up for use by your Pulsar cluster. For instructions, see [this document](deploy-bare-metal.md). Once you have ZooKeeper set up and have connection strings for both ZooKeeper quorums, you can use the [`proxy`](reference-cli-tools.md#pulsar-proxy) command of the [`pulsar`](reference-cli-tools.md#pulsar) CLI tool to start up the proxy (preferably on its own machine or in its own VM):

To start the proxy:

```bash
$ cd /path/to/pulsar/directory
$ bin/pulsar proxy \
  --zookeeper-servers zk-0,zk-1,zk-2 \
  --configuration-store-servers zk-0,zk-1,zk-2
```

> You can run as many instances of the Pulsar proxy in a cluster as you would like.


## Stopping the proxy

The Pulsar proxy runs by default in the foreground. To stop the proxy, simply stop the process in which it's running.

## Proxy frontends

We recommend running the Pulsar proxy behind some kind of load-distributing frontend, such as an [HAProxy](https://www.digitalocean.com/community/tutorials/an-introduction-to-haproxy-and-load-balancing-concepts) load balancer.

## Using Pulsar clients with the proxy

Once your Pulsar proxy is up and running, preferably behind a load-distributing [frontend](#proxy-frontends), clients can connect to the proxy via whichever address is used by the frontend. If the address were the DNS address `pulsar.cluster.default`, for example, then the connection URL for clients would be `pulsar://pulsar.cluster.default:6650`.

## Proxy configuration

The Pulsar proxy can be configured using the [`proxy.conf`](reference-configuration.md#proxy) configuration file. The following parameters are available in that file:

|Name|Description|Default|
|---|---|---|
|zookeeperServers|  The ZooKeeper quorum connection string (as a comma-separated list)  ||
|configurationStoreServers| Configuration store connection string (as a comma-separated list) ||
|zookeeperSessionTimeoutMs| ZooKeeper session timeout (in milliseconds) |30000|
|servicePort| The port to use for server binary Protobuf requests |6650|
|servicePortTls|  The port to use to server binary Protobuf TLS requests  |6651|
|statusFilePath | Path for the file used to determine the rotation status for the proxy instance when responding to service discovery health checks ||
|authenticationEnabled| Whether authentication is enabled for the Pulsar proxy  |false|
|authenticationProviders| Authentication provider name list (a comma-separated list of class names) ||
|authorizationEnabled|  Whether authorization is enforced by the Pulsar proxy |false|
|authorizationProvider| Authorization provider as a fully qualified class name  |org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider|
|brokerClientAuthenticationPlugin|  The authentication plugin used by the Pulsar proxy to authenticate with Pulsar brokers  ||
|brokerClientAuthenticationParameters|  The authentication parameters used by the Pulsar proxy to authenticate with Pulsar brokers  ||
|brokerClientTrustCertsFilePath|  The path to trusted certificates used by the Pulsar proxy to authenticate with Pulsar brokers ||
|superUserRoles|  Role names that are treated as “super-users,” meaning that they will be able to perform all admin ||
|forwardAuthorizationCredentials| Whether client authorization credentials are forwarded to the broker for re-authorization. Authentication must be enabled via authenticationEnabled=true for this to take effect.  |false|
|maxConcurrentInboundConnections| Max concurrent inbound connections. The proxy will reject requests beyond that. |10000|
|maxConcurrentLookupRequests| Max concurrent outbound connections. The proxy will error out requests beyond that. |10000|
|tlsEnabledInProxy| Whether TLS is enabled for the proxy  |false|
|tlsEnabledWithBroker|  Whether TLS is enabled when communicating with Pulsar brokers |false|
|tlsCertificateFilePath|  Path for the TLS certificate file ||
|tlsKeyFilePath|  Path for the TLS private key file ||
|tlsTrustCertsFilePath| Path for the trusted TLS certificate pem file ||
|tlsHostnameVerificationEnabled|  Whether the hostname is validated when the proxy creates a TLS connection with brokers  |false|
|tlsRequireTrustedClientCertOnConnect|  Whether client certificates are required for TLS. Connections are rejected if the client certificate isn’t trusted. |false|

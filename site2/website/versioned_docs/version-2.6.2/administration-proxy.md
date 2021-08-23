---
id: version-2.6.2-administration-proxy
title: The Pulsar proxy
sidebar_label: Pulsar proxy
original_id: administration-proxy
---

The [Pulsar proxy](concepts-architecture-overview.md#pulsar-proxy) is an optional gateway that you can run in front of the brokers in a Pulsar cluster. You can run a Pulsar proxy in cases when direction connections between clients and Pulsar brokers are either infeasible, undesirable, or both, for example when you run Pulsar in a cloud environment or on [Kubernetes](https://kubernetes.io) or an analogous platform.

## Configure the proxy

The proxy must have some way to find the addresses of the brokers of the cluster. You can do this by either configuring the proxy to connect directly to service discovery or by specifying a broker URL in the configuration. 

### Option 1: Use service discovery

Pulsar uses [ZooKeeper](https://zookeeper.apache.org) for service discovery. To connect the proxy to ZooKeeper, specify the following in `conf/proxy.conf`.
```properties
zookeeperServers=zk-0,zk-1,zk-2
configurationStoreServers=zk-0:2184,zk-remote:2184
```

> If you use service discovery, the network ACL must allow the proxy to talk to the ZooKeeper nodes on the zookeeper client port, which is usually 2181, and on the configuration store client port, which is 2184 by default. Opening the network ACLs means that if someone compromises a proxy, they have full access to ZooKeeper. For this reason, using broker URLs to configure the proxy is more secure.

### Option 2: Use broker URLs

The more secure method of configuring the proxy is to specify a URL to connect to the brokers.

> [Authorization](security-authorization#enable-authorization-and-assign-superusers) at the proxy requires access to ZooKeeper, so if you use these broker URLs to connect to the brokers, you should disable the Proxy level authorization. Brokers still authorize requests after the proxy forwards them.

You can configure the broker URLs in `conf/proxy.conf` as follows.

```properties
brokerServiceURL=pulsar://brokers.example.com:6650
brokerWebServiceURL=http://brokers.example.com:8080
functionWorkerWebServiceURL=http://function-workers.example.com:8080
```

Or if you use TLS:
```properties
brokerServiceURLTLS=pulsar+ssl://brokers.example.com:6651
brokerWebServiceURLTLS=https://brokers.example.com:8443
functionWorkerWebServiceURL=https://function-workers.example.com:8443
```

The hostname in the URLs provided should be a DNS entry which points to multiple brokers or a Virtual IP which is backed by multiple broker IP addresses so that the proxy does not lose connectivity to the pulsar cluster if a single broker becomes unavailable.

The ports to connect to the brokers (6650 and 8080, or in the case of TLS, 6651 and 8443) should be open in the network ACLs.

Note that if you do not use functions, then you do not need to configure `functionWorkerWebServiceURL`.

## Start the proxy

To start the proxy:

```bash
$ cd /path/to/pulsar/directory
$ bin/pulsar proxy
```

> You can run as many instances of the Pulsar proxy in a cluster as you want.


## Stop the proxy

The Pulsar proxy runs by default in the foreground. To stop the proxy, simply stop the process in which the proxy is running.

## Proxy frontends

You can run the Pulsar proxy behind some kind of load-distributing frontend, such as an [HAProxy](https://www.digitalocean.com/community/tutorials/an-introduction-to-haproxy-and-load-balancing-concepts) load balancer.

## Use Pulsar clients with the proxy

Once your Pulsar proxy is up and running, preferably behind a load-distributing [frontend](#proxy-frontends), clients can connect to the proxy via whichever address that the frontend uses. If the address is the DNS address `pulsar.cluster.default`, for example, then the connection URL for clients is `pulsar://pulsar.cluster.default:6650`.

## Proxy configuration

You can configure the Pulsar proxy using the [`proxy.conf`](reference-configuration.md#proxy) configuration file. The following parameters are available in that file:

|Name|Description|Default|
|---|---|---|
|zookeeperServers|  The ZooKeeper quorum connection string (as a comma-separated list)  ||
|configurationStoreServers| Configuration store connection string (as a comma-separated list) ||
|zookeeperSessionTimeoutMs| ZooKeeper session timeout (in milliseconds) |30000|
|servicePort| The port to use for server binary Protobuf requests |6650|
|servicePortTls|  The port to use to server binary Protobuf TLS requests  |6651|
|statusFilePath | Path for the file used to determine the rotation status for the proxy instance when responding to service discovery health checks ||
|advertisedAddress|Hostname or IP address the service advertises to the outside world.|`InetAddress.getLocalHost().getHostname()`|
|authenticationEnabled| Whether authentication is enabled for the Pulsar proxy  |false|
|authenticateMetricsEndpoint| Whether the '/metrics' endpoint requires authentication. Defaults to true. 'authenticationEnabled' must also be set for this to take effect. |true|
|authenticationProviders| Authentication provider name list (a comma-separated list of class names) ||
|authorizationEnabled|  Whether authorization is enforced by the Pulsar proxy |false|
|authorizationProvider| Authorization provider as a fully qualified class name  |org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider|
|brokerClientAuthenticationPlugin|  The authentication plugin used by the Pulsar proxy to authenticate with Pulsar brokers  ||
|brokerClientAuthenticationParameters|  The authentication parameters used by the Pulsar proxy to authenticate with Pulsar brokers  ||
|brokerClientTrustCertsFilePath|  The path to trusted certificates used by the Pulsar proxy to authenticate with Pulsar brokers ||
|superUserRoles|  Role names that are treated as “super-users,” meaning that they are able to perform all admin ||
|forwardAuthorizationCredentials| Whether client authorization credentials are forwarded to the broker for re-authorization. Authentication must be enabled via authenticationEnabled=true for this to take effect.  |false|
|maxConcurrentInboundConnections| Max concurrent inbound connections. The proxy rejects requests beyond that. |10000|
|maxConcurrentLookupRequests| Max concurrent outbound connections. The proxy errors out requests beyond that. |50000|
|tlsEnabledInProxy| Whether TLS is enabled for the proxy  |false|
|tlsEnabledWithBroker|  Whether TLS is enabled when communicating with Pulsar brokers |false|
|tlsCertificateFilePath|  Path for the TLS certificate file ||
|tlsKeyFilePath|  Path for the TLS private key file ||
|tlsTrustCertsFilePath| Path for the trusted TLS certificate pem file ||
|tlsHostnameVerificationEnabled|  Whether the hostname is validated when the proxy creates a TLS connection with brokers  |false|
|tlsRequireTrustedClientCertOnConnect|  Whether client certificates are required for TLS. Connections are rejected if the client certificate is not trusted. |false|

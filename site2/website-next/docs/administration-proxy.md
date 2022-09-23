---
id: administration-proxy
title: Pulsar proxy
sidebar_label: "Pulsar proxy"
---

Pulsar proxy is an optional gateway. Pulsar proxy is used when direct connections between clients and Pulsar brokers are either infeasible or undesirable. For example, when you run Pulsar in a cloud environment or on [Kubernetes](https://kubernetes.io) or an analogous platform, you can run Pulsar proxy.

The Pulsar proxy is not intended to be exposed on the public internet. The security considerations in the current design expect network perimeter security. The requirement of network perimeter security can be achieved with private networks.

If a proxy deployment cannot be protected with network perimeter security, the alternative would be to use [Pulsar's "Proxy SNI routing" feature](concepts-proxy-sni-routing.md) with a properly secured and audited solution. In that case Pulsar proxy component is not used at all.

## Configure the proxy

Before using a proxy, you need to configure it with a broker's address in the cluster. You can configure the broker URL in the proxy configuration, or the proxy to connect directly using service discovery.

### Use service discovery

Pulsar uses [ZooKeeper](https://zookeeper.apache.org) for service discovery. To connect the proxy to ZooKeeper, specify the following in `conf/proxy.conf`.

```properties

zookeeperServers=zk-0,zk-1,zk-2
configurationStoreServers=zk-0:2184,zk-remote:2184

```

> To use service discovery, you need to open the network ACLs, so the proxy can connects to the ZooKeeper nodes through the ZooKeeper client port (port `2181`) and the configuration store client port (port `2184`).

> However, it is not secure to use service discovery. Because if the network ACL is open, when someone compromises a proxy, they have full access to ZooKeeper. 

### Use broker URLs

It is more secure to specify a URL to connect to the brokers.

Proxy authorization requires access to ZooKeeper, so if you use these broker URLs to connect to the brokers, you need to disable authorization at the Proxy level. Brokers still authorize requests after the proxy forwards them.

You can configure the broker URLs in `conf/proxy.conf` as follows.

```properties
brokerServiceURL=pulsar://brokers.example.com:6650
brokerWebServiceURL=http://brokers.example.com:8080
functionWorkerWebServiceURL=http://function-workers.example.com:8080
```

If you use TLS, configure the broker URLs in the following way:

```properties
brokerServiceURLTLS=pulsar+ssl://brokers.example.com:6651
brokerWebServiceURLTLS=https://brokers.example.com:8443
functionWorkerWebServiceURL=https://function-workers.example.com:8443
```

The hostname in the URLs provided should be a DNS entry that points to multiple brokers or a virtual IP address, which is backed by multiple broker IP addresses, so that the proxy does not lose connectivity to Pulsar cluster if a single broker becomes unavailable.

The ports to connect to the brokers (6650 and 8080, or in the case of TLS, 6651 and 8443) should be open in the network ACLs.

Note that if you do not use functions, you do not need to configure `functionWorkerWebServiceURL`.

> However, it is not secure to use service discovery. Because if the network ACL is open, when someone compromises a proxy, they have full access to ZooKeeper.

### Restricting target broker addresses to mitigate CVE-2022-24280

The Pulsar Proxy trusts clients to provide valid target broker addresses to connect to.
Unless the Pulsar Proxy is explicitly configured to limit access, the Pulsar Proxy is vulnerable as described in the security advisory [Apache Pulsar Proxy target broker address isn't validated (CVE-2022-24280)](https://github.com/apache/pulsar/wiki/CVE-2022-24280).

It is necessary to limit proxied broker connections to known broker addresses by specifying `brokerProxyAllowedHostNames` and `brokerProxyAllowedIPAddresses` settings.

When specifying `brokerProxyAllowedHostNames`, it's possible to use a wildcard. 
Please notice that `*` is a wildcard that matches any character in the hostname. It also matches dot `.` characters.

It is recommended to use a pattern that matches only the desired brokers and no other hosts in the local network. Pulsar lookups will use the default host name of the broker by default. This can be overridden with the `advertisedAddress` setting in `broker.conf`.

To increase security, it is also possible to restrict access with the `brokerProxyAllowedIPAddresses` setting. It is not mandatory to configure `brokerProxyAllowedIPAddresses` when `brokerProxyAllowedHostNames` is properly configured so that the pattern matches only the target brokers.
`brokerProxyAllowedIPAddresses` setting supports a comma separate list of IP address, IP address ranges and IP address networks [(supported format reference)](https://seancfoley.github.io/IPAddress/IPAddress/apidocs/inet/ipaddr/IPAddressString.html).

Example: limiting by host name in a Kubernetes deployment
```yaml
  # example of limiting to Kubernetes statefulset hostnames that contain "broker-" 
  PULSAR_PREFIX_brokerProxyAllowedHostNames: '*broker-*.*.*.svc.cluster.local'
```

Example: limiting by both host name and ip address in a `proxy.conf` file for host deployment.
```properties
# require "broker" in host name
brokerProxyAllowedHostNames=*broker*.localdomain
# limit target ip addresses to a specific network
brokerProxyAllowedIPAddresses=10.0.0.0/8
```

Example: limiting by multiple host name patterns and multiple ip address ranges in a `proxy.conf` file for host deployment.
```properties
# require "broker" in host name
brokerProxyAllowedHostNames=*broker*.localdomain,*broker*.otherdomain
# limit target ip addresses to a specific network or range demonstrating multiple supported formats
brokerProxyAllowedIPAddresses=10.10.0.0/16,192.168.1.100-120,172.16.2.*,10.1.2.3
```

## Start the proxy

To start the proxy:

```bash

$ cd /path/to/pulsar/directory
$ bin/pulsar proxy \
  --metadata-store zk:my-zk-1:2181,my-zk-2:2181,my-zk-3:2181 \
  --configuration-metadata-store zk:my-zk-1:2181,my-zk-2:2181,my-zk-3:2181
```

> You can run multiple instances of the Pulsar proxy in a cluster.

## Stop the proxy

Pulsar proxy runs in the foreground by default. To stop the proxy, simply stop the process in which the proxy is running.

## Proxy frontends

You can run Pulsar proxy behind some kind of load-distributing frontend, such as an [HAProxy](https://www.digitalocean.com/community/tutorials/an-introduction-to-haproxy-and-load-balancing-concepts) load balancer.

## Use Pulsar clients with the proxy

Once your Pulsar proxy is up and running, preferably behind a load-distributing [frontend](#proxy-frontends), clients can connect to the proxy via whichever address that the frontend uses. If the address is the DNS address `pulsar.cluster.default`, for example, the connection URL for clients is `pulsar://pulsar.cluster.default:6650`.

For more information on Proxy configuration, refer to [Pulsar proxy](reference-configuration.md#pulsar-proxy).

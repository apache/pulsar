---
id: version-2.7.0-administration-proxy
title: Pulsar proxy
sidebar_label: Pulsar proxy
original_id: administration-proxy
---

Pulsar proxy is an optional gateway. Pulsar proxy is used when direction connections between clients and Pulsar brokers are either infeasible or undesirable. For example, when you run Pulsar in a cloud environment or on [Kubernetes](https://kubernetes.io) or an analogous platform, you can run Pulsar proxy.

## Configure the proxy

Before using the proxy, you need to configure it with the brokers addresses in the cluster. You can configure the proxy to connect directly to service discovery, or specify a broker URL in the configuration. 

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

The hostname in the URLs provided should be a DNS entry which points to multiple brokers or a virtual IP address, which is backed by multiple broker IP addresses, so that the proxy does not lose connectivity to Pulsar cluster if a single broker becomes unavailable.

The ports to connect to the brokers (6650 and 8080, or in the case of TLS, 6651 and 8443) should be open in the network ACLs.

Note that if you do not use functions, you do not need to configure `functionWorkerWebServiceURL`.

## Start the proxy

To start the proxy:

```bash
$ cd /path/to/pulsar/directory
$ bin/pulsar proxy
```

> You can run multiple instances of the Pulsar proxy in a cluster.

## Stop the proxy

Pulsar proxy runs in the foreground by default. To stop the proxy, simply stop the process in which the proxy is running.

## Proxy frontends

You can run Pulsar proxy behind some kind of load-distributing frontend, such as an [HAProxy](https://www.digitalocean.com/community/tutorials/an-introduction-to-haproxy-and-load-balancing-concepts) load balancer.

## Use Pulsar clients with the proxy

Once your Pulsar proxy is up and running, preferably behind a load-distributing [frontend](#proxy-frontends), clients can connect to the proxy via whichever address that the frontend uses. If the address is the DNS address `pulsar.cluster.default`, for example, the connection URL for clients is `pulsar://pulsar.cluster.default:6650`.

For more information on Proxy configuration, refer to [Pulsar proxy](reference-configuration.md#pulsar-proxy).

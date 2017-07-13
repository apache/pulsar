[Clients](../../getting-started/Clients) connecting to Pulsar {% popover brokers %} need to be able to communicate with an entire Pulsar {% popover instance %} using a single URL. Pulsar provides a built-in service discovery mechanism that you can set up using the instructions [immediately below](#service-discovery-setup).

You can also use your own service discovery system if you'd like. If you use your own system, there is just one requirement: when a client performs an HTTP request to an endpoint for a Pulsar {% popover cluster %}, such as `http://pulsar.us-west.example.com:8080`, the client needs to be redirected to *some* active broker in the desired cluster, whether via DNS, an HTTP or IP redirect, or some other means.

{% include admonition.html type="success" title="Service discovery already provided by many scheduling systems" content="
Many large-scale deployment systems, such as [Kubernetes](../../deployment/Kubernetes), have service discovery systems built in. If you're running Pulsar on such a system, you many not need to provide your own service discovery mechanism.
" %}

### Service discovery setup

The service discovery mechanism included with Pulsar maintains a list of active brokers, stored in {% popover ZooKeeper %}, and supports lookup using HTTP and also Pulsar's [binary protocol](../../project/BinaryProtocol).

To get started setting up Pulsar's built-in service discovery, you need to change a few parameters in the [`conf/discovery.conf`](../../reference/Configuration#service-discovery) configuration file. Set the [`zookeeperServers`](../../reference/Configuration#service-discovery-zookeeperServers) parameter to the global ZooKeeper quorum connection string and the [`globalZookeeperServers`](../../reference/Configuration#service-discovery-globalZookeeperServers)

```properties
# Zookeeper quorum connection string
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Global zookeeper quorum connection string
globalZookeeperServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184
```

To start the discovery service:

```shell
$ bin/pulsar-daemon start discovery
```

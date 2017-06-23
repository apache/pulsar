[Clients](../../getting-started/Clients) connecting to Pulsar {% popover brokers %} need to be able to communicate with an entire Pulsar {% popover instance %} using a single URL. You can use your own service discovery system for this if you'd like, but Pulsar provides a built-in service discovery mechanism. For setup instructions, see the [Deploying a Pulsar instance](../../deployment/InstanceSetup) guide.

You can either use the provided `discovery-service` or any other method. The
only requirement is that when the client does a HTTP request on
`http://pulsar.us-west.example.com:8080/` it must be redirected (through DNS, IP
or HTTP redirect) to an active broker, without preference.

The included discovery service maintains the list of active brokers from ZooKeeper and it supports lookup redirection with HTTP and also with [binary protocol](https://github.com/yahoo/pulsar/blob/master/docs/BinaryProtocol.md#service-discovery).

Add the ZK servers in `conf/discovery.conf`:

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

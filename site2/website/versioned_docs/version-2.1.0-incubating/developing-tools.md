---
id: version-2.1.0-incubating-develop-tools
title: Simulation tools
sidebar_label: Simulation tools
original_id: develop-tools
---

It is sometimes necessary create an test environment and incur artificial load to observe how well load managers
handle the load. The load simulation controller, the load simulation client, and the broker monitor were created as an
effort to make create this load and observe the effects on the managers more easily.

## Simulation Client
The simulation client is a machine which will create and subscribe to topics with configurable message rates and sizes.
Because it is sometimes necessary in simulating large load to use multiple client machines, the user does not interact
with the simulation client directly, but instead delegates their requests to the simulation controller, which will then
send signals to clients to start incurring load. The client implementation is in the class
`org.apache.pulsar.testclient.LoadSimulationClient`.

### Usage
To Start a simulation client, use the `pulsar-perf` script with the command `simulation-client` as follows:

```
pulsar-perf simulation-client --port <listen port> --service-url <pulsar service url>
```

The client will then be ready to receive controller commands.
## Simulation Controller
The simulation controller send signals to the simulation clients, requesting them to create new topics, stop old
topics, change the load incurred by topics, as well as several other tasks. It is implemented in the class
`org.apache.pulsar.testclient.LoadSimulationController` and presents a shell to the user as an interface to send
command with.

### Usage
To start a simulation controller, use the `pulsar-perf` script with the command `simulation-controller` as follows:

```
pulsar-perf simulation-controller --cluster <cluster to simulate on> --client-port <listen port for clients>
--clients <comma-separated list of client host names>
```

The clients should already be started before the controller is started. You will then be presented with a simple prompt,
where you can issue commands to simulation clients. Arguments often refer to tenant names, namespace names, and topic
names. In all cases, the BASE name of the tenants, namespaces, and topics are used. For example, for the topic
`persistent://my_tenant/my_cluster/my_namespace/my_topic`, the tenant name is `my_tenant`, the namespace name is
`my_namespace`, and the topic name is `my_topic`. The controller can perform the following actions:

* Create a topic with a producer and a consumer
    * `trade <tenant> <namespace> <topic> [--rate <message rate per second>]
    [--rand-rate <lower bound>,<upper bound>]
    [--size <message size in bytes>]`
* Create a group of topics with a producer and a consumer
    * `trade_group <tenant> <group> <num_namespaces> [--rate <message rate per second>]
    [--rand-rate <lower bound>,<upper bound>]
    [--separation <separation between creating topics in ms>] [--size <message size in bytes>]
    [--topics-per-namespace <number of topics to create per namespace>]`
* Change the configuration of an existing topic
    * `change <tenant> <namespace> <topic> [--rate <message rate per second>]
    [--rand-rate <lower bound>,<upper bound>]
    [--size <message size in bytes>]`
* Change the configuration of a group of topics
    * `change_group <tenant> <group> [--rate <message rate per second>] [--rand-rate <lower bound>,<upper bound>]
    [--size <message size in bytes>] [--topics-per-namespace <number of topics to create per namespace>]`
* Shutdown a previously created topic
    * `stop <tenant> <namespace> <topic>`
* Shutdown a previously created group of topics
    * `stop_group <tenant> <group>`
* Copy the historical data from one ZooKeeper to another and simulate based on the message rates and sizes in that
history
    * `copy <tenant> <source zookeeper> <target zookeeper> [--rate-multiplier value]`
* Simulate the load of the historical data on the current ZooKeeper (should be same ZooKeeper being simulated on)
    * `simulate <tenant> <zookeeper> [--rate-multiplier value]`
* Stream the latest data from the given active ZooKeeper to simulate the real-time load of that ZooKeeper.
    * `stream <tenant> <zookeeper> [--rate-multiplier value]`

The "group" arguments in these commands allow the user to create or affect multiple topics at once. Groups are created
when calling the `trade_group` command, and all topics from these groups may be subsequently modified or stopped
with the `change_group` and `stop_group` commands respectively. All ZooKeeper arguments are of the form
`zookeeper_host:port`.

### Difference Between Copy, Simulate, and Stream
The commands `copy`, `simulate`, and `stream` are very similar but have significant differences. `copy` is used when
you want to simulate the load of a static, external ZooKeeper on the ZooKeeper you are simulating on. Thus,
`source zookeeper` should be the ZooKeeper you want to copy and `target zookeeper` should be the ZooKeeper you are
simulating on, and then it will get the full benefit of the historical data of the source in both load manager
implementations. `simulate` on the other hand takes in only one ZooKeeper, the one you are simulating on. It assumes
that you are simulating on a ZooKeeper that has historical data for `SimpleLoadManagerImpl` and creates equivalent
historical data for `ModularLoadManagerImpl`. Then, the load according to the historical data is simulated by the
clients. Finally, `stream` takes in an active ZooKeeper different than the ZooKeeper being simulated on and streams
load data from it and simulates the real-time load. In all cases, the optional `rate-multiplier` argument allows the
user to simulate some proportion of the load. For instance, using `--rate-multiplier 0.05` will cause messages to
be sent at only `5%` of the rate of the load that is being simulated.

## Broker Monitor
To observe the behavior of the load manager in these simulations, one may utilize the broker monitor, which is
implemented in `org.apache.pulsar.testclient.BrokerMonitor`. The broker monitor will print tabular load data to the
console as it is updated using watchers.

### Usage
To start a broker monitor, use the `monitor-brokers` command in the `pulsar-perf` script:

```
pulsar-perf monitor-brokers --connect-string <zookeeper host:port>
```

The console will then continuously print load data until it is interrupted.


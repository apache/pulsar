<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

[Clients](../../getting-started/Clients) connecting to Pulsar {% popover brokers %} need to be able to communicate with an entire Pulsar {% popover instance %} using a single URL. Pulsar provides a built-in service discovery mechanism that you can set up using the instructions [immediately below](#service-discovery-setup).

You can also use your own service discovery system if you'd like. If you use your own system, there is just one requirement: when a client performs an HTTP request to an [endpoint](../../reference/Configuration) for a Pulsar {% popover cluster %}, such as `http://pulsar.us-west.example.com:8080`, the client needs to be redirected to *some* active broker in the desired cluster, whether via DNS, an HTTP or IP redirect, or some other means.

{% include admonition.html type="success" title="Service discovery already provided by many scheduling systems" content="
Many large-scale deployment systems, such as [Kubernetes](../../deployment/Kubernetes), have service discovery systems built in. If you're running Pulsar on such a system, you may not need to provide your own service discovery mechanism.
" %}

### Service discovery setup

The service discovery mechanism included with Pulsar maintains a list of active brokers, stored in {% popover ZooKeeper %}, and supports lookup using HTTP and also Pulsar's [binary protocol](../../project/BinaryProtocol).

To get started setting up Pulsar's built-in service discovery, you need to change a few parameters in the [`conf/discovery.conf`](../../reference/Configuration#service-discovery) configuration file. Set the [`zookeeperServers`](../../reference/Configuration#service-discovery-zookeeperServers) parameter to the cluster's ZooKeeper quorum connection string and the [`configurationStoreServers`](../../reference/Configuration#service-discovery-configurationStoreServers) setting to the {% popover configuration store %} quorum connection string.

```properties
# Zookeeper quorum connection string
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Global configuration store connection string
configurationStoreServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184
```

To start the discovery service:

```shell
$ bin/pulsar-daemon start discovery
```
